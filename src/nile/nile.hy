; nile.hy
; A prototype functional wrapper for NILE.
; Implemented in Hy (Python with Lisp Syntax).

;--
(require [hy.contrib.walk [let]])

;--
(import os re json
        jpype
        jpype.imports
        [time [sleep]]
        [mpi4py [MPI]]
        [numpy :as np]
        [pandas :as pd]
        [sqlite3 :as sql]
        [nltk.tokenize.treebank [TreebankWordTokenizer]])
(jpype.addClassPath (.format "{}/NILE.jar" (os.path.dirname (os.path.abspath __file__))))

;--
(defclass NILE [object]

  ;--
  (defn __init__ [self &optional [mpi False] [conn None] [verbose True]]
    (setv self._comm (if mpi MPI.COMM_WORLD None)
          self._rank (if mpi (.Get_rank self._comm) 0)
          self._worldsize (if mpi (.Get_size self._comm) 1)
          self._workers (if mpi (list (range 1 self._worldsize)) [0])
          self._buf (np.zeros (len self._workers) :dtype "int8")
          self._win (if mpi (self._init_win) None)
          self._staged None
          self._conn conn
          self._verbose verbose
          self._preprocess_regex1 (re.compile r"\s+")
          self._preprocess_regex2 (re.compile r"(\S+)\s(\S?\'\S+\s)")
          self._tokenizer (TreebankWordTokenizer)))

  ;--
  (defn _preprocess-text [self text]
    (let [value (.lower text)
          value (.sub self._preprocess_regex1 " " value)
          tokenized (.tokenize self._tokenizer value)
          value (.join " " tokenized)
          value (.sub self._preprocess_regex2 r"\1\2" value)]
      value))

  ;--
  (defn _parse_SemanticObj [self obj sentence]
    (let [hit {"tokens" (.join "|" (lfor t (.getTokens sentence) (str t)))
               "role" (str (.getSemanticRole obj))
               "code" (.join ";" (lfor code (.getCode obj) (str code)))
               "term" (str (.getText obj))
               "token_start" (int (.getOffsetStart obj))
               "token_end" (int (.getOffsetEnd obj))
               "certain" (= "YES" (str (.getCertainty obj)))
               "fam_hx" (= 1 (int (.isFamilyHistory obj)))}]
      (for [mod self._mods]
        (let [checks (lfor m (.getModifiers obj) (in mod (lfor c (.getCode m) (str c))))]
          (assoc hit mod (any checks))))
      hit))

  ;--
  (defn __call__ [self text]
    (let [text-preprocessed (self._preprocess-text text)
          sentences (self._nlp.digTextLine text-preprocessed)
          semobjs []]
      (for [sent sentences]
        (for [obj (.getSemanticObjs sent)]
          (.append semobjs (self._parse_SemanticObj obj sent))))
      semobjs))

  ;--
  (defn __enter__ [self]
    (jpype.startJVM)
    (import [edu.harvard.hsph.biostats.nile [NaturalLanguageProcessor]]
            [edu.harvard.hsph.biostats.nile [SemanticRole]])
    (setv self._nlp (NaturalLanguageProcessor)
          self._observation SemanticRole.OBSERVATION
          self._modifier SemanticRole.MODIFIER
          self._mods [])
    (unless (= self._comm None)
      (if (= self._rank 0)
          (self._comm.Barrier)
          (self._worker_process)))
    self)

  ;--
  (defn __exit__ [self e1 e2 e3]
    (jpype.shutdownJVM)
    (unless (= self._comm None)
      (for [i (range 1 self._worldsize)]
        (let [data {"task" "__exit__"}]
          (self._comm.send data :dest i :tag 11)))))

  ;--
  (defn add-observation [self term code]
    (try
      (self._nlp.addPhrase term code self._observation)
      (unless (| (= self._comm None) (!= self._rank 0))
        (for [i (range 1 self._worldsize)]
          (let [data {"task" "add_observation" "term" term "code" code}]
            (self._comm.send data :dest i :tag 11))))
      (except []
        (when self._verbose
          (print (.format "java: nile.InconsistentPhraseDefinitionException on ({}, {}, {})"
                          term code "OBSERVATION"))))))

  ;--
  (defn add-modifier [self term code]
    (try
      (self._nlp.addPhrase term code self._modifier)
      (unless (in code self._mods) (.append self._mods code))
      (unless (| (= self._comm None) (!= self._rank 0))
        (for [i (range 1 self._worldsize)]
          (let [data {"task" "add_modifier" "term" term "code" code}]
            (self._comm.send data :dest i :tag 11))))
      (except []
        (when self._verbose
          (print (.format "java: nile.InconsistentPhraseDefinitionException on ({}, {}, {})"
                          term code "MODIFIER"))))))

  ;--
  (defn add-lexicon [self df]
    (assert (= (type df) pd.DataFrame) "Requires a pandas.DataFrame.")
    (assert (& (in "term" df.columns) (in "code" df.columns) (in "role" df.columns))
            (+ "NILE Lexicons require terms(str), codes(str) and roles(enum\{'OBSERVATION','MODIFIER'\}).\n"
               "DataFrame missing one of following columns ['term', 'code', 'role']."))
    (for [(, i row) (.iterrows (get df ["term" "code" "role"]))]
      (setv (, term code role) row)
      (cond [(= role "OBSERVATION") (self.add-observation term code)]
            [(= role "MODIFIER") (self.add-modifier term code)])))

  ;--
  (defn _extract_staged [self query xcol uid-cols]
    (with [conn (sql.connect self._conn)]
      (setv self._staged (get (pd.read-sql query conn) (+ uid-cols [xcol])))))

  ;--
  (defn extract [self query &optional [xcol "text"] [uid-cols ["index"]] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (self._extract_staged query xcol uid-cols)
        (let [data {"task" "extract" "query" query "xcol" xcol "uid_cols" uid-cols}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn _extract-pandas_staged [self inpath xcol uid-cols]
    (setv self._staged
          (cond [(.endswith inpath ".csv") (get (pd.read-csv inpath) (+ uid-cols [xcol]))]
                [(.endswith inpath ".parquet") (get (pd.read-parquet inpath) (+ uid-cols [xcol]))])))

  ;--
  (defn extract-pandas [self inpath &optional [xcol "text"] [uid-cols ["index"]] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (self._extract-pandas_staged inpath xcol uid-cols)
        (let [data {"task" "extract_pandas" "inpath" inpath "xcol" xcol "uid_cols" uid-cols}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn _tranform_staged [self xcol uid-cols]
    (when (is self._staged None) (raise "nile: transform: No DataFrame has been staged."))
    (let [results []]
      (for [(, i row) (.iterrows self._staged)]
        (let [semobjs (self (get row xcol))]
          (for [obj semobjs]
            (let [nrow {} meta {}]
              (for [col uid-cols] (assoc nrow col (get row col)))
              (for [col ["role" "code" "term" "certain"]] (assoc nrow col (get obj col)))
              (for [col (.keys obj)]
                (when (not (in col ["role" "code" "term" "certain"]))
                  (assoc meta col (get obj col))))
              (assoc nrow "meta" (json.dumps meta))
              (.append results nrow)))))
      (setv self._staged (pd.DataFrame results))))

  ;--
  (defn transform [self &optional [df None] [xcol "text"] [uid-cols ["index"]] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (do (unless (is df None) (setv self._staged df))
            (self._tranform_staged xcol uid-cols))
        (let [data {"task" "transform" "df" df "xcol" xcol "uid_cols" uid-cols}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn _load_staged [self to_table if-exists]
    (when (is self._staged None) (raise "nile: load: No DataFrame has been staged."))
    (with [conn (sql.connect self._conn)]
      (.to-sql self._staged to_table conn :if-exists if-exists :index False)
      (setv self._staged None)))

  ;--
  (defn load [self to_table &optional [if-exists "append"] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (self._load_staged to_table if-exists)
        (let [data {"task" "load" "to_table" to_table "if_exists" if-exists}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn _load-pandas_staged [self outpath]
    (when (is self._staged None) (raise "nile: load: No DataFrame has been staged."))
    (cond [(.endswith outpath ".csv") (.to-csv self._staged outpath :index False)]
          [(.endswith outpath ".parquet") (.to-csv self._staged outpath :index False)])
    (setv self._staged None))

  ;--
  (defn load-pandas [self outpath &optional [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (self._load-pandas_staged to_table if-exists)
        (let [data {"task" "load_pandas" "outpath" outpath}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn gather [self &optional [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (let [staged self._staged]
          (setv self._staged None)
          staged)
        (let [data {"task" "gather"}]
          (self._comm.send data :dest rank :tag 11)
          (self._comm.recv :source rank :tag 11))))

  ;--
  (defn etl [self query to_table &optional [xcol "text"] [uid-cols ["index"]] [if-exists "append"] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (do (self._extract_staged query xcol uid-cols)
            (self._tranform_staged xcol uid-cols)
            (self._load_staged to_table if-exists))
        (let [data {"task" "etl" "query" query "to_table" to_table "xcol" xcol "uid_cols" uid-cols "if_exists" if-exists}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn etl-pandas [self inpath outpath &optional [xcol "text"] [uid-cols ["index"]] [rank 0]]
    (if (| (= self._comm None) (= rank 0))
        (do (self._extract-pandas_staged inpath xcol uid-cols)
            (self._tranform_staged xcol uid-cols)
            (self._load-pandas_staged outpath))
        (let [data {"task" "etl_pandas" "inpath" inpath "outpath" outpath "xcol" xcol "uid_cols" uid-cols}]
          (self._set_active rank 1)
          (self._comm.send data :dest rank :tag 11))))

  ;--
  (defn _init_win [self]
    (let [size (* (len self._workers) (MPI.CHAR.Get_size))
          win (MPI.Win.Allocate size :comm self._comm)]
      (when (= self._rank 0)
        (win.Lock :rank 0)
        (win.Put self._buf :target-rank 0)
        (win.Unlock :rank 0))
      (self._comm.Barrier)
      win))

  ;--
  (defn _set_active [self wid val]
    (let [wid (- wid 1)]
      (self._win.Lock :rank 0)
      (self._win.Get self._buf :target-rank 0)
      (setv (get self._buf wid) val)
      (self._win.Put self._buf :target-rank 0)
      (self._win.Unlock :rank 0)))

  ;--
  (defn _worker_process [self]
    (self._comm.Barrier)
    (while True
      (let [data (self._comm.recv :source 0 :tag 11)]
        (cond
          [(= (get data "task") "__exit__") (do (jpype.shutdownJVM) (exit))]
          [(= (get data "task") "add_observation")
           (self.add-observation (get data "term") (get data "code"))]
          [(= (get data "task") "add_modifier")
           (self.add-modifier (get data "term") (get data "code"))]
          [(= (get data "task") "extract")
           (do (self._extract_staged (get data "query") (get data "xcol") (get data "uid_cols"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "extract_pandas")
           (do (self._extract-pandas_staged (get data "inpath") (get data "xcol") (get data "uid_cols"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "transform")
           (do (unless (is (get data "df") None) (setv self._staged (get data "df")))
               (self._tranform_staged (get data "xcol") (get data "uid_cols"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "load")
           (do (self._load_staged (get data "to_table") (get data "if_exists"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "load_pandas")
           (do (self._load-pandas_staged (get data "outpath"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "gather")
           (let [staged self._staged]
             (setv self._staged None)
             (self._comm.send staged :dest 0 :tag 11))]
          [(= (get data "task") "etl")
           (do (self._extract_staged (get data "query") (get data "xcol") (get data "uid_cols"))
               (self._tranform_staged (get data "xcol") (get data "uid_cols"))
               (self._load_staged (get data "to_table") (get data "if_exists"))
               (self._set_active self._rank 0))]
          [(= (get data "task") "etl_pandas")
           (do (self._extract-pandas_staged (get data "inpath") (get data "xcol") (get data "uid_cols"))
               (self._tranform_staged (get data "xcol") (get data "uid_cols"))
               (self._load-pandas_staged (get data "outpath"))
               (self._set_active self._rank 0))]))))

  ;--
  (defn get-workers [self]
    (if (= self._comm None)
        {0 0}
        (do (self._win.Lock :rank 0)
            (self._win.Get self._buf :target-rank 0)
            (self._win.Unlock :rank 0)
            (dfor rank self._workers [rank (get self._buf (- rank 1))]))))

  ;--
  (defn barrier [self]
    (let [actives (sum (lfor (, rank active) (.items (self.get-workers)) active))]
      (while (> actives 1)
        (setv actives (sum (lfor (, rank active) (.items (self.get-workers)) active)))))))
