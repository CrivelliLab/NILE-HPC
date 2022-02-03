;; umls.hy
;; Author: Rafael Zamora-Resendiz, Computational Research Division, LBNL
;;
;; Module contains functions which are used to read in UMLS Metathesarus data
;; Docstrings for each function contains descriptor for each column.
;;

;- Requires
(require [hy.contrib.walk [let]])

;- Imports
(import os
        [pandas :as pd])

;- STATIC GLOBALS
(setv UMLSPATH "/lustre/valustre/VA_MVP011_CDWTIUNotes/MVP011N_Datasets/umls/")

;--
(defn read-MRCONSO [&optional [PATH UMLSPATH]]
  """ Concept Names and Sources:
  CUI	~ Unique identifier for concept
  LAT	~ Language of term
  TS	~ Term status
  LUI	~ Unique identifier for term
  STT	~ String type
  SUI	~ Unique identifier for string
  ISPREF ~ Atom status - preferred (Y) or not (N) for this string within this concept
  AUI	~ Unique identifier for atom - variable length field, 8 or 9 characters
  SAUI ~ Source asserted atom identifier [optional]
  SCUI ~ Source asserted concept identifier [optional]
  SDUI ~ Source asserted descriptor identifier [optional]
  SAB	~ Abbreviated source name (SAB). Maximum field length is 20 alphanumeric characters.
        Two source abbreviations are assigned:
          Root Source Abbreviation (RSAB) — short form, no version information, for example, AI/RHEUM, 1993, has an RSAB of 'AIR'
          Versioned Source Abbreviation (VSAB) — includes version information, for example, AI/RHEUM, 1993, has an VSAB of 'AIR93'
        Official source names, RSABs, and VSABs are included on the UMLS Source Vocabulary Documentation page.
  TTY ~	Abbreviation for term type in source vocabulary, for example PN (Metathesaurus Preferred Name) or CD (Clinical Drug). Possible values are listed on the Abbreviations Used in Data Elements page.
  CODE ~ Most useful source asserted identifier (if the source vocabulary has more than one identifier), or a Metathesaurus-generated source entry identifier (if the source vocabulary has none)
  STR ~	String
  SRL ~	Source restriction level
  SUPPRESS ~ Suppressible flag. Values = O, E, Y, or N
        O: All obsolete content, whether they are obsolesced by the source or by NLM. These will include all atoms having obsolete TTYs, and other atoms becoming obsolete that have not acquired an obsolete TTY (e.g. RxNorm SCDs no longer associated with current drugs, LNC atoms derived from obsolete LNC concepts).
        E: Non-obsolete content marked suppressible by an editor. These do not have a suppressible SAB/TTY combination.
        Y: Non-obsolete content deemed suppressible during inversion. These can be determined by a specific SAB/TTY combination explicitly listed in MRRANK.
        N: None of the above
      Default suppressibility as determined by NLM (i.e., no changes at the Suppressibility tab in MetamorphoSys) should be used by most users, but may not be suitable in some specialized applications.
      See the MetamorphoSys Help page for information on how to change the SAB/TTY suppressibility to suit your requirements.
      NLM strongly recommends that users not alter editor-assigned suppressibility, and MetamorphoSys cannot be used for this purpose.
      CVF	Content View Flag. Bit field used to flag rows included in Content View.
      This field is a varchar field to maximize the number of bits available for use.
  """
  (setv df (pd.read-parquet (.format "{}MRCONSO.ENG.parquet" PATH))))

;--
(defn read-MRDEF [&optional [chunksize "16MB"]]
  """ Definitions:
  CUI ~	Unique identifier for concept
  AUI	~ Unique identifier for atom - variable length field, 8 or 9 characters
  ATUI ~ Unique identifier for attribute
  SATUI	~ Source asserted attribute identifier [optional-present if it exists]
  SAB	~ Abbreviated source name (SAB) of the source of the definition
        Maximum field length is 20 alphanumeric characters. Two source abbreviations are assigned:
          Root Source Abbreviation (RSAB) — short form, no version information, for example, AI/RHEUM, 1993, has an RSAB of 'AIR'
          Versioned Source Abbreviation (VSAB) — includes version information, for example, AI/RHEUM, 1993, has an VSAB of 'AIR93'
          Official source names, RSABs, and VSABs are included on the UMLS Source Vocabulary Documentation page.
  DEF ~	Definition
  SUPPRESS ~ Suppressible flag. Values = O, E, Y, or N.
             Reflects the suppressible status of the attribute; not yet in use.
             See also SUPPRESS in MRCONSO.RRF, MRREL.RRF, and MRSAT.RRF.
  CVF ~	Content View Flag.
        Bit field used to flag rows included in Content View.
        This field is a varchar field to maximize the number of bits available for use.
  """
  (setv CALLPATH (os.getcwd))
  (os.chdir UMLSPATH)
  (setv df (ddf.read-parquet "MRDEF.ENG.parquet"
                             :partition-size chunksize))
  (os.chdir CALLPATH)
  df)

;--
(defn read-MRSTY [&optional [chunksize "16MB"]]
  """ Semantic Types:
  CUI	~ Unique identifier of concept
  TUI	~ Unique identifier of Semantic Type
  STN	~ Semantic Type tree number
  STY	~ Semantic Type. The valid values are defined in the Semantic Network.
  ATUI ~ Unique identifier for attribute
  CVF	~ Content View Flag.
        Bit field used to flag rows included in Content View.
        This field is a varchar field to maximize the number of bits available for use.
  """
  (setv CALLPATH (os.getcwd))
  (os.chdir UMLSPATH)
  (setv df (ddf.read-parquet "MRSTY.ENG.parquet"
                             :partition-size chunksize))
  (os.chdir CALLPATH)
  df)

;--
(defn read-MRREL [&optional [chunksize "16MB"]]
  """  Related Concepts:
  CUI1 ~ Unique identifier of first concept
  AUI1 ~ Unique identifier of first atom
  STYPE1 ~ The name of the column in MRCONSO.RRF that contains the identifier used for the first element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.
  REL ~	Relationship of second concept or atom to first concept or atom
  CUI2 ~ Unique identifier of second concept
  AUI2 ~ Unique identifier of second atom
  STYPE2 ~ The name of the column in MRCONSO.RRF that contains the identifier used for the second element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.
  RELA ~ Additional (more specific) relationship label (optional)
  RUI ~ Unique identifier of relationship
  SRUI ~ Source asserted relationship identifier, if present
  SAB ~	Abbreviated source name of the source of relationship. Maximum field length is 20 alphanumeric characters.
        Two source abbreviations are assigned:
          Root Source Abbreviation (RSAB) — short form, no version information, for example, AI/RHEUM, 1993, has an RSAB of 'AIR'
          Versioned Source Abbreviation (VSAB) — includes version information, for example, AI/RHEUM, 1993, has an VSAB of 'AIR93'
        Official source names, RSABs, and VSABs are included on the UMLS Source Vocabulary Documentation page.
  SL ~ Source of relationship labels
  RG ~ Relationship group. Used to indicate that a set of relationships should be looked at in conjunction.
  DIR ~ Source asserted directionality flag.
        Y indicates that this is the direction of the relationship in its source;
        N indicates that it is not; a blank indicates that it is not important or has not yet been determined.
  SUPPRESS ~ Suppressible flag. Reflects the suppressible status of the relationship.
             See also SUPPRESS in MRCONSO.RRF, MRDEF.RRF, and MRSAT.RRF.
  CVF	~ Content View Flag. Bit field used to flag rows included in Content View.
        This field is a varchar field to maximize the number of bits available for use.
  """
  (setv CALLPATH (os.getcwd))
  (os.chdir UMLSPATH)
  (setv df (ddf.read-parquet "MRREL.ENG.parquet"
                             :partition-size chunksize))
  (os.chdir CALLPATH)
  df)

; Macros
(require [hy.contrib.walk [let]])

; Imports
(import os
        [pandas :as pd]
        [networkx :as nx]
        [lbnl-mvp.data.umls [*]])

;- STATICS
(setv UMLSPATH "/lustre/valustre/VA_MVP011_CDWTIUNotes/MVP011N_Datasets/umls/")

;--
(defn read-unique-concepts []
  (let [concepts (read-MRCONSO)]
    (setv (get concepts 'STR) (.apply (get concepts 'STR) (fn [x] (.lower x)) :meta "str"))
    (.drop-duplicates concepts 'STR)))

;--
(defn read-relations [&optional [directed False] [REL-filter None] [RELA-filter None]]
  (setv df (read-MRREL)
        df (if directed (get df (= (get df 'DIR) "Y")) df)
        df (if REL-filter (get df (.apply (get df 'REL) REL-filter :meta "bool")) df)
        df (if RELA-filter (get df (.apply (get df 'RELA) RELA-filter :meta "bool")) df))
  df)

;--
(defn build-graph [concepts relations]
  (let [DG (nx.DiGraph)
        relate (fn [x]
                 (setv C1 (get x 'CUI1) C2 (get x 'CUI2) R (get x 'REL) RA (get x 'RELA))
                 (.add-edge DG C1 C2 :REL R :RELA RA))]
    (.add_nodes_from DG (-> concepts (get 'CUI) .unique))
    (.apply (get relations ['CUI1 'CUI2 'REL 'RELA]) relate :axis 1)
    (.remove-edges-from DG (nx.selfloop_edges DG))
    DG))

;--
(defn read-DAG []
  (let [PATH (.format "{}/concept_dag.pickle" UMLSPATH)]
    (if (os.path.exists PATH)
      (nx.read-gpickle PATH)
      (let [concepts (.compute (read-unique-concepts))
            REL-filter (fn [x] (in x ["SY" "RU" "RQ" "RN" "CHD" "RL"]))
            relations (.compute (read-relations :directed True :REL-filter REL-filter))
            dag (build-graph concepts relations)]
        (nx.write_gpickle dag PATH)
        dag))))

;--
(defn match-on [concepts metric &optional [threshold identity]]
  (let [score (metric concepts)
        matches (.apply score threshold)]
    (get concepts matches)))

;--
(defn related-subgraph [graph concepts &optional [depth-limit None]]
  (let [parents (flatten (lfor c (.unique concepts)
                           (lfor p (.items (nx.dfs_successors graph c 1)) p)))
        children (flatten (lfor c parents
                            (lfor p (.items (nx.dfs_predecessors graph c)) p)))
        subgraph (.copy (.subgraph graph (list (set (+ children parents (list concepts))))))]
    (for [c (flatten (lfor c (.nodes subgraph) (if (= (len (list (get subgraph c))) 0) c [])))]
      (.add-node subgraph "ROOT")
      (.add-edge subgraph c "ROOT"))
    subgraph))

;--
(defn expand-lexicon [hits concepts dag &optional [verbose False]]
  (let [subgraph (related-subgraph dag (get hits 'CUI))]
    (when verbose
      (print (.format "Number of unique related-concepts: {}" (len (.unique (get hits 'CUI)))))
      (print (.format "Number of unique related-concepts: {}" (+ -1 (len (.nodes subgraph))))))
    (setv expanded-hits (get concepts (.isin (get concepts 'CUI) (.nodes subgraph)))
          expanded-hits (.append hits expanded-hits))
    (, expanded-hits subgraph)))
