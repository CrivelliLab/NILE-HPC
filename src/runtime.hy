;--
(require [hy.contrib.walk [let]])

;--
(import [time [time]]
        [numpy :as np]
        [pandas :as pd]
        [nile [NILE]]
        [hy.contrib.pprint [pprint]])

;--
(setv TEMPPATH "../daskspace/"
      CLEVERPATH "data/lexicons/clever_mods.csv"
      LEXICONPATH "data/lexicons/lifeevents_obs.csv"
      DATASETPATH "data/RV.50.TIUReportText.parquet"
      XCOL "ReportText"
      UID-COLS ["PatientICN" "Sta3n" "TIUDocumentIEN" "TIUDocumentSID"])

;--
(defmain [&rest ARGS]

  ;-
  (with [nlp (NILE :mpi False :verbose True)]

    ;-
    (let [NB-SAMPLES (int (get ARGS 1))
          NB-LEXICON (int (get ARGS 2))
          NB-WORKERS (len (nlp.get-workers))
          dataset (.dropna (pd.read-parquet DATASETPATH) :subset [XCOL])
          dataset (get dataset (+ UID-COLS [XCOL]))
          dataset (.reset-index (.sample dataset :n NB-SAMPLES) :drop True)
          lexicon (pd.concat [(pd.read_csv LEXICONPATH) (pd.read_csv CLEVERPATH)])
          lexicon (.reset-index (.sample lexicon :n NB-LEXICON) :drop True)]
      (nlp.add_lexicon lexicon)

      ;-
      (let [results [] t (time)
            workers (nlp.get-workers)
            chunksize (// (len dataset) NB-WORKERS)]
        (for [(, i worker) (enumerate workers)]
          (.to-parquet (cut dataset (* i chunksize) (* (+ i 1) chunksize)) (.format "{}part.{}.parquet" TEMPPATH i) :index False))
        (nlp.barrier)
        (for [(, i worker) (enumerate workers)]
          (nlp.extract-pandas (.format "{}part.{}.parquet" TEMPPATH i) :xcol XCOL :uid-cols UID-COLS :rank worker))
        (nlp.barrier)
        (for [(, i worker) (enumerate workers)]
          (nlp.transform :xcol XCOL :uid-cols UID-COLS :rank worker))
        (nlp.barrier)
        (for [(, i worker) (enumerate workers)]
          (nlp.gather :rank worker))
        (pprint {"NB-SAMPLES" NB-SAMPLES
                 "NB-LEXICON" NB-LEXICON
                 "NB-WORKERS" NB-WORKERS
                 "RUNTIME" (- (time) t)})))))
