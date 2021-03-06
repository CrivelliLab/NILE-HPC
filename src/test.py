# test.py
# Testing NILE wrapper in Python3.
# Implemented MPI: $ mpirun -n cores python3 test.py

#--
import json
import pandas as pd
import sqlite3 as sql
from nile import NILE

#- SQL Connections
conn = "data/example.db"
query = """
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY tid) AS rn FROM Documents
) T1
WHERE rn % {workers} = {wid}"""
to_table = "nile"

#--
if __name__ == "__main__":

    #- Instaniate NILE Context
    with NILE(mpi=True,conn=conn,verbose=False) as nlp:

        #- Create Database
        with sql.connect(conn) as db:
            df = pd.read_csv("data/datasets/example.csv")
            df.to_sql(name="Documents",if_exists="replace",con=db)
            try: db.cursor().execute("DROP TABLE {};".format(to_table))
            except: pass

        #- Load and Add Lexicon to NILE
        lexicon = pd.read_csv("data/lexicons/clever_mods.csv")
        nlp.add_lexicon(lexicon)

        #- Add Individual Phrases
        nlp.add_observation(term="suicide", code="SUICIDE")

        #- Parse Single String
        text = "The mother has a history of suicide. The pt may be at risk for suicide."
        parsed = nlp(text)
        print(json.dumps(parsed, indent=4, sort_keys=True))

        #- Broadcast ETL (Extract, Transform, Load) Across Workers
        workers = nlp.get_workers()
        for i, worker in enumerate(workers):
            query_ = query.format(workers=len(workers),wid=i)
            nlp.etl(query_, to_table, xcol="text", uid_cols=["tid"] ,rank=worker)
        nlp.barrier()

        #- Load NILE Results
        with sql.connect(conn) as db:
            results = pd.read_sql("SELECT * FROM {};".format(to_table),db)
            print(results)
            print(results.columns)
