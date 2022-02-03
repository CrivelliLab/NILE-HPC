# test.py
# Testing NILE functional wrapper in Python3.
# Implemented MPI: $ mpirun -n cores python3 test.py

#--
import json
import numpy as np
import pandas as pd
from nile import NILE

#--
if __name__ == "__main__":

    #-
    with NILE(mpi=False) as nlp:

        #- Load A Lexicon
        lexicon = pd.read_csv("data/lexicons/clever_mods.csv")
        nlp.add_lexicon(lexicon)

        #- Add individual phrases
        nlp.add_observation("suicide", "SUICIDE")

        #- Single Text Example
        text = "The mother has a history of suicide. The pt may be at risk for suicide."
        parsed = nlp(text)
        print(json.dumps(parsed, indent=4, sort_keys=True))

        #- DataFrame Example
        df = pd.DataFrame([(i, text) for i in range(1)], columns=["tid", "text"])
        dfs = np.array_split(df, len(nlp.workers))
        outs = []

        #- Broadcast NILE Across Workers
        for i, worker in enumerate(nlp.workers):
            nlp.transform(dfs[i],xcol="text",uid_cols=["tid"], rank=worker)

        #- Gather NILE Results From Workers
        for i, worker in enumerate(nlp.workers):
            outs.append(nlp.gather(rank=worker))

        #-
        out = pd.concat(outs)
        print(out)
