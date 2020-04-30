'''Parser für Output von DFH$MOLS
'''
from timeit import default_timer as timer
import pandas as pd

from . import scanners as g
from . import help_io  as io
def load(pattern):
    '''
        Lädt SYSPRINT von DFH$MOLS in einen CMF-DataFrame
    '''

    pathes = io.gen_pathes(pattern)      #
    #
    #  4 parts Field (xxxxxxxx, tnnn), Nicknam and Content
    #
    tuples = (
        rec.split(maxsplit=3)
        for rec in io.gen_cat(pathes)
        if len(rec) > 60 and rec[:4] == ' '*4
    )
    fields = g.gen_keyvalpairs(tuples)  # Formatiere Tuple

    t_start = timer()
    #
    # DataFrame kann auch direkt aus einem Generator erzeugt werden
    #
    dframe = pd.DataFrame(g.gen_dicts(fields))
    delta = timer() - t_start
    len_dframe = len(dframe) if len(dframe) > 0 else 1
    print(
        f"{len(dframe)} CMF records "
        f"in {delta:3.2f} seconds,"
        f"({delta/len_dframe*1000:3.1f} ms)"
    )

    # Erzeuge weitere Spalte RESP für die Responsetime in Sekunden
    if len(dframe) > 0:
        t_start = timer()
        dframe['RESP'] = (dframe['STOP']-dframe['START']).apply(
            pd.Timedelta.total_seconds)
        delta_2 = timer() - t_start
        print(f"Added RESP in {delta_2:3.2f} Seconds.")
    return dframe
