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
    dfhmols = io.gen_cat(pathes)         # Verkette die Eingaben

    tuples = (g.gen_splits(dfhmols))    # Erzeuge aus Zeile ein Tuple
    fields = g.gen_keyvalpairs(tuples)  # Formatiere Tuple

    t_start = timer()
    cmfrecs = list(g.gen_dicts(fields))
    delta = timer() - t_start
    print(
        f"{len(cmfrecs)} CMF Records"
        f"in {delta:3.2f} Sekunden"
        f"({delta/len(cmfrecs)*1000:3.1f} ms)"
    )
    t_start = timer()
    #
    # DataFrame kann auch direkt aus einem Generator erzeugt werden
    # dann brauchen die list cmfrecs nicht mehr, sollte Speicher sparen!
    #
    dframe = pd.DataFrame(cmfrecs)
    delta = timer() - t_start

    # Erzeuge weitere Spalte RESP für die Responsetime in Sekunden
    t_start = timer()
    dframe['RESP'] = (dframe['STOP']-dframe['START']).apply(
        pd.Timedelta.total_seconds)
    delta_2 = timer() - t_start
    print(f"Konvertiert in DataFrame in {delta:3.2f} Sekunden")
    print(f"Berechnet RESP in {delta_2:3.2f} Sekunden")
    return dframe
