'''Parser für Output von DFH$MOLS
'''
import pandas as pd
from timeit import default_timer as timer

from . import scanners as g
from . import help_io  as io
def load(pattern):
    '''
        Lädt SYSPRINT von DFH$MOLS in einen CMF-DataFrame
    '''

    pathes =io.gen_pathes(pattern)      #
    dfhmols = io.gen_cat(pathes)         # Verkette die Eingaben

    tuples = (g.gen_splits(dfhmols))    # Erzeuge aus Zeile ein Tuple 
    fields = g.gen_keyvalpairs(tuples)  # Formatiere Tuple 

    t_start = timer()
    cmfrecs = [cmfrec for cmfrec in g.gen_dicts(fields)]
    delta = timer() - t_start
    print("{:0} CMF Records in {:3.2f} Sekunden ({:3.1f} ms)".format(
        len(cmfrecs), delta, delta / len(cmfrecs) * 1000)
         )
    t_start = timer()
    dframe = pd.DataFrame(cmfrecs)
    delta = timer() - t_start

    # Erzeuge weitere Spalte RESP für die Responsetime in Sekunden
    t_start = timer()
    dframe['RESP'] = (dframe['STOP']-dframe['START']).apply(
        pd.Timedelta.total_seconds)
    delta_2 = timer() - t_start
    print("Konvertiert in DataFrame in {:3.2f} Sekunden".format(delta))
    print("Berechnet RESP in {:3.2f} Sekunden".format(delta_2))
    return dframe
