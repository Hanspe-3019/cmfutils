'''Little Helpers to analyze
   - df_info(dataframe)
   - describe(column)
   - timestamp_from_baid(baid)
   - start_stop(dataframe)
'''
import datetime
import pandas as pd

from .fields import field_to_desc


def df_info(dframe):
    '''Übersicht über die Daten ausgaben
    '''
    bis = dframe.STOP.max()
    von = dframe.START.min()
    print("von    : {:} \nbis    : {:}".format(von, bis))
    print("Trans/s: {:4.1f}".format(len(dframe)/(bis-von).total_seconds()
                                   )
         )
    if 'APPLID' in dframe.columns:
        print(dframe.APPLID.value_counts())
    print(dframe.ix[dframe.RESP.nlargest(10).index][['TRAN', 'RESP', 'START']])

def describe(spalte):
    '''Beschreibung der Inhalte (custom describe)
    '''
    beschr = "{:}, {:.1%} ".format(
            field_to_desc.get(spalte.name, '?'),
            spalte.count()/len(spalte)
            )
    anteil = "{:} von {:}".format(spalte.count(), len(spalte))
    return spalte.describe().to_frame().T.style.set_caption(beschr)

def timestamp_from_baid(baid):
    '''
    Die Benutzeraktion-Id ist ein 13-stelliger Timestamp-String
    im Format MMDDhhmmSSsss.
    Es fehlt also das Jahr. Um daraus ein Timestamp zu machen,
    braucht man diese Hilfsfunktion:
    '''
    jahr = datetime.date.today().year
    stamp = datetime.datetime(
        jahr,
        int(baid[:2]),
        int(baid[2:4]),
        int(baid[4:6]),
        int(baid[6:8]),
        int(baid[8:10]),
        int(baid[10:13])*1000)
    return pd.Timestamp(stamp)

def start_stop(dframe):
    '''Erzeugt neuen DataFrame aus den Start- und Stopzeiten der Tasks
    Columns sind die Transaktionscode.
    Nutzung:
    .resample('1t', how=max)
    '''
    tran_start_stop = pd.concat(
        [
            pd.DataFrame({'TS': dframe.START, 'TRAN': dframe.TRAN, 'tic': 1}),
            pd.DataFrame({'TS': dframe.STOP, 'TRAN': dframe.TRAN, 'tic': -1})
        ])
    tran_start_stop.set_index(tran_start_stop.TS, inplace=True)
    tran_start_stop.sort_index(inplace=True)
    tran_start_stop = tran_start_stop.pivot('TS', 'TRAN', 'tic')
    tran_start_stop.fillna(0, inplace=True)
    return tran_start_stop.cumsum()

def felder(dframe, pattern=None, patdesc=None):
    ''' Liste der Spalten in DataFrame
        pattern : Nur Felder, die mit <pattern> beginnen.
    '''
    lst = list(dframe.columns)
    if patdesc:
        pat = patdesc.upper()
        lst = [feld for feld in lst if pat in
               field_to_desc.get(feld, '-').upper()]
    else:
        if pattern:
            pat = pattern.upper()
            lst = [feld for feld in lst if feld.startswith(pat)]
    return lst
def feldertext(dframe, pattern=None, patdesc=None):
    ''' Beschreibung der Felder ausgeben
        pattern : Nur Felder, die mit <pattern> beginnen.
    '''
    for feld in felder(dframe, pattern=pattern, patdesc=patdesc):
        print("%-10s : %s" % (feld, field_to_desc.get(feld, "-")))
