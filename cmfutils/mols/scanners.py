'''
    Interne Generatoren für das Parsen der Ausgabe von DFH$MOLS
    neu strukturiert, Ermittelung der APPLID kommt aus
'''
import pandas as pd
import numpy as np


def genold_splits(recs):
    """ Hier kommt die Ausgabe von DFH$MOLS rein
      Mindestlaenge ist 60
      Split der Zeile in einzelene Woerter
      yield: list aus applid und split()
    """
    cnt_data = 0
    cnt_rest = 0
    for rec in recs:
        if len(rec) > 60 and rec[:4] == ' '*4:
            cnt_data = cnt_data + 1
            yield rec.split(None, 8)
        else:
            cnt_rest = cnt_rest + 1

    print("splt: {:} / {:}".format(cnt_data, cnt_rest))

def gen_keyvalpairs(listen):
    '''extrahiert aus Zeile Key (Name) und Value (Formatierter Inhalt)
    '''
    hexafields = frozenset(
        'IVASORIG IVASSERV TTYPE TRANFLAG OTRANFLG'.split()
    )

    val = {
        "A": get_counter,
        "S": get_clocktime,
        "C": get_character,
        "X": get_hexa,
        "T": get_timestamp,
        "P": get_packed,
    }

    i = 0
    j = 0
    for liste in listen:
        if len(liste) != 4:
            continue
        if len(liste[0]) > 8:
            continue  # DFHTASK, ...

        feld, nick, content = liste[1:]

        if len(feld) != 4:
            continue  # C001, ...
        feldtyp, feldnum = feld[0], feld[1:]
        if nick in hexafields:
            feldtyp = 'X'

        if feldtyp in val:
            if feldnum in ('095', '033', '054',     # dämliche UDSA below
                           '117'):    # dämliche CDSA below
                nick = nick + '24'
            i = i + 1
            yield (nick, val[feldtyp](content))
            if feldtyp == 'S':
                j = j + 1
                yield (nick + '_CT', stck_to_cnt(content))
    print("pair: {:} / {:}".format(i, j))

def gen_dicts(fields):
    '''Fasse die Felder zu einer Task in ein Dictionary zusammen
    '''
    # Gruppenwechsel ist schwierig: Gruppe startet mit TRAN-Zeile
    # Zunaechst vorruecken bis an erste Tranzeile
    # Dann weiterlesen bis zur naechsten TRAN-Zeile und dict ausliefern
    # usw.

    # Falls fields leer ist oder kein TRAN vorhanden:
    feld, inhalt = None, None

    for feld, inhalt in fields:
        if feld == "TRAN":
            break
    else:   # fields ist leer oder kein TRAN vorhanden
        return

    task = {}
    task[feld] = inhalt
    for feld, inhalt in fields:
        if feld == "TRAN":
            yield task
            task = {}
        task[feld] = inhalt
    yield task            # letzte Gruppe nicht vergessen!

def get_counter(content):
    '''  interpreted 4 oder 8 Bytes
    '''
    return int(content.split()[-1])
def get_clocktime(content):
    ''' Umwandlung STCK in Sekunden
      ' 00000000008AFEA800000011    000 00:00:00.002223  17'
    '''
    stck = content.split()[0]
    return int(stck[:13], 16) / 1E6
def get_character(content):
    '''
      ' E2E8F3E2 D6C3D2D7                              SY3SOCKP'
    '''
    return content.split()[-1]
def get_hexa(content):
    '''
    4 oder 8 Bytes in Hexadarstellung
      ' 8000800004000000'
    '''
    return content.split()[0]
def get_timestamp(content):
    '''
    Time Stamp Date, Time
      'D467BDCF5C83D018                 2018/05/30 14:34:59.255357'
    '''
    interpreted = content.split()[-2:]
    return pd.to_datetime(' '.join(interpreted))
def get_packed(content):
    '''
    Zahl aus Hexa-Darstellung extrahieren
      '0054580C                              54580'
    '''
    hexa_packed = content.split()[0]
    try:
        return int(hexa_packed[-1])
    except ValueError:
        pass
    return np.nan
def stck_to_cnt(content):
    ''' Umwandlung STCK in count
      ' 00000000008AFEA800000011    000 00:00:00.002223  17'
    '''
    cnt = content.split()[-1]
    return int(cnt)
