'''
    Interne Generatoren für das Parsen der Ausgabe von DFH$MOLS
    neu strukturiert, Ermittelung der APPLID kommt aus
'''
import pandas as pd
import numpy as np

def gen_keyvalpairs(fields):
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

    for ftuple in fields:
        if len(ftuple) != 4:
            continue
        if len(ftuple[0]) > 8:
            continue  # DFHTASK, ...

        field, nick, content = ftuple[1:]

        if len(field) != 4:
            continue  # C001, ...
        fieldtype, fieldnum = field[0], field[1:]
        if nick in hexafields:
            fieldtype = 'X'

        if fieldtype not in val:
            continue

        if fieldnum in ('095', '033', '054',     # dämliche UDSA below
                        '117'):                  # dämliche CDSA below
            nick += '24'

        yield (nick, val[fieldtype](content))
        if fieldtype == 'S':
            yield (nick + '_CT', stck_to_cnt(content))

def gen_dicts(nicks):
    '''Fasse die Felder zu einer Task in ein Dictionary zusammen
    '''
    # Gruppenwechsel ist schwierig: Gruppe startet mit TRAN-Zeile
    # Zunaechst vorruecken bis an erste Tranzeile
    # Dann weiterlesen bis zur naechsten TRAN-Zeile und dict ausliefern
    # usw.

    # Falls nicks leer ist oder kein TRAN vorhanden:
    nick, nick_value = None, None

    for nick, nick_value in nicks:
        if nick == "TRAN":
            break
    else:   # nicks ist leer oder kein TRAN vorhanden
        return

    task = {}
    task[nick] = nick_value
    for nick, nick_value in nicks:
        if nick == "TRAN":
            yield task
            task = {}
        task[nick] = nick_value
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
        return int(hexa_packed[:-1])
    except ValueError:
        pass
    return np.nan
def stck_to_cnt(content):
    ''' Umwandlung STCK in count
      ' 00000000008AFEA800000011    000 00:00:00.002223  17'
    '''
    cnt = content.split()[-1]
    return int(cnt)
