'''
    Interne Generatoren für das Parsen der Ausgabe von DFH$MOLS
    neu strukturiert, Ermittelung der APPLID kommt aus
'''
import pandas as pd
import numpy as np
import timeit


def gen_splits(recs):
    """ Hier kommt die Ausgabe von DFH$MOLS rein
      Mindestlaenge ist 60
      Split der Zeile in einzelene Woerter
      yield: list aus applid und split()
    """
    cnt_data  = 0
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
    XFIELDS = 'IVASORIG IVASSERV TTYPE TRANFLAG OTRANFLG'.split()

    def packed(hexa_packed):
        '''Zahl aus Character-Darstellung (letzte Spalte) extrahieren
        '''
        try:
            return int(hexa_packed[-1])
        except ValueError:
            pass
        return np.nan
    def stck_to_sec(stck):
        ''' Umwandlung STCK in Sekunden
        '''
        # 0000 0000 041A 587E 0000 0009
        return int(stck[:13], 16) / 1E6
    def stck_to_cnt(stck):
        ''' Umwandlung STCK in count
        '''
        # 01234567 89012345 67 890123
        # 00000000 041A587E 00 000009
        return int(stck[18:], 16)

    val = {"A": # counter
                lambda liste: int(liste[4]),
           "S": # Clock Time
               lambda liste: stck_to_sec(liste[3]),
           "C": # Character
               lambda liste: liste[-1],
           "X": # 4 oder 8 Bytes in Hexadarstellung
               lambda liste: liste[3],
           "T": # Time Stamp Date, Time
               lambda liste: pd.to_datetime(' '.join(liste[-2:])),
           "P": packed # Packed Decimal
    }

    i = 0
    j = 0
    for liste in listen:
        if len(liste) < 4:
            continue
        if len(liste[0]) > 8:
            continue  # DFHTASK, ...

        feld = liste[1]
        nick = liste[2]

        if len(feld) != 4:
            continue  # C001, ...
        feldtyp, feldnum = feld[0], feld[1:]
        if nick in XFIELDS:
            feldtyp = 'X'

        if feldtyp in val:
            if feldnum in ('095', '033', '054',     # dämliche UDSA below
                           '117'):    # dämliche CDSA below
                nick = nick + '24'
            i = i + 1
            yield (nick, val[feldtyp](liste))
            if feldtyp == 'S':
                j = j + 1
                yield (nick + '_CT', stck_to_cnt(liste[3]))
    print("pair: {:} / {:}".format(i, j))

def gen_dicts(fields):
    '''Fasse die Felder zu einer Task in ein Dictionary zusammen
    '''
    # Gruppenwechsel ist schwierig: Gruppe startet mit TRAN-Zeile
    # Zunaechst vorruecken bis an erste Tranzeile
    # Dann weiterlesen bis zur naechsten TRAN-Zeile und dict ausliefern
    # usw.

    # Falls fields leer ist oder kein TRAN vorhanden:
    feld, inhalt =  None, None

    for feld, inhalt in fields:
        if feld == "TRAN":
            break
    else:   # fields ist leer oder kein TRAN vorhanden
        raise StopIteration

    task = {}
    task[feld] = inhalt
    for feld, inhalt in fields:
        if feld == "TRAN":
            yield task
            task = {}
        task[feld] = inhalt
    yield task            # letzte Gruppe nicht vergessen!
