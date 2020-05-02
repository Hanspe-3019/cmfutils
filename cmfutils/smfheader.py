''' Analyze DFH$MOLS Output file
'''
from enum import Enum
from functools import wraps
from collections import Counter
from collections import namedtuple
class Where(Enum):
    ''' Beschreibt die aktuelle Satzart
    '''
    PROLOG = 1
    SMFHEAD = 2
    PRODSECT = 3
    TASKDATA = 4
    EPILOG = 9

class Result(
        namedtuple(
            '_Result',
            'rectypes '
            'smfrecs '
            'smfprods '
            'mols_prolog '
            'mols_epilog ',
            defaults=(
                Counter(), [], [], [], [],
            )
        )
):
    ''' Ergebnis als Named Tuple
    '''
    def __repr__(self):
        return (
            f'Result: {len(self.smfrecs)} Records, '
            f'{self.rectypes["TRANREC"]} transactions.'
        )
    def total(self):
        ''' Print Total Report from DFH$MOLS
        '''
        for line in self.mols_epilog:
            print(line[5:])

    def selection(self):
        ''' Print Selection Criteria of DFH$MOLS run.
        '''
        for line in self.mols_prolog:
            print(line[:60])

def analyze_molsoutput(path):
    ''' Analyze structure of Outputfile
    '''
    state = namedtuple(
        'State',
        'proc '             # current staged coroutine
        'where '            # current Where in file
        'smfheaderdata '    # current smfheader data
        'prodsectdata '     # current product section data
    )
    state.where = Where.PROLOG
    result = Result()

    with open(path, errors='ignore') as molsfile:
        for line in molsfile:
            line = line.rstrip()
            process_line(line, state, result)

    return result

def process_line(line, state, result):
    ''' Wo sind wir?
    '''
    if len(line) < 10:
        result.rectypes['SHORT'] += 1

    elif line.startswith(' '*14 + '...'):
        result.rectypes['...LINE'] += 1

    elif line.startswith('1*********'):
        #  Begin new SMF Record
        result.rectypes['SMFREC'] += 1
        state.where = Where.SMFHEAD
        state.smfheader = {}
        state.proc = fill_header_info(state.smfheader)

    elif state.where == Where.PROLOG:
        result.mols_prolog.append(line[1:])

    elif state.where == Where.EPILOG:
        result.mols_epilog.append(line[1:])

    elif line.startswith(' -----'):
        # Begin Task Data
        result.rectypes['TRANREC'] += 1

    elif state.where == Where.SMFHEAD:
        if line.startswith(' **********************'):
            # Ende SMF-Header, Start Product Section
            result.smfrecs.append(state.smfheader)
            result.rectypes['PRODSECTION'] += 1
            state.where = Where.PRODSECT
            state.prodsect = {}
            state.proc = fill_product_section(state.prodsect)
        else:
            state.proc.send(line)

    elif state.where == Where.PRODSECT:
        if line.startswith(' **********************'):
            # Ende Product Section
            state.where = Where.TASKDATA
            result.smfprods.append(state.prodsect)
        else:
            state.proc.send(line)

    elif line.startswith(' '):
        result.rectypes['DATA'] += 1

    elif line.startswith('1***  DFH$MOLS'):
        state.where = Where.EPILOG

def coroutine(func):
    """Decorator: primes `func` by advancing to first `yield`"""
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer

def word_behind(line, behind):
    ''' Helper: Wort nach behind-String
        None, wenn nicht gefunden
    '''
    try:
        after_behind = line.split(behind, maxsplit=1)[1].strip()
    except IndexError:
        return None

    try:
        after_word = after_behind.split(' ')[0]
    except IndexError:
        return after_behind
    return after_word

@coroutine
def fill_header_info(info):
    '''Fische interessante Angaben aus dem SMF Header

    *  TIME = 14:35:00.18    DATE = 2018/150   SYSTEM-ID = IVP1
                               SUBSYS-ID = CICS    OPSYS = X'DE'(MVS/ESA)   *
    *  REC TYPE = 110   REC SUBTYPE = 1    NO OF TRIPLETS = 2
                                                   OPSYS LEVEL = SP7.2.2    *
    *  PROD SECT OFFSET = 44     PROD SECT LENGTH = 114
                       NUM OF PROD SECTS = 1                                *
    *  DATA SECT OFFSET = 158    DATA SECT LENGTH = 32030
                       NUM OF DATA SECTS = 1                                *
    '''
    first_line = (yield)
    if val := word_behind(first_line, 'TIME ='):
        info['TIME'] = val
    if val := word_behind(first_line, 'DATE ='):
        info['DATE'] = val
    if val := word_behind(first_line, 'SYSTEM-ID ='):
        info['LPAR'] = val

    while True:
        _ = (yield)

@coroutine
def fill_product_section(info):
    '''Fische interessante Angaben aus der Product Section

    *  REC VERSION = 0710  REC MAINT IND = 0    G-APPLID = CICPRD0
                                   S-APPLID = CICPRD0    DATA CLASS = 3      *
    *  JOB NAME = CICPRD0    ENTRY DATE = 2018/150
                  ENTRY TIME =  4:02:12.04   USER IDENTIFICATION =           *
    *  1ST CONN. OFFSET = 158    CONNECTOR LENGTH = 2
                        NUM OF CONNECTORS = 125                              *
    *  1ST DATA OFFSET  = 408    DATA ROW LENGTH  = 908
                        NUM OF DATA ROWS  = 35                               *
    *  COMPRESSED DATA LENGTH = 13006
                                             MCT OPTIONS = X'60'             *
    *  LOCAL TIME ZONE = 00001AD2  LEAP SECOND OFFSET = 00000000 00000000
                                        DATE/TIME OFFSET = 00001AD2 74800000 *
    '''
    while True:
        line = (yield)
        if val := word_behind(line, 'JOB NAME = '):
            info['JOBNAME'] = val
        if val := word_behind(line, 'DATA CLASS = '):
            info['CLASS'] = int(val)
        if val := word_behind(line, ' ROW LENGTH  = '):
            info['ROWLEN'] = int(val)
        if val := word_behind(line, 'DATA ROWS  = '):
            info['ROWS'] = int(val)
        if val := word_behind(line, 'COMPRESSED DATA LENGTH = '):
            info['COMPLEN'] = int(val)
