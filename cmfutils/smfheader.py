''' Analyze DFH$MOLS Output file
'''
from functools import wraps
from collections import defaultdict
from collections import namedtuple

def analyze_molsoutput(path):
    ''' Anaylyze structure of Outputfile
    '''
    state = namedtuple(
        'State',
        'molsheader_footer'
        'smfrec_header'
        'product_section'
    )
    result = namedtuple(
        'Result',
        'rectypes'
        'header_infos'
        'mols_header_footer'
    )
    result.rectypes = defaultdict(int)
    result.header_infos = []
    result.mols_header_or_footer = []
    state.molsheader_or_footer = True
    state.smfrec_header = False
    state.product_section = False

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
        state.product_section = False

#   if line.startswith(b'12345678901234...'):
    elif line.startswith(' '*14 + '...'):
        result.rectypes['...LINE'] += 1

    elif line.startswith('1*********'):
        #  Begin new SMF Record
        result.rectypes['SMFREC'] += 1
        state.molsheader_or_footer = False
        state.smfrec_header = True
        smfheader = {}
        proc = fill_header_info(smfheader)
        breakpoint()

    elif state.molsheader_or_footer:
        result.mols_header_or_footer.append(line[1:])

    elif line.startswith(' -----'):
        # Begin Task Data
        result.rectypes['TRANREC'] += 1
        state.molsheader_or_footer = False
        state.smfrec_header = False
        state.product_section = False

    elif state.smfrec_header:
        breakpoint()
        if line.startswith(' **********************'):
            # Ende SMF-Header, Start Product Section
            result.header_infos.append(smfheader)
            result.rectypes['PRODSECTION'] += 1
            state.smfrec_header = False
            state.product_section = True
            prodsect = {}
            proc = fill_product_section(prodsect)
        else:
            proc.send(line)

    elif state.product_section:
        if line.startswith(' **********************'):
            # Ende Product Section
            state.product_section = False
            result.product_section.append(prodsect)
        else:
            proc.send(line)

    elif line.startswith(' '):
        result.rectypes['DATA'] += 1

    elif line.startswith('1***  DFH$MOLS'):
        state.molsheader_or_footer = True

def coroutine(func):
    """Decorator: primes `func` by advancing to first `yield`"""
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer

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
    while True:
        line = (yield)
        tsplit = line.split('TIME = ')
        breakpoint()
        if len(tsplit) == 2:
            tsplit = tsplit[1].strip().split(' ', maxsplit=1)
            info['TIME'] = tsplit[0]
            tsplit = tsplit[1].split('DATE = ', maxsplit=1)
            tsplit = tsplit[1].strip().split(' ', maxsplit=1)
            info['DATE'] = tsplit[0]
            tsplit = tsplit[1].split('SYSTEM-ID = ', maxsplit=1)
            tsplit = tsplit[1].strip().split(' ', maxsplit=1)
            info['LPAR'] = tsplit[0]

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
        tsplit = line.split('JOB NAME = ')
        if len(tsplit) == 2:
            tsplit = tsplit[1].split(' ', maxsplit=1)
            info['JOBNAME'] = tsplit[0]
            return
        tsplit = line.split('COMPRESSED DATA LENGTH = ')
        if len(tsplit) == 2:
            tsplit = tsplit[1].split(' ', maxsplit=1)
            info['COMPLEN'] = int(tsplit[0])
