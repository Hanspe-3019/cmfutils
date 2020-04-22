'''
Status: CICS TS 5.4
370 (TYPE-A, 'OTRANFLG', 8 BYTES)
Originating transaction flags, a string of 64 bits used for signaling
transaction definition and status information:

Byte 0 The facility-type of the originating transaction:
    Bit 0 None (X'80')
    Bit 1 Terminal (X'40')
    Bit 2 Surrogate (X'20')
    Bit 3 Destination (X'10')
    Bit 4 3270 bridge (X'08')

Byte 1 Transaction identification information:
    Bit 0 System transaction (x'80')
    Bit 1 Mirror transaction (x'40')
    Bit 2 DPL mirror transaction (x'20')
    Bit 3 ONC/RPC Alias transaction (x'10')
    Bit 4 WEB Alias transaction (x'08')
    Bit 5 3270 Bridge transaction (x'04')
    Bit 6 Reserved (x'02')
    Bit 7 CICS BTS Run transaction

Byte 2 z/OS workload manager request (transaction).

Byte 3 Transaction definition information:
    Bit 0 Taskdataloc = below (x'80')
    Bit 1 Taskdatakey = cics (x'40')
    Bit 2 Isolate = no (x'20')
    Bit 3 Dynamic = yes (x'10')

Byte 4 The type of the originating transaction:
    X'01' None
    X'02' Terminal
    X'03' Transient data
    X'04' START
    X'05' Terminal-related START
    X'06' CICS business transaction services (BTS) scheduler
    X'07' Transaction manager domain (XM)-run transaction
    X'08' 3270 bridge
    X'09' Socket domain
    X'0A' CICS web support (CWS)
    X'0B' Internet Inter-ORB Protocol (IIOP)
    X'0C' Resource Recovery Services (RRS)
    X'0D' LU 6.1 session
    X'0E' LU 6.2 (APPC) session
    X'0F' MRO session
    X'10' External Call Interface (ECI) session
    X'11' IIOP domain request receiver
    X'12' Request stream (RZ) instore transport
    X'13' IP interconnectivity session
    X'14' Event
    X'15' JVM server
    X'16' Asynchronous services domain (AS)-run transaction 

Byte 5 Transaction status information.

Byte 6 Transaction tracking origin data tag.

Byte 7 Recovery manager information:
    Bit 0 Indoubt wait = no
    Bit 1 Indoubt action = commit
    Bit 2 Recovery manager - UOW resolved with indoubt action
    Bit 3 Recovery manager - shunt
    Bit 4 Recovery manager - unshunt
    Bit 5 Recovery manager - indoubt failure
    Bit 6 Recovery manager - resource owner failure
'''

BYTES_DESCSTRINGS = 'FAC ID WLM DEF ORIG STAT ODAT RM'.split()
BYTES_DESCMAP = {i: desc for i, desc in enumerate(BYTES_DESCSTRINGS)}

ORIG_DESCSTRINGS = '''- None Term TD START TSTART BTS XM 3270bridge
Socket CWS IIOP RRS LU61 APPC MRO ECI IIOP RZ IPIC Event JVM AS
'''.split()
ORIG_DESCMAP = {
    '{:02X}'.format(i): desc for i, desc in enumerate(ORIG_DESCSTRINGS)
}

def format_tranflag(df):
    '''return dataframe with formatted Tranflag columns
    '''
    # generate Data Frame with 8 columns for Bytes 0 to 7
    result = df.TRANFLAG.str.extractall(r'(.{2})').unstack()
    # rename the resulting Mulitindex columns:
    result.columns = ['TFLAG_' + BYTES_DESCMAP[i]
        for i in result.columns.get_level_values(1)
    ]
    result.TFLAG_ORIG = result.TFLAG_ORIG.map(ORIG_DESCMAP)
    result.TFLAG_ID = result.TFLAG_ID.map(format_tranidentinfo)
    result.TFLAG_DEF = result.TFLAG_DEF.map(format_trandefinfo)
    result.TFLAG_FAC = result.TFLAG_FAC.map(format_facilitytype)

    return result


def format_facilitytype(hexastring):
    '''
    '''
    i = int(hexastring, base=16)
    return ''.join([
        'none' if i & 0x80 else '',
        'term' if i & 0x40 else '',
        'surr' if i & 0x20 else '',
        'dest' if i & 0x10 else '',
        'bridge' if i & 0x08 else ''
        ])
def format_tranidentinfo(hexastring):
    '''
    '''
    i = int(hexastring, base=16)
    return ''.join([
        'sys' if i & 0x80 else '',
        'mir' if i & 0x40 else '',
        'dpl' if i & 0x20 else '',
        'rpc' if i & 0x10 else '',
        'web' if i & 0x08 else '',
        'bri' if i & 0x04 else '',
        'bts' if i & 0x01 else '',
        ])
def format_trandefinfo(hexastring):
    '''
    '''
    i = int(hexastring, base=16)
    return ''.join([
        'low ' if i & 0x80 else '',
        'cic ' if i & 0x40 else '',
        'no ' if i & 0x20 else '',
        'dyn' if i & 0x10 else ''
        ])
