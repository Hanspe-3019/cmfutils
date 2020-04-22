'''Alle Waittimes laut CICS TS 5.3
'''
from .fields import field_to_desc

ALL_WAIT_TIMES = {
    'TCIOWTT',  #   'Terminal I/O wait time',
    'JCIOWTT',  #   'Journal I/O wait time',
    'TSIOWTT',  #   'Temporary storage I/O wait time',
    'FCIOWTT',  #   'File I/O wait time',
    'IRIOWTT',  #   'Interregion I/O wait time',
    'TDIOWTT',  #   'Transient data I/O wait time',
    'GNQDELAY',  #   'Global ENQ delay time',
    'LMDELAY',  #   'Lock Manager delay time',
    'ENQDELAY',  #   'Local ENQ delay time',
    'LU61WTT',  #   'LU 6.1 I/O wait time',
    'LU62WTT',  #   'LU 6.2 I/O wait time',
    'SZWAIT',  #   'FEPI suspend time',
    'RMISUSP',  #   'Resource manager interface (RMI) suspend time',
    'RLSWAIT',  #   'RLS File I/O wait time',
    'CFDTWAIT',  #   'CF data tables server I/O wait time',
    'SRVSYWTT',  #   'CF data tables server sync point and resynch wait time',
    'TSSHWAIT',  #   'Shared temporary storage I/O wait time',
    'WTEXWAIT',  #   'EXEC CICS WAIT EXTERNAL wait time',
    'WTCEWAIT',  #   'EXEC CICS WAITCICS and WAIT EVENT wait time',
    'ICDELAY',  #   'Interval Control delay time',
    'GVUPWAIT',  #   'Dispatchable Wait wait time',
    'IMSWAIT',  #   'IMS™ DBCTL wait time',
    'DB2RDYQW',  #   'DB2® ready queue wait time',
    'DB2CONWT',  #   'DB2 connection time',
    'RRMSWAIT',  #   'RRMS/MVS indoubt wait time',
    'RQRWAIT',  #   'Request Receiver wait time',
    'RQPWAIT',  #   'Request Processor wait time',
    'RUNTRWTT',  #   'CICS BTS run process/activity synchronous wait time',
    'SYNCDLY',  #   'Sync point delay time',
    'SOIOWTT',  #   'Inbound Socket I/O wait time',
    'DSCHMDLY',  #   'CICS change TCB mode delay time',
    'MXTOTDLY',  #   'CICS L8 and L9 mode open TCB delay time',
    'JVMSUSP',  #   'JVM suspend time',
    'DSTCBMWT',  #   'TCB mismatch wait time',
    'DSMMSCWT',  #   'MVS™ storage constraint wait time',
    'MAXSTDLY',  #   'CICS SSL TCB delay time',
    'MAXXTDLY',  #   'CICS XP TCB delay time',
    'MAXTTDLY',  #   'CICS JVM server thread TCB delay time',
    'PTPWAIT',  #   '3270 bridge partner wait time',
    'SOOIOWT',  #   'Outbound Socket I/O wait time',
    'ISIOWTT',  #   'IS I/O wait time',
    'ISALWTT',  #   'IPIC session allocation wait time',
    'TCALWTT',  #   'MRO, LU6.1, and LU6.2 session allocation wait time',
    'WMQGETWT',  #   'MQ GETWAIT wait time',
    'JVMTHDWT',  #   'JVM server thread wait time',
    'TDILWTT',  #   'Transient Data intrapartition lock wait time',
    'TDELWTT',  #   'Transient Data extrapartition lock wait time',
    'FCXCWTT',  #   'FC wait time for exclusive control of a VSAM CI',
    'FCVSWTT',  #   'FC wait time for a VSAM string',
    'DSAPTHWT',  #   'Dispatcher allocate pthread wait time'
}
def wait_times_observed(dataframe,
        groupby_tran=False,
        describe_fields=False
        ):
    '''Liefert Liste der Waittimes in einem DataFrame
    Optional:
     - Gruppiert nach Transaction Code
     - Ausgabe der Field Descripions
    '''
    def top_trans(series):
        'get TRAN of nlargest'
        index = series.nlargest().index
        return ' '.join(set(dataframe.loc[index].TRAN))
    def pct95(series):
        'compute quantile and give it a name (no lambda)'
        return series.quantile(.95)

    def pct95all(series):
        'compute quantile and give it a name (no lambda)'
        return series.fillna(0).quantile(.95)

    agg_funcs = ['count', pct95all, pct95, 'min', 'max']

    stats = dataframe.groupby('TRAN') if groupby_tran else dataframe

    cols = set(dataframe.columns).intersection(ALL_WAIT_TIMES)
    if describe_fields:
        for k in cols:
            print("{:8s} {:s}".format(
                k, field_to_desc.get(k, k)
                ))

    if not groupby_tran:
        agg_funcs.append(top_trans)

    stats =  stats[list(cols)].agg(agg_funcs)

    stats = stats.stack(level=0) if groupby_tran else stats.T
    return (stats
            .assign(count=lambda row: row['count'].astype('int'))
            .rename(columns=lambda col: col.capitalize())
           )
