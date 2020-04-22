'''Shows how to set tics in order to analyse events
'''
import pandas as pd
import numpy as np

def get_akcc_tclsname(df):
    '''Bei einem AKCC-Abend ist TCLSNAME nicht gefüllt! Komisch!?
    '''
    try:
        tran_with_akcc = df.loc[df.ABCODEO=='AKCC', 'TRAN'].unique()
    except AttributeError:  # keine Abend in den Daten, schön
        return {}
    try:
        _ = df.TCLSNAME
    except AttributeError: # keine TRANCLASS in den Datem!?
        return {}

    akcc_tclsname = dict(
        df.loc[
            df.TRAN.isin(tran_with_akcc), 'TRAN TCLSNAME'.split()
        ]
        .groupby('TRAN')
        .TCLSNAME.first()
    )
    return akcc_tclsname
def get_start_stop_events(df):
    '''returns new Data Frame suitable for event time Series analysis of
    START, STOP and TCLDELAY
    Result is indexed by Timestamp of Event
    and contains these columns:
     - TRAN     : Transaction Code
     - TCLSNAME : Transaction Class name
     - ID       : Index of corresponding data in original Data Frame
     - waiting  : Entered but Waiting for first dispatch
     - execing  : First dispatched
     - purged   : Purged after Entering (AKCC-Abend)

    Possible usage:

        pd.merge(events, df.loc[:, 'RESP IRIOWTT'.split()],
            left_on='ID',
            right_index=True
            )
    '''

    # Die effektive StartTime: START + TCLDELAY
    disptime = df.START + pd.to_timedelta(
        df.TCLDELAY.fillna(0), unit='s'
        )

    mask_no_delay = df.TCLDELAY.isna()
    base_cols = 'TRAN TCLSNAME'.split() 

    event_has_to_wait = (df
            .loc[~ mask_no_delay, base_cols]
            .assign(
                TS=df.START,
                TCLDELAY=df.TCLDELAY,
                execing=0,
                waiting=1,
                arrived=True,
                queued=True,
                )
           )

    event_start_running = (df
             .loc[:, base_cols]
             .assign(
                 TS=disptime,
                 waiting=np.where(mask_no_delay, 0, -1),
                 arrived=np.where(mask_no_delay, True, False),
                 execing=1,
             )
            )

    event_finished = (df
            .loc[:, base_cols]
            .assign(
                TS=df.STOP,
                waiting=0,
                execing=-1,
                purged = np.where(df.ABCODEO == 'AKCC', True, False)
            )
           )

    concat = (
        pd.concat([
            event_has_to_wait,
            event_start_running,
            event_finished],
            sort=True
            )

        .assign(ID=lambda x: x.index)
        .set_index('TS')
        .sort_index()
        )

    tran_to_tclsname = get_akcc_tclsname(df)
    ohne_tclsname = concat.TCLSNAME.isna()
    concat.loc[ohne_tclsname, 'TCLSNAME'] = (
        concat.loc[ohne_tclsname, ['TRAN']]
            .apply(
                lambda row: tran_to_tclsname.get(row.TRAN, '**NONE**'),
                axis=1
            )
        )
    return concat
class Task_Events(object):
    '''Hält die Daten aus einem CMF-Dataframe als Event-Dataframe mit
        Index Timestamp der Events:
        - Arrival with TCLDELAY
        - Start Running (immediately or after TCLDELAY
        - Finished: EOT
    '''
    def __init__(self, df):
        self.ev = get_start_stop_events(df)
        self.df = df
        self.duration = df.STOP.max() - df.STOP.min()
        self.size = len(df)
        self.tran_count = len(df.TRAN.unique())
    def __repr__(self):
        return "{:d} Transaktionen in {:.1f} Minuten, {:.1f} TX/s".format(
            self.size,
            self.duration.seconds/60,
            self.size/self.duration.seconds,
            )
    def arrivals_by_tclsname(self,
           startswith=None,
           isin=None,
           resample=None,
        ):
        '''
        returns Data Frame grouped by TCLSNAME
        '''
        mask = self.ev.arrived == 1
        if startswith:
            mask = mask & self.ev.TCLSNAME.str.startswith(startswith)
        if isin:
            mask = mask & self.ev.TCLSNAME.isin(isin)
        if resample:
            groupby = self.ev.loc[mask].groupby([
                'TCLSNAME',
                pd.Grouper(level=0, freq=resample),
            ])
        else:
            groupby = self.ev.loc[mask].groupby('TCLSNAME')
        return groupby.arrived.sum()

    def by_tclsname(self,
           startswith=None,
           isin=None,
           resample=None,
        ):
        '''
        returns Data Frame grouped by TCLSNAME
        '''
        mask = [True] * len(self.ev)  
        if startswith:
            mask = mask & self.ev.TCLSNAME.str.startswith(startswith)
        if isin:
            mask = mask & self.ev.TCLSNAME.isin(isin)
        temp = self.ev.loc[mask].copy()

        if resample:
            groupby = temp.groupby([
                'TCLSNAME',
                pd.Grouper(level=0, freq=resample),
            ])
        else:
            groupby = temp.groupby('TCLSNAME')

        temp.loc[:, 'hw_executing'] = groupby.execing.cumsum()
        temp.loc[:, 'hw_queued'] = groupby.waiting.cumsum()
        return groupby.agg({
                'arrived': 'sum',
                'purged': 'sum',
                'hw_executing': 'max',
                'queued': 'sum',
                'hw_queued': 'max',
                'TCLDELAY': 'min mean max'.split(),
                }).fillna(0)


    def by_tran(self,
           startswith=None,
           isin=None,
           resample=None,
        ):
        '''
        returns Data Frame grouped by TRAN
        '''
        mask = [True] * len(self.ev)  
        if startswith:
            mask = mask & self.ev.TRAN.str.startswith(startswith)
        if isin:
            mask = mask & self.ev.TRAN.isin(isin)
        temp = self.ev.loc[mask].copy()

        if resample:
            groupby = temp.groupby([
                'TRAN',
                pd.Grouper(level=0, freq=resample),
            ])
        else:
            groupby = temp.groupby('TRAN')

        temp.loc[:, 'hw_executing'] = groupby.execing.cumsum()
        temp.loc[:, 'hw_queued'] = groupby.waiting.cumsum()
        return groupby.agg({
                'arrived': 'sum',
                'purged': 'sum',
                'hw_executing': 'max',
                'queued': 'sum',
                'hw_queued': 'max',
                'TCLDELAY': 'min mean max'.split(),
                }).fillna(0)



