'''Optimierung des CMF-Dataframe durch Nutzung von Category
'''
from timeit import default_timer as timer
import io
import pandas as pd

def mig_coltypes_to_categories(df_old, verbose=True):
    '''
        Wandelt dtype von object to category um.
        Jedes float64 und int64 und datatime64 belegen 8 Bytes,
        die objects belegen mehr Platz i.a. sind das Strings.
        Wenn es nur wenige Textausprägungen gibt, kann man das Attribut
        sinnvoll in eine category umwandeln.
    '''
    start = timer()
    mem_usage = df_old.memory_usage(deep=True)
    mem_usage_high = mem_usage[mem_usage > len(df_old)*8]
    objects_to_category = pd.DataFrame(
        (
            df_old[mem_usage_high.index].nunique(),
            df_old[mem_usage_high.index].count(),
        )
    ).T.rename(
        columns={
            0: 'Uniques',
            1: 'Count'
        }
    ).assign(
        Ratio=lambda row: row.Uniques/row.Count,
    ).loc[lambda row: row.Ratio < .01]

    to_category = {col: 'category' for col in objects_to_category.index}
    df_new = df_old.astype(to_category)
    if verbose:
        print(f'Changed to category in {timer()-start:.3f} secs')
        print(
            f'Memory Usage old: {memusage(df_old)}, '
            f'new: {memusage(df_new)}'
        )

    return df_new

def memusage(dframe):
    ''' Memory Usage als Dictionary

        df.info(memory_usage='deep')
        <class 'pandas.core.frame.DataFrame'>
        RangeIndex: 99750 entries, 0 to 99749
        Columns: 106 entries, TRAN to RESP
        dtypes: datetime64[ns](3), float64(70), int64(11), object(22)
        memory usage: 178.7 MB
    '''
    with io.StringIO() as trap:
        dframe.info(memory_usage='deep', buf=trap)
        trap.seek(0)
        dfinfo = dict(
            tuple(line.split(':', maxsplit=1))
            for line in trap.readlines()
            if ':' in line
        )
    return dfinfo['memory usage'].strip()
