import pandas as pd
import multiprocessing as mp
import numpy as np
import time
import datetime


years = np.arange(1950,2016,1).astype(str)
parsed_dir = 'P:/Projects/WoS/WoS/parsed/'
N = mp.cpu_count()

def process_year(year):

    year_start = time.time()

    metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'])

    cats = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','heading','subheading','categories'])
    cats['categories'].fillna('')

    merged = cats.merge(metadata,on='uid')

    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date, row.uid, cat]) for cat in row.categories.split('|')]
        except Exception as e:
            print row
            raise(e)

    merged = pd.DataFrame(rows,columns=['date','uid','category'])

    resampled = merged.groupby(['date','category']).count()

    td = str(datetime.timedelta(seconds=time.time()-year_start))
    print "{} processed in {} (data length: {})".format(year,td)

    return resampled

if __name__ == '__main__':

    overall_start = time.time()

    pool = mp.Pool(N)
    final_result = pd.concat(pool.map(process_year,years))
    td = str(datetime.timedelta(seconds=time.time()-overall_start))
    log_handler("Parsing complete: {} total records processed in {}".format(sum(record_count),td))

    pd.to_pickle('d_pop.pkl')

    pool.close()


