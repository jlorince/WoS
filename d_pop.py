import pandas as pd
import multiprocessing as mp
import numpy as np
import time
import datetime


years = np.arange(1950,2016,1).astype(str)
parsed_dir = 'P:/Projects/WoS/WoS/parsed/'
N = mp.cpu_count()

def process_year_pubs(year):

    year_start = time.time()

    metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'])

    cats = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','heading','subheading','categories'])
    cats['categories'] = cats['categories'].fillna('')

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
    records = len(resampled)
    print "{} processed in {} (data length: {})".format(year,td,records)

    return resampled

def process_year_refs(year):

    year_start = time.time()

    metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'])

    refs = pd.read_table('{}references/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','total_refs','refs','missing_refs'],usecols=['uid','refs'])
    refs['refs'] = refs['refs'].fillna('')

    cats = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','heading','subheading','categories'],usecols=['uid','categories'])
    cats['categories'] = cats['categories'].fillna('')

    merged = refs.merge(metadata,on='uid')

    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date,ref]) for ref in row.refs.split('|')]
        except Exception as e:
            print row
            raise(e)

    merged = pd.DataFrame(rows,columns=['date','uid']) # NOTE THAT HERE UID IS THE UID OF A REFERENCED PAPER!

    merged = merged.merge(cats,on='uid')

    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date, row.uid, cat]) for cat in row.categories.split('|')]
        except Exception as e:
            print row
            raise(e)

    final = pd.DataFrame(rows,columns=['date','ref','category'])

    resampled = merged.groupby(['date','category']).count()

    td = str(datetime.timedelta(seconds=time.time()-year_start))
    records = len(resampled)
    print "{} processed in {} (data length: {})".format(year,td,records)

    return resampled

if __name__ == '__main__':

    overall_start = time.time()

    pool = mp.Pool(N)
    final_df = pd.concat(pool.map(process_year_refs,years))
    td = str(datetime.timedelta(seconds=time.time()-overall_start))
    print "Parsing complete  in {} (total data length: {})".format(td, len(final_df))
    final_df.to_pickle('d_pop.pkl')

    pool.close()


