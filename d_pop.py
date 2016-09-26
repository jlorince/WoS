import pandas as pd
import multiprocessing as mp
import numpy as np
import time
import datetime


years = np.arange(1950,2016,1).astype(str)
parsed_dir = 'P:/Projects/WoS/WoS/parsed/'
N = mp.cpu_count()


def process_year_keywordw(year):

    year_start = time.time()

    metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'])

    kw = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','n_keywords','keywords'])
    kw['keywords'] = kw['keywords'].fillna('')

    merged = cats.merge(metadata,on='uid')

    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date, row.uid, k]) for k in row.keywords.split('|')]
        except Exception as e:
            print row
            raise(e)

    merged = pd.DataFrame(rows,columns=['date','uid','keyword'])

    resampled = merged.groupby(['date','keyword']).count()

    td = str(datetime.timedelta(seconds=time.time()-year_start))
    records = len(resampled)
    print "{} processed in {} (data length: {})".format(year,td,records)

    return resampled    


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

    metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'],quoting=3)
    #p#rint 'metadata: {}'.format(len(metadata))

    refs = pd.read_table('{}references/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','total_refs','refs','missing_refs'],usecols=['uid','refs'])
    refs['refs'] = refs['refs'].fillna('')
    #print 'refs: {}'.format(len(refs))

    cats = pd.concat([pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,y),compression='gzip',header=None,names=['uid','heading','subheading','categories'],usecols=['uid','categories']) for y in xrange(1950,int(year)+1)])
    cats['categories'] = cats['categories'].fillna('')
    #print 'cats: {}'.format(len(cats))

    merged = refs.merge(metadata,on='uid')
    #print 'refs merged with metadata: {}'.format(len(merged))

    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date,ref]) for ref in row.refs.split('|')]
        except Exception as e:
            print row
            raise(e)

    merged = pd.DataFrame(rows,columns=['date','uid']) # NOTE THAT HERE UID IS THE UID OF A REFERENCED PAPER!
    #print 'refs merged with metadata, unpacked: {}'.format(len(merged))    

    merged['cnt'] = 1
    merged = merged.groupby(['date','uid']).sum().reset_index()
    #print 'reference counts: {}'.format(len(merged))

    merged = merged.merge(cats,on='uid')
    #print 'merged with categories: {}'.format(len(merged))


    rows = []
    for row in merged.itertuples():
        try:
            [rows.append([row.date,cat,row.cnt]) for cat in row.categories.split('|')]
        except Exception as e:
            print row
            raise(e)

    final = pd.DataFrame(rows,columns=['date','category','cnt'])
    #print 'merged with categories (unpacked): {}'.format(len(final))

    resampled = final.groupby(['date','category']).sum()
    #print 'resampled: {}'.format(len(resampled))

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
    final_df.to_pickle('d_pop_refs.pkl')

    pool.close()


