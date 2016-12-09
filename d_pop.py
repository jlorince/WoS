import pandas as pd
import multiprocessing as mp
import numpy as np
import time
import datetime
import re
from nltk.stem.wordnet import WordNetLemmatizer
lem = WordNetLemmatizer()


years = np.arange(1991,2016,1).astype(str)
parsed_dir = 'P:/Projects/WoS/WoS/parsed/'
N = mp.cpu_count()

def keyword_parser(kw):
    result = []
    for k in kw.split('|'):
        current = [lem.lemmatize(w) for w in k.replace('-',' ').replace('/',' ').replace('"','').replace("'","").split()]
        if len(current)==0:
            continue
        last = current[-1]
        if last.startswith('(') and last.endswith(')'):
            result+=['.'.join(current[:-1]),last.replace('(','').replace(')','')]
        else:
            result.append('.'.join(current))
    return result


def process_year_keywords(year):

    year_start = time.time()

    
    kw_current = pd.read_table('S:/UsersData_NoExpiration/jjl2228/keywords/pubs_by_year/{}.txt.gz'.format(year),header=None,names=['keyword','uid']).dropna()

    cats_current = pd.read_table('P:/Projects/WoS/WoS/parsed/subjects/{}.txt.gz'.format(year),header=None,names=['uid','heading','subheading','categories'],usecols=['uid','categories'])
    cats_current['topcat'] = cats_current['categories'].apply(lambda x: x.split('|')[0])
    merged = kw_current.merge(cats_current[['uid','topcat']],on='uid')
    
    result = merged.groupby(['topcat','keyword']).count().reset_index()
    result['year'] = year
    result.columns = ['topcat','keyword','freq','year']


    td = str(datetime.timedelta(seconds=time.time()-year_start))
    records = len(result)
    print "{} processed in {} (data length: {})".format(year,td,records)

    return {cat:df for cat,df in result.groupby('topcat')}


def process_year_pubs(year,downsample = True):

    year_start = time.time()

    cats = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','heading','subheading','categories'])
    cats['categories'] = cats['categories'].fillna('')

    if not downsample:
        metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'])

        merged = cats.merge(metadata,on='uid')

        rows = []
        for row in merged.itertuples():
            [rows.append([row.date, row.uid, cat]) for cat in row.categories.split('|')]
        unstacked = pd.DataFrame(rows,columns=['date','uid','category'])
        result = merged.groupby(['date','category']).count().reset_index()
        result.columns = ['date','category','freq']

    else:
        rows = []
        for row in merged.itertuples():
            [rows.append(cat) for cat in row.categories.split('|')]
        result = pd.Series(rows).value_counts().reset_index()
        result.columns = ['category','freq']
        result['date'] = datetime.datetime(year=int(year),month=1,day=1)


    td = str(datetime.timedelta(seconds=time.time()-year_start))
    records = len(result)
    print "{} processed in {} (data length: {})".format(year,td,records)

    return result

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
    #chunksize = int(math.ceil(len(years) / float(N)))
    #final_df = pd.concat(pool.map(process_year_keywords,years))

    final = pool.map(process_year_keywords,years)
    allkeys = set(sum([d.keys() for d in final],[]))
    for k in allkeys:
        current = pd.concat([d[k] for d in final])
        current.to_pickle('P:/Projects/WoS/WoS/data/d_pop_keywords_by_field/{}.pkl'.format(k))


    td = str(datetime.timedelta(seconds=time.time()-overall_start))
    print "Parsing complete  in {} (total data length: {})".format(td, len(final_df))
    #final_df.to_pickle('d_pop_keywords_lem_monthly.pkl')

    #pool.close()


