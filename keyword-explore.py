import pandas as pd
import multiprocessing as mp
import numpy as np
import time
import datetime


years = np.arange(1950,2016,1).astype(str)
parsed_dir = 'P:/Projects/WoS/WoS/parsed/'
N = mp.cpu_count()

keyword = 'genomics'

def process_year_keywords(year,downsample=True):

    year_start = time.time()

    
    kw = pd.read_table('{}keywords/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','n_keywords','keywords'],quoting=3)
    kw['keywords'] = kw['keywords'].fillna('').apply(lambda x: [k.lower() for k in x.split('|')] if keyword in x else None)
    kw = kw.dropna()

    cats = pd.read_table('{}subjects/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','heading','subheading','categories'])
    cats['categories'] = cats['categories'].fillna('')

    if not downsample:
        metadata = pd.read_table('{}metadata/{}.txt.gz'.format(parsed_dir,year),compression='gzip',header=None,names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],usecols=['uid','date'],parse_dates=['date'],quoting=3)
        merged = kw.merge(metadata,on='uid')

        rows = []
        for row in merged.itertuples():
            ks = set()
            for k in row.keywords.split('|'):
                ks.add(k.lower())
        
            [rows.append([row.date, row.uid, k]) for k in ks]
        unstacked = pd.DataFrame(rows,columns=['date','uid','keyword'])
        result = unstacked.groupby(['date','keyword']).count().reset_index()
        result.columns = ['date','keyword','freq']

    else:
        # handle in parens keywords
        rows = []
        for row in kw.itertuples():        
            [rows.append(k) for k in row.keywords.split('|')]
        result = pd.Series(rows).value_counts().reset_index()
        result.columns = ['keyword','freq']
        result['date'] = datetime.datetime(year=int(year),month=1,day=1)