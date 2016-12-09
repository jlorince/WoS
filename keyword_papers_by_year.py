import numpy as np
import pandas as pd
import multiprocessing as mp
from nltk.stem.wordnet import WordNetLemmatizer
from scipy.spatial.distance import pdist
import gzip
import time,datetime,csv
import string

lem = WordNetLemmatizer()


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


def process_year(year):
        start = time.time()

        kw = pd.read_table('P:/Projects/WoS/WoS/parsed/keywords/{}.txt.gz'.format(year),names=['uid','n_kw','keywords'],usecols=['uid','keywords'],quoting=csv.QUOTE_NONE)
        kw['keywords'] = kw['keywords'].apply(keyword_parser)

        n=0
        with gzip.open('S:/UsersData_NoExpiration/jjl2228/keywords/pubs_by_year/{}.txt.gz'.format(year),'wb') as out:
            for row in kw.itertuples():
                for k in row.keywords:
                    out.write("{}\t{}\n".format(k,row.uid))
                    n+=1
        print '----{} complete in {} ({} records)----'.format(year,str(datetime.timedelta(seconds=time.time()-start)),n)


if __name__=='__main__':

    pool = mp.Pool(mp.cpu_count())
    pool.map(process_year,xrange(1991,2016))