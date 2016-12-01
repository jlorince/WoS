import gzip,redis,time,datetime,string
import multiprocessing as mp
import pandas as pd
import numpy as np
from glob import glob

r = redis.StrictRedis(host='localhost', port=6379, db=0)

keyword_dir = '/backup/home/jared/keywords/parsed/'

global_vocab = pd.Index(open(keyword_dir+'vocab').read().split())



"""
Support functions - ALWAYS NORMALIZE FIRST
"""

# def simpson_index(arr):
#     return 1 - a_entropy(arr,2)
#     return (arr**2).sum()

def a_entropy(arr,alpha=2):
    return (1./(1.-alpha)) * ((arr**alpha).sum()-1.0)

def div(p,q,alpha=2):
    return a_entropy((p+q)/2.,alpha)-.5*(a_entropy(p,alpha))-.5*(a_entropy(q,alpha))

def div_max(p,q,alpha=2):
    return (((2.0**(1.-alpha)) - 1.0)/2.) * (a_entropy(p,alpha)+a_entropy(q,alpha)+(2./(1.-alpha)))

def div_norm(p,q,alpha=2):
    return div(p,q,alpha) / div_max(p,q,alpha)



def process_keyword(fi):
    
    df = pd.read_pickle(fi)
    dicts = []

    last = None
    for year,data in df.groupby('year'):

        print year
        
        """
        ENTROPY MEASURES
        """

        # concatenate all text
        abstracts = []
        for uid in data['uid']:
            a = r.get(uid)
            if a is not None:
                abstracts.append(a)
        n_abs = len(abstracts)
        if n_abs>0:
            alltext = ' '.join(abstracts).split()
            
            # reindex by global vocab and convert to simple array
            text_freqs = pd.Series(alltext).value_counts().reindex(global_vocab,fill_value=0.).values
            # normalize
            dist = text_freqs / float(text_freqs).sum()

            entropy = a_entropy(dist,2)
            simpson = 1-entropy

            if last is not None:
                divergence = div_norm(last,dist,2)

        else:
            last = None
            divergence = np.nan
            entropy = np.nan
            simpson = np.nan

        n = len(data)


        dicts.append({val:vars()[var] for var in ['year','divergence','entropy','simpson','n','n_abs']})

    result = pd.DataFrame(dicts).set_index('year').reindex(xrange(1991,2016))

    return result



if __name__=='__main__':
    import math

    keyword_files = glob(keyword_dir+'*')

    n_procs = mp.cpu_count()
    chunksize = int(math.ceil(len(keyword_files) / float(n_procs)))

    pool = mp.Pool(process_keyword,keyword_files,chunksize=chunksize)

    dfs = pool.map(process_keyword)
    final = pd.concat(dfs)

    final.to_pickle('/backup/home/jared/keywords/processed.pkl')

