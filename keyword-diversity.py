import numpy as np
import pandas as pd
import multiprocessing as mp
from nltk.stem.wordnet import WordNetLemmatizer
import empty_module 
from scipy.spatial.distance import pdist 

lem = WordNetLemmatizer()

def get_ref_idx(refs):
    result = []
    for r in refs.split('|'):
        idx = uids.get(r)
        if idx:
            result.append(idx)
    if len(result)>0:
        return result
    else:
        return None


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


def initProcess(share):
    empty_module.d = share

def calc_diversities(input_tuple):
    papers,refs = input_tuples
    paper_diversity = pdist(np.vstack([empty_module.d[i] for i in papers])).mean()
    reference_diversity = pdist(np.vstack([empty_module.d[i] for i in refs])).mean()
    return paper_diversity,reference_diversity

if __name__=='__main__':

    import gzip
    import time,datetime

    d = {}
    print 'building array dict..'
    start = time.time()     

    features = np.load('P:/Projects/WoS/WoS/parsed/abstracts/features-w2v-200.npy')
    print '(array loaded in in {})'.format(str(datetime.timedelta(seconds=time.time()-start)))
    uids = {line.strip():i for i,line in enumerate(gzip.open('P:/Projects/WoS/WoS/parsed/abstracts/uid_indices.txt.gz'))} 

    for i in xrange(len(features)):
        d[i] = mp.Array('d',features[i],lock=False)
    print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

    pool = mp.Pool(mp.cpu_count(),initializer=initProcess,initargs=(d,))

    for year in xrange(1991,2016):
        print 'Beginning processing for {}:'.format(year)
        start = time.time()

        print 'Reading dataframes...'
        start = time.time()
        kw = pd.read_table('P:/Projects/WoS/WoS/parsed/keywords/{}.txt.gz'.format(year),names=['uid','n_kw','keywords'],usecols=['uid','keywords'])
        #metadata = pd.read_table('P:/Projects/WoS/WoS/parsed/metadata/{}.txt.gz'.format(year),
        #        names=['uid','date','pubtype','volume','issue','pages','paper_title','source_title','doctype'],
        #        header=None,usecols=['uid','date'],parse_dates=['date'])
        references = pd.read_table('P:/Projects/WoS/WoS/parsed/references/{}.txt.gz'.format(year),
            names=['uid','n_refs','refs','no_uid'],usecols=['uid','refs']).dropna()
        print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

        print "Cleaning/joining/etc..."
        start = time.time()
        references['refs'] = references['refs'].apply(get_ref_idx)
        kw['keywords'] = kw['keywords'].apply(keyword_parser)
        #metadata['date'] = metadata['date'].apply(lambda x: x.year)
        combined = references.dropna().merge(kw,on='uid')#.merge(metadata,on='uid')
        combind['idx'] = combined['uid'].apply(lambda x: uids.get(x))
        combined = combined.dropna()
        combined['idx'] = combined['idx'].astype(int)
        print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

        print 'Unstacking...'
        rows = []
        for row in combined.itertuples():
            [rows.append([k,[row.idx],row.refs]) for k in row.keywords]
        unstacked = pd.DataFrame(rows)
        unstacked.columns = ['keyword','idx','refs']
        print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

        print 'Aggregating by keyword...'
        start = time.time()
        grp = unstacked.groupby('keyword').sum()    
        print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

        print 'Beginning parallel diversity computations...'
        start = time.time()
        #result = pool.map(calc_diversities,zip(grp['idx'],grp['refs']))
        with gzip.open('P:/Projects/WoS/WoS/data/keyword-diversity/{}.txt.gz','wb') as out:
            for kw,(paper,ref) in zip(grp['idx'],pool.imap_unordered(calc_diversities,zip(grp['idx'],grp['refs']))):
                out.write('\t'.join(map(str,[kw,paper,ref]))+'\n')
        print '...done in {}'.format(str(datetime.timedelta(seconds=time.time()-start)))

        print '----{} complete in {}----'.format(str(datetime.timedelta(seconds=time.time()-start)))        




