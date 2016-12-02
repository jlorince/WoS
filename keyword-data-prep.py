import numpy as np
import gzip,time,datetime,string,signal,sys,cPickle
import pandas as pd
import multiprocessing as mp
from nltk.stem.snowball import EnglishStemmer
stemmer = EnglishStemmer()

kwdir = 'S:/UsersData_NoExpiration/jjl2228/keywords/parsed/'
tmpdir = 'S:/UsersData_NoExpiration/jjl2228/keywords/temp/'

debug = 10000

class timed(object):
    def __init__(self,desc='command',pad='',**kwargs):
        self.desc = desc
        self.kwargs = kwargs
        self.pad = pad
    def __enter__(self):
        self.start = time.time()
        print '{} started...'.format(self.desc)
    def __exit__(self, type, value, traceback):
        if len(self.kwargs)==0:
            print '{}{} complete in {}{}'.format(self.pad,self.desc,str(datetime.timedelta(seconds=time.time()-self.start)),self.pad)
        else:
            print '{}{} complete in {} ({}){}'.format(self.pad,self.desc,str(datetime.timedelta(seconds=time.time()-self.start)),','.join(['{}={}'.format(*kw) for kw in self.kwargs.iteritems()]),self.pad)

def gen_series(s):
    ser = pd.Series(index=['cat{}'.format(i) for i in xrange(8)])
    if pd.isnull(s):
        return ser
    else:
        li = s.split('|')
    ser[:len(li)] = li
    return ser

def parse_abs(rawtext_arr):
    result = []
    for i,rawtext in enumerate(rawtext_arr):
        if pd.isnull(rawtext):
            result.append('')
        else:
            rawtext = rawtext.translate(None,string.punctuation).decode('utf8').split()
            if len(rawtext)>0:
                cleaned = [stemmer.stem(w) for w in rawtext]
                result.append(' '.join(cleaned))
            else:
                result.append('')
    wordset = set(' '.join(result).split())
    return wordset,result


def process(year):
    with timed(desc=year,pad='----'):
        with timed('keyword loading',year=year):
            kw_current = pd.read_table('S:/UsersData_NoExpiration/jjl2228/keywords/pubs_by_year/{}.txt.gz'.format(year),header=None,names=['keyword','uid'],nrows=debug).dropna()
        with timed('metadata loading',year=year):
            md_current = pd.read_table('P:/Projects/WoS/WoS/parsed/metadata/{}.txt.gz'.format(year),header=None, nrows=debug,
                                   names=["uid","date","pubtype","volume","issue","pages","paper_title","source_title","doctype"],
                                  usecols=["uid","pubtype","paper_title","source_title","doctype"])
        with timed('category loading',year=year):
            cats_current = pd.read_table('P:/Projects/WoS/WoS/parsed/subjects/{}.txt.gz'.format(year),header=None,names=['uid','heading','subheading','categories'], nrows=debug)
        with timed('category formatting',year=year):
            cats_current = pd.concat([cats_current[['uid','heading','subheading']],cats_current['categories'].apply(gen_series)],axis=1)
        with timed('reference loading',year=year):
            refs_current = pd.read_table('P:/Projects/WoS/WoS/parsed/references/{}.txt.gz'.format(year),header=None,names=['uid','n_refs','refs','missing'],usecols=['uid','refs'], nrows=debug)
        with timed('abstract loading',year=year):
            abs_current = pd.read_table('P:/Projects/WoS/WoS/parsed/abstracts/{}.txt.gz'.format(year),header=None,names=['uid','abstract'], nrows=debug)
        with timed('abstract parsing',year=year):
            wordset,parsed_abstracts = parse_abs(abs_current['abstract'].values)
            abs_current['abstract'] = parsed_abstracts
        print 'wordset length: {} ({})'.format(len(wordset),year)
        with timed('data merging',year=year):
            current = kw_current.merge(md_current,on='uid',how='inner').merge(cats_current,on='uid',how='inner').merge(refs_current,on='uid',how='inner').merge(abs_current,on='uid',how='left')
            current['year'] = year
        with timed('saving data'):
            current.to_pickle('{}{}.pkl'.format(tmpdir,year))
            cPickle.dump(wordset,open('{}vocab_{}'.format(tmpdir,year),'wb'))
        print 'final datasize: {} ({})'.format(current.shape,year)
    return wordset,current

def main(n_procs):
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)  
    pool = mp.Pool(n_procs)
    signal.signal(signal.SIGINT, original_sigint_handler)
    try:
        res = pool.map_async(process,xrange(1991,2016))
        result = res.get(9999999999999999)
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        print("Normal termination")
        pool.close()
    pool.join()
    return result
        
if __name__=='__main__':

    procs = 25

    with timed('keyword processing',pad=' ######## '):
        with timed('parallel processing'):
            #result = main(procs)
            #if result is None:
            #    sys.exit()
            pool = mp.Pool(25)
            result = pool.map(process,xrange(1991,2016))
            print 'result collected'
            with timed('pool shutdown'):
                try:
                    pool.join()
                    pool.terminate()
                    pool.close()
                except:
                    print "exception in pool shutdown, but let's keep going..."


        with timed('final wordset unioning'):
            final_wordset = set.union(*[r[0] for r in result])

        with timed('final wordset writing'), open(kwdir+'vocab','w') as fout:
            fout.write('\n'.join(final_set))

        with timed('dataframe concatenation'):
            df = pd.concat([r[1] for r in result])

        with timed('per-keyword df generation'),open(kwdir+'vocab_idx','w') as idx:
            i = 0
            for kw,kw_df in df.groupby('keyword'):
                if len(kw_df)>=100:
                    kw_df.to_pickle("{}{}.pkl".format(kwdir,i))
                    idx.write(kw+'\n')
                    i+=1



