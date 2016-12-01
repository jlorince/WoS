import gzip,redis,time,datetime,string
import multiprocessing as mp
from nltk.stem.snowball import EnglishStemmer
stemmer = EnglishStemmer()

r = redis.StrictRedis(host='localhost', port=6379, db=0)

abstract_dir = '/backup/home/jared/abstracts/'

def process_year(year):
    print '----starting processing for {}----'.format(year)
    start = time.time()
    wordset=set()

    for i,line in enumerate(gzip.open('{}raw/{}.txt.gz'.format(abstract_dir,year)),1):
        try:
            uid,rawtext = line.strip().split('\t',1)
        except ValueError:
            continue
        rawtext = rawtext.translate(None,string.punctuation).decode('utf8').split()
        if len(rawtext)>0:
            cleaned = [stemmer.stem(w) for w in rawtext]
            r.set(uid,' '.join(cleaned))
            wordset = wordset.union(set(cleaned))

    print '----{} complete in {} ({} records)----'.format(year,str(datetime.timedelta(seconds=time.time()-start)),i)
    return wordset



if __name__=='__main__':
    overall_start = time.time()
    
    start = time.time()
    pool = mp.Pool(mp.cpu_count())
    result = pool.map(process_year,range(1991,2016))
    print '----all years complete in {} ----'.format(str(datetime.timedelta(seconds=time.time()-start)))

    start=time.time()
    print 'generating final vocab set'
    final_set = set.union(*result)
    print '----union generated in {} ----'.format(str(datetime.timedelta(seconds=time.time()-start)))

    start=time.time()   
    print 'writing to disk'
    with open(abstract_dir+'vocab','w') as fout:
        fout.write('\n'.join(final_set))
    print 'done in {} ----'.format(str(datetime.timedelta(seconds=time.time()-start)))

    print '----processing complete in {} ----'.format(str(datetime.timedelta(seconds=time.time()-overall_start)))
    
    
    