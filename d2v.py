from gensim.models.doc2vec import Doc2Vec,TaggedLineDocument,TaggedDocument
from gensim import utils
import gzip,os,glob,time,datetime,sys
import numpy as np
from tqdm import tqdm as tq
from nltk.tokenize import word_tokenize
import multiprocess as mp


abs_dir = 'P:/Projects/WoS/WoS/parsed/abstracts/'
d2v_dir = 'P:/Projects/WoS/WoS/parsed/abstracts/d2v/'



class timed(object):
    def __init__(self,desc='command',pad='',**kwargs):
        self.desc = desc
        self.kwargs = kwargs
        self.pad = pad
    def __enter__(self):
        self.start = time.time()
        print('{} started...'.format(self.desc))
    def __exit__(self, type, value, traceback):
        if len(self.kwargs)==0:
            print('{}{} complete in {}{}'.format(self.pad,self.desc,str(datetime.timedelta(seconds=time.time()-self.start)),self.pad))
        else:
            print('{}{} complete in {} ({}){}'.format(self.pad,self.desc,str(datetime.timedelta(seconds=time.time()-self.start)),','.join(['{}={}'.format(*kw) for kw in self.kwargs.iteritems()]),self.pad))

# custom class to parse documents 
class custom_TLD(TaggedLineDocument):
    def __iter__(self):
        """Iterate through the lines in the source."""
        try:
            # Assume it is a file-like object and try treating it as such
            # Things that don't have seek will trigger an exception
            self.source.seek(0)
            for item_no, line in enumerate(self.source):
                yield TaggedDocument(utils.to_unicode(line).lower().split(), [item_no])
        except AttributeError:
            # If it didn't work like a file, use it as a string filename
            with utils.smart_open(self.source) as fin:
                for item_no, line in enumerate(fin):
                    yield TaggedDocument(utils.to_unicode(line).lower().split(), [item_no])  


def normalize_text(text):
    return word_tokenize(text.strip().lower().replace('|', ' '))


if __name__ == '__main__':
    if len(sys.argv)>1:
        args = sys.argv[1:]
        size,window,min_count,workers,preprocess = map(int,args)

    else:
        size= 100
        window = 5
        min_count = 5
        workers = 60
        preprocess = True



if preprocess:

    pool = mp.Pool(25)

    def process_year(year):
        import gzip
        from nltk.tokenize import word_tokenize
        abs_dir = 'P:/Projects/WoS/WoS/parsed/abstracts/'
        d2v_dir = 'P:/Projects/WoS/WoS/parsed/abstracts/d2v/'
        result = []
        with gzip.open('{}uid_indices/{}.txt.gz'.format(d2v_dir,year),'wb') as idx_out:
            for line in gzip.open('{}{}.txt.gz'.format(abs_dir,year)):
                uid,text = line.decode('utf8').split('\t')
                normed = ' '.join(word_tokenize(text.strip().lower().replace('|', ' ')))
                if normed == "":
                    continue
                #docs.write((normed+'\n').encode('utf8'))
                idx_out.write((uid+'\n').encode('utf8'))
                result.append(normed)
        return '\n'.join(result)


    with gzip.open(d2v_dir+'docs.txt.gz','wb') as docs:
        #for year in tq(range(1991,2016)):
        for r in tq(pool.imap(process_year,range(1991,2016))):
            docs.write((r+'\n').encode('utf8'))

    pool.terminate()
    pool.close()



documents = custom_TLD(d2v_dir+'docs.txt.gz')
with timed('Running Doc2Vec'):
    model = Doc2Vec(documents, size=size, window=window, min_count=min_count,workers=workers)

with timed('Norming vectors'):
    from sklearn.preprocessing import Normalizer
    nrm = Normalizer('l2')
    normed = nrm.fit_transform(model.docvecs.doctag_syn0)
    words_normed = nrm.fit_transform(model.syn0)

with timed('Saving data'):
    pathname = "{}-{}-{}".format(size,window,min_count)
    if not os.path.exists(d2v_dir+pathname):
        os.mkdir(d2v_dir+pathname)
    np.save('{0}{1}/doc_features_normed_{1}.npy'.format(d2v_dir,pathname),normed)
    np.save('{0}{1}/word_features_normed_{1}.npy'.format(d2v_dir,pathname),words_normed)
    model.save('{0}{1}/model_{1}.npy'.format(d2v_dir,pathname))
