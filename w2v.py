from gensim.models.doc2vec import Doc2Vec,TaggedLineDocument
import gzip,os,glob,time,datetime
#import pathos.multiprocessing as mp
import numpy as np
from tqdm import tqdm


#abs_dir = os.path.expanduser('~')+'/parsed/abstracts/'
abs_dir = 'P:/Projects/WoS/WoS/parsed/abstracts/'
#abs_dir = os.path.expanduser('~')+'/'
#abs_dir = os.path.expanduser('~')+'/storage/abstracts_parsed/'

size= 200
window = 5
min_count = 5
workers = 60

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


def normalize_text(text):
    norm_text = text.lower()

    # Replace breaks with spaces
    norm_text = norm_text.replace('|', ' ')

    # Pad punctuation with spaces on both sides
    for char in ['.', '"', ',', '(', ')', '!', '?', ';', ':']:
        norm_text = norm_text.replace(char, ' ' + char + ' ')
    norm_text = norm_text.strip()
    if norm_text:
        return norm_text.strip()
    else:
        return None


def preprocess_docs(in_dir,out_dir):
    #with timed('Preprocessing docs'):
    with gzip.open(out_dir+'docs.txt.gz','wb') as docs:
        overall_total = 0
        for year in tqdm(xrange(1991,2016)):
            #with timed('Processing year: {}'.format(year)):
            #with gzip.open(in_dir+'uid_indices_{}.txt.gz'.format(year),'wb') as idx_out:
            f = '{}{}.txt.gz'.format(in_dir,year)
            #print "Starting file {}".format(f)
            #for i,line in enumerate(tqdm(gzip.open(f))):
            for line in gzip.open(f):
                uid,text = line.split('\t')
                if text.strip() == '':
                    continue
                normed = normalize_text(text)
                if normed is None:
                    continue
                docs.write(normed+'\n')
                #idx_out.write(uid+'\n')
                #total = i+1
                #overall_total += total
        #print "{} complete: {} total documents ({} overall)".format(f,total,overall_total)


preprocess_docs(abs_dir,abs_dir)

documents = [doc for doc in TaggedLineDocument(abs_dir+'docs.txt.gz')]
model = Doc2Vec(documents, size=size, window=window, min_count=min_count,workers=workers)

from sklearn.preprocessing import Normalizer
nrm = Normalizer('l2')
normed = nrm.fit_transform(model.docvecs.doctag_syn0)
words_normed = nrm.fit_transform(model.syn0)

np.save(abs_dir+'d2v/doc_features_normed-{}-{}-{}.npy'.format(size,window,min_count),normed)
np.save(abs_dir+'d2v/word_features_normed-{}-{}-{}.npy'.format(size,window,min_count),words_normed)
model.save(abs_dir+'d2v/model-{}-{}-{}'.format(size,window,min_count))
