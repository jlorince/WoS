from gensim.models.doc2vec import Doc2Vec,TaggedLineDocument
import gzip,os,glob
import multiprocessing as mp
import numpy as np

debug = False

if debug:
    test= "_test"
else:
    test = ""



#abs_dir = os.path.expanduser('~')+'/parsed/abstracts/'
#abs_dir = 'S:/UsersData/jjl2228/WoS/parsed/abstracts/'
abs_dir = os.path.expanduser('~')+'/'

def normalize_text(text):
    norm_text = text.lower()

    # Replace breaks with spaces
    norm_text = norm_text.replace('|', ' ')

    # Pad punctuation with spaces on both sides
    for char in ['.', '"', ',', '(', ')', '!', '?', ';', ':']:
        norm_text = norm_text.replace(char, ' ' + char + ' ')

    return norm_text.strip()


def preprocess_docs(in_dir,out_dir):
    with gzip.open(in_dir+'uid_indices{}.txt.gz'.format(test),'wb') as idx_out, gzip.open(out_dir+'docs{}.txt.gz'.format(test),'wb') as docs:
        overall_total = 0
        if debug:
            files = glob.glob('{}2015.txt.gz'.format(in_dir))
        else:
            files = glob.glob('{}/*.txt.gz'.format(in_dir))
        for f in files:
            if 'docs' or 'uid_indices' in f:
                continue
            print "Starting file {}".format(f)
            for i,line in enumerate(gzip.open(f)):
                # if debug:
                #     if i==100000:
                #         break
                try:
                    uid,text = line.split('\t')
                except:
                    print line 
                    1/0
                if text.strip() == '':
                    continue
                docs.write(normalize_text(text)+'\n')
                idx_out.write(uid+'\n')
            total = i+1
            overall_total += total
            print "{} complete: {} total documents ({} overall)".format(f,total,overall_total)


#preprocess_docs(abs_dir,abs_dir)



documents = TaggedLineDocument(abs_dir+'docs{}.txt.gz'.format(test))
model = Doc2Vec(documents, size=200, window=5, min_count=5,workers=16)

features = model.docvecs.doctag_syn0.copy()

np.savetxt('features{}.tsv'.format(test),features,delimiter='\t')

from sklearn.preprocessing import Normalizer
nrm = Normalizer('l2')
normed = nrm.fit_transform(features)

np.savetxt('features_normed{}.tsv'.format(test),normed,delimiter='\t')

