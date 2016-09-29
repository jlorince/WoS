from gensim.models.word2vec import Word2Vec,LineSentence
import gzip
import multiprocessing as mp
import glob

files = glob.glob('*.txt.gz')
for f in files:
    for line in gzip.open(f):
        with gzip.open('all_sentences','a') as fout:
            fout.write(' '.join(['_'.join(x.split()) for x in line.strip().split('\t')[2].split('|')])+'\n')



sentences = reader(files)


model = Word2Vec(LineSentence('all_sentences'), size=100, window=5, min_count=5,workers=60)