import numpy as np
import gzip,time,datetime,string,signal,sys,cPickle,codecs
import pandas as pd
import multiprocessing as mp
from nltk.corpus import stopwords
stop = set(stopwords.words('english'))

tmpdir = 'S:/UsersData_NoExpiration/jjl2228/keywords/temp/'
kwdir = 'S:/UsersData_NoExpiration/jjl2228/keywords/parsed/'



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

#### Build deduped dataframe (and save)

with timed('dataframe concatenation'):
    concat = []
    for year in xrange(1991,2016):
        concat.append(pd.read_pickle('{}{}.pkl'.format(tmpdir,year)))
        print year,
    df = pd.concat(concat)

deduped = df.drop_duplicates(subset='uid')

def format_headings(row):
    if row.heading == 'Arts & Humanities':
        return 'Arts & Humanities'
    else:
        return "{} - {}".format(row.heading,row.subheading)

global_vocab = pd.Index(codecs.open(kwdir+'vocab','r','utf-8').read().split())
deduped['cleaned_subheading'] = deduped.apply(format_headings,1) 

del deduped['keyword']
deduped.to_pickle(kwdir+'all_pubs.pkl')


# Generate global vocab frequency distribution
total = len(deduped)
termdict = {line.strip():0 for line in codecs.open(kwdir+'vocab','r','utf-8')}

for i,row in enumerate(deduped.abstract.dropna(),1):
    for term in row.split():
        termdict[term] += 1
    if i%100000==0: 
        print "{}/{} ({}%)".format(i,total,100*(i/float(total)))

global_term_counts = pd.Series(termdict)
global_term_counts.save_pickle(kwdir+'global_term_counts.pkl')


# prune and save
pruned = global_term_counts[global_term_counts>=1000]
vocab_len = 0
with codecs.open(kwdir+'vocab_pruned_1000','w','utf-8') as fout:
    for term in pruned.index.values:
        if (term not in stop) and (term.isalpha()):
            fout.write(term+'\n')
            vocab_len += 1

print vocab_len



year_indices = {i:y for i,y in enumerate(xrange(1991,2016))}



d = 'S:/UsersData_NoExpiration/jjl2228/keywords/cat_word_dists/'
with timed('per-heading grouping'):
    for heading,data in deduped.groupby(['heading']):
        with timed('heading ({},year)'.format(heading,year)):
            result = np.zeros((25,vocab_len))
            for year,inner_data in data.groupby('year'):
                termdict = {line.strip():0 for line in codecs.open(kwdir+'vocab_pruned_100','r','utf-8')}
                for i,abstract in enumerate(data.abstract.dropna(),1):
                    if i%100000==0: print i,
                    for term in abstract.split():
                        try:
                            termdict[term] += 1
                        except KeyError: 
                            continue

                ser = pd.Series(termdict).reindex(global_vocab,fill_value=0)
                result[year_indices[year]] = (ser/float(ser.sum())).values
            np.save('{}headings/{}.npy'.format(d,heading),np.vstack(result))

    for subheading,data in deduped.groupby('cleaned_subheading'):
        with timed('subheading ({})'.format(heading)):
            result = np.zeros((25,vocab_len))
            for year,inner_data in data.groupby('year'):
                termdict = {line.strip():0 for line in codecs.open(kwdir+'vocab_pruned_100','r','utf-8')}
                for i,abstract in enumerate(data.abstract.dropna(),1):
                    if i%100000==0: print i,
                    for term in abstract.split():
                        try:
                            termdict[term] += 1
                        except KeyError: 
                            continue

            ser = pd.Series(termdict).reindex(global_vocab,fill_value=0)
            result[year_indices[year]] = (ser/float(ser.sum())).values
            np.save('{}subheadings/{}.npy'.format(d,heading),ser.values)
