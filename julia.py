import pandas as pd
import numpy as np
from tqdm import tqdm
from time import sleep
import pickle
import multiprocessing as mp


path="P:/Projects/WoS/julia/"

import time,datetime
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
            

def process(row):
    
    aids = [int(a) for a in row.author_id.split('|')] # list of author ids in that row
    n = len(aids)
    names = row.author_name.split('|')  # list of author names in that row
    if len(names)!=n:
        return pd.DataFrame({'uid':[],'author_id':[],'author_name':[],'affiliation':[],'seq':[]})

    affil = []
    if pd.isnull(row.affiliation):   # if there is no affiliation info, then affil is a list of n nans
        affil = [np.nan]*n
    else:
        affil_list = row.affiliation.split('|') # if there is affiliation info, i build a list of affiliations                       
        
        for idx in row.idx.split('|'): ## list of affiliations indexes for each author , idx is a list of affil indexes for one author                                                           
            
            
            if idx=='-1':
                affil.append('?')
                # OR:
                # affil.append(row.affiliation)
            else:
                try:
                    affil.append('|'.join([affil_list[int(i)] for i in idx.split(',')]))
                except IndexError: 
                    affil.append('?')

                   
    return pd.DataFrame({'uid':[row.uid]*n,'author_id':aids,'author_name':names,'affiliation':affil,'seq':range(len(aids))})


def unpack_year(year):
    with timed('Processing year {}'.format(year)):
        df = pd.read_table('P:/Projects/WoS/WoS/parsed/authors/{}.txt.gz'.format(year),header=None,names=['uid','author_id','author_name','affiliation','idx'])
        result = pd.concat([process(row[1]) for row in  df.iterrows(),total=len(df)])  
        # get rid of all rows without a valid author_id
        result=result.loc[result['author_id'] != -1]  # i filter out the authors without desambiguated author id
        # filter to US only
        result=result.dropna(subset=['affiliation']).loc[result['affiliation'].dropna().str.contains('USA')]  

        #result['author_name'] = result.author_name.str.lower()#apply(re_capilatizing_lastnames)

        result.to_csv('{}temp/unpacked_'.format(ddir,year))
    print("{} --> raw_data rows={}, unpacked rows={}".format(year,len(df),len(result)))
    return result


def grouping(input_df):
    
    tot_n_pubs=len(input_df)
    result = pd.Series({'author_names':input_df.author_name.unique(),'affiliations':input_df.affiliation.unique(),'seqs':input_df.seq.values,'tot_n_pub':tot_n_pubs}) #if the values of some of the columns are list, use SERIES instead of DATAFRAME            
    return result
 

if __name__ == '__main__':

    procs = mp.cpu_count()
    pool = mp.Pool(procs)

    FINAL = pool.map(unpack_year,range(1950,2016))
    FINAL = pd.concat(FINAL)

    try:
        pool.close()
    except:
        pass

    tqdm.pandas()
    with timed('Grouping all data by author'):
        grouped = FINAL.groupby('author_id').progress_apply(grouping).reset_index()
    with timed('Saving grouped data'):
        grouped.to_csv("{}final.tsv".format(ddir), sep='\t',index=False)
    with timed("Pulling out multi-match author data"):
        indices = grouped.author_name.progress_apply(lambda x: x>1)
        grouped[indices].to_csv("{}lookup_multiple_author_names.tsv".format(ddir), sep='\t',index=False)



