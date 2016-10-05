import graphlab as gl
import os
import numpy as np

gl.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 60)
gl.set_runtime_config('GRAPHLAB_DEFAULT_NUM_GRAPH_LAMBDA_WORKERS', 60)
gl.set_runtime_config('GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY',100000000000)
gl.set_runtime_config('GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE',100000000000)

workdir = 'S:/UsersData/jjl2228/WoS/parsed/abstracts/'

if os.path.exists(workdir+'gl_docs_raw'):
    docs = gl.SFrame(workdir+'gl_docs_raw')
else:
    docs = None
    for year in np.arange(1950,2016,1):
        print year
        try:
            nxt = gl.SFrame.read_csv('S:/UsersData/jjl2228/WoS/parsed/abstracts/{}.txt.gz'.format(year),delimiter='\t',header=None,column_type_hints=[str,str])
        except:
            continue
        if docs is None:
            docs = nxt
        else:
            docs = docs.append(nxt)

    docs.save(workdir+'gl_docs_raw')

# Remove stopwords and convert to bag of words
docs['X2'] = docs['X2'].apply(lambda x: ' '.join(x.split('|')))
docs = gl.text_analytics.count_words(docs['X2'])
docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

# Learn topic model
#model = gl.topic_model.create(docs,num_topics=200,num_iterations=50)

doc_topic_mat = np.array(model.predict(docs,output_type='probabilities'))

np.save(workdir+'features-lda-200.npy',doc_topic_mat)

from sklearn.preprocessing import Normalizer
nrm = Normalizer('l2')
normed = nrm.fit_transform(doc_topic_mat)

np.save(workdir+'features_normed-lda-200.npy',normed)

model = gl.load_model(workdir+'model_200')
topics = model.get_topics(num_words=100)
topics_grp = topics.groupby('topic',{'terms': gl.aggregate.CONCAT("word","score")})
df = topics_grp.to_dataframe()