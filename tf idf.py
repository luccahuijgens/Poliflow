import math
import os
import re
import nltk
import gensim
from gensim.models import Word2Vec
from textblob import TextBlob as tb
from sklearn.decomposition import PCA
from matplotlib import pyplot
from gensim.models import KeyedVectors


def tf(word, blob):
    return blob.words.count(word) / len(blob.words)

def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob.words)

def idf(word, bloblist):
    return math.log(len(bloblist) / (1 + n_containing(word, bloblist)))

def tfidf(word, blob, bloblist):
    return tf(word, blob) * idf(word, bloblist)

#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#database = myclient["poliflow"]
#collection = database
#cursor = collection.find({})
#id=1
#articles=[]
#for document in cursor:
    #article=Article(id, document.get('name'), document.get('name'), document.get('name'))
    #articles.append(article)
    #id+=1

sentencelist=[]
wordcorpus=[]
corpus=[]
news_subjects=['zorg','politiek','onderwijs','verkiezingen','economie','binnenland','buitenland','koningshuis','technologie','cultuur','media','regio']
for file in os.listdir('C:/Users/Lucca.LAPTOP-PKB9NQVU/Downloads/nieuws/2002'):
    text= open('C:/Users/Lucca.LAPTOP-PKB9NQVU/Downloads/nieuws/2002/'+file,'r').read()
    text = re.sub('[^a-zA-Z]', ' ', text )
    text = re.sub(r'\s+', ' ', text)
    corpus.append(tb(text).lower())

for blob in corpus:
    for sent in blob.sentences:
        wordcorpus.append(sent.words)

model=Word2Vec(wordcorpus, min_count=2)

for i, blob in enumerate(corpus):
    #print("Top words in document {}".format(i + 1))
    scores = {word: tfidf(word, blob, corpus) for word in blob.words}
    sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    articlesubjects=[]
    for word, score in sorted_words[:3]:
        #print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))
        subjectdictionary={}
        for subject in news_subjects:
            try:
                value=0
                value=model.wv.similarity(word,subject)
                subjectdictionary[subject]=value
            except:
                print("Similarity ("+word+" + "+subject+": not enough data for subject determination")

        sorteddictionary = [(k, subjectdictionary[k]) for k in sorted(subjectdictionary, key=subjectdictionary.get, reverse=True)]
        counter=0
        for key,value in sorteddictionary:
            if counter<3:
                articlesubjects.append(key)
                counter+=1

    print (str(blob))
    distinct_values = set(articlesubjects)
    print(str(set (articlesubjects)))
    for distinct_value in distinct_values:
        if articlesubjects.count(distinct_value)>=2:
            print("subject: "+str(distinct_value))
