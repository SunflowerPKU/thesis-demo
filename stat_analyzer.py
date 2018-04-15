from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models

import gensim
import pandas as pd
import statsmodels.api as sm


def linear_regression(x, y):
    X = pd.DataFrame({'ts':x, 'const': [1.0] * len(x)}, index=x)
    mod = sm.OLS(y, X)
    res = mod.fit()
    slope = res.params['ts']
    pvalue = res.pvalues['ts']
    return slope, pvalue

def lda(messages, num_topics=5):
    tokenizer = RegexpTokenizer(r'\w+')
    en_stop = get_stop_words('en')
    p_stemmer = PorterStemmer()

    def preprocess(message):
        tokens = tokenizer.tokenize(message.lower())
        stopped_tokens = [i for i in tokens if (not i in en_stop) and (not i.isdigit()) and (len(i) > 2)]
        stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]
        return stemmed_tokens

    texts = map(preprocess, list(messages))
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    tfidf = models.TfidfModel(corpus, id2word=dictionary)
    vec = [v for k,v in tfidf.idfs.items()]
    low_value = 0.02
    low_value_words = []
    for bow in corpus:
        low_value_words += [id for id, value in tfidf[bow] if value < low_value]
    dictionary.filter_tokens(bad_ids=low_value_words)
    corpus = [dictionary.doc2bow(text) for text in texts]


    ldamodel = models.ldamodel.LdaModel(corpus, num_topics=num_topics, id2word = dictionary, passes=50)
    topic_distribution = map(lambda c: map(lambda t: t[1], ldamodel.get_document_topics(c, minimum_probability=0)), corpus)
    topic_distribution = pd.DataFrame(topic_distribution, index=messages.index)
    terms = map(lambda idx: map(lambda t: t[0], ldamodel.show_topic(idx, 5)), range(num_topics))
    return topic_distribution, terms