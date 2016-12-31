from __future__ import division


import numpy as np
import scipy
import sqlite3
import re
import cPickle as pickle
import time
from nltk.stem.wordnet import WordNetLemmatizer

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from scipy.sparse import csr_matrix


# Load data and vectorize
def db_document_stream(db_path, sql_query, lemmatize=False):
    '''
    Runs search query on database and yields all documents when requested
    (Yielding them individually helps to save memory)

    :param db_path: path to sqlite database
    :param sql_query: query to run on the database, e.g. SELECT abstract FROM refs WHERE abstract IS NOT NULL LIMIT 1000
    :return: Nothing--the function yields documents to a vectorizer
    '''
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    lemmatizer = WordNetLemmatizer()
    count = 0

    # Yield first row empty so that database rowids and matrix rowids are synchronized
    yield ''
    for row in cursor.execute(sql_query):


        # merge title and abstract if the article has an abstract
        title_and_abstract = row[0]
        if not row[1] is None:
            title_and_abstract = '; '.join([row[0], row[1]])

        count += 1
        if count % 10000 == 0:
            print "Yielding document {}.".format(count)

        text = re.sub(r'[0-9]', ' ', re.sub(r'[^\x00-\x7F]', ' ', title_and_abstract))

        # actually the lemmatizer sucks quite a bit because it would require the word type (noun, verb) as input
        # without it, it assumes that every word is a noun and turns "has" into "ha"
        if lemmatize:
            print text.split()
            text_lemmatized = [lemmatizer.lemmatize(i) for i in text.split()]
            print text_lemmatized
            print ' '.join(text_lemmatized)
            print

            yield ' '.join(text_lemmatized)

        else:
            yield text
    connection.close()

def tfidf_vectorize(documents, n_features=10000, max_df=0.95, min_df=2, vocabulary=None, dtype=np.float64,
                    use_idf=True, ngram_range=(1,1)):
    '''
    See http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html

    :param documents: use db_document_stream to create a generator
    :param n_features: number of terms
    :param max_df: max document frequency. term gets ignored if it appears in more than max_df of all documents
    :param min_df: term has to appear at least min_df times
    :param vocabulary:
    :return: Document-term matrix and list of feature names

    '''
    print "Vectorizing text with tf-idf vectorizer"

    vectorizer = TfidfVectorizer(
        max_df=max_df,
        min_df=min_df,
        max_features=n_features,
        stop_words='english',
        vocabulary=vocabulary,
        dtype=dtype,
        use_idf=use_idf,
        ngram_range=ngram_range
    )
    document_matrix = vectorizer.fit_transform(documents)
    feature_names = vectorizer.get_feature_names()

    return document_matrix, feature_names


def count_vectorize(documents, n_features=200000, max_df=0.95, min_df=2, vocabulary=None, dtype=np.int64,
                    ngram_range=(1,1)):
    '''
    See http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html

    :param documents: use db_document_stream to create a generator
    :param n_features: number of terms
    :param max_df: max document frequency. term gets ignored if it appears in more than max_df of all documents
    :param min_df: term has to appear at least min_df times
    :param vocabulary:
    :return: Document-term matrix and list of feature names
    '''

    vectorizer = CountVectorizer(
        max_df=max_df,
        min_df=min_df,
        max_features=n_features,
        stop_words='english',
        vocabulary=vocabulary,
        dtype=dtype,
        ngram_range=ngram_range
    )
    document_matrix = vectorizer.fit_transform(documents)
    feature_names = vectorizer.get_feature_names()
    return document_matrix, feature_names


def store_vectorized_to_file(file_name, matrix, feature_names):
    '''
    Stores matrix as file_name.npz and feature_names as file_name.pickle

    :param file_path:
    :return:
    '''

    np.savez('{}.npz'.format(file_name), data=matrix.data, indices=matrix.indices, indptr=matrix.indptr,
             shape=matrix.shape)
    pickle.dump(feature_names, open('{}.pickle'.format(file_name), 'wb'), -1)


def load_vectorized_from_file(file_name):
    '''
    file_name should be without file endings. so "/tobacco/tfidf" will look for "/tobacco/tfidf.npz" and
    "/tobacco/tfidf.pickle"

    :param file_path:
    :return:
    '''

    print "Loading matrix and feature_names from file"


    with open('{}.pickle'.format(file_name), 'rb') as pickle_file:
        feature_names = pickle.load(pickle_file)

    y = np.load('{}.npz'.format(file_name))
    matrix = csr_matrix( (y['data'], y['indices'], y['indptr']), shape=y['shape'])

    return matrix, feature_names

def load_feature_names_from_file(file_path):
    '''
    Loads just feature_names from a pickle file. Required for the query

    :param file_path: Path to pickle file
    :return:
    '''

    with open(file_path, 'rb') as pickle_file:
        feature_names = pickle.load(pickle_file)

    return feature_names


def usage_example():

    # 1. Initialize documents
    documents = db_document_stream(db_path='/home/stephan/tobacco/medline/medline_complete_normalized.db',
                                   sql_query='SELECT title, abstract FROM refs order by rowid asc  LIMIT 10000')
    # 2. Tokenize
    matrix, feature_names = tfidf_vectorize(documents)

    # 3. Store as pickle
    file_name = 'medline_vectorized'
    store_vectorized_to_file(file_name, matrix, feature_names)

    # 4 Load matrix and feature names from file
    matrix, feature_names = load_vectorized_from_file(file_name)


if __name__ == "__main__":

    # 1. Initialize documents
    documents = db_document_stream(db_path='/tobacco/medline_5mio/medline_5mio.db',
                                   sql_query='SELECT title, abstract FROM refs order by rowid asc limit 100000000;',
                                   lemmatize=False)

    # 2. Tokenize
    matrix, feature_names = count_vectorize(documents, 3000000, min_df=10, dtype=np.bool)

    print "nnz", matrix.getnnz(), "shape: ", matrix.shape

    store_vectorized_to_file('/tobacco/medline_5mio/medline_5mio_lookup', matrix, feature_names)


