from medline.topic_modeling.vectorizer import db_document_stream, tfidf_vectorize, load_feature_names_from_file
from medline.utilities.sparse_matrices import load_csr_matrix_from_file
from anser_indicus.analytics.low_rank.spca import run_spcav2
from medline.utilities.sparse_matrices import store_csr_matrix_to_file

from scipy.sparse import csr_matrix, dok_matrix

import numpy as np
import time
import pickle

def spca(matrix, center='row', card_docs=25, card_terms=25, n_topics=10, n_iterations=100,
         deflation_type=-1):
    '''

    :param matrix:
    :param center:
    :param card_docs:
    :param card_terms:
    :param n_topics:
    :param n_iterations:
    :param deflation_type: -1: remove words and documents. 0: only remove documents. 1: only remove words
    :return:
    '''



    sparse_pca_model = run_spcav2(matrix,
                                  center=center,
                                  card_docs=card_docs,
                                  card_terms=card_terms,
                                  n_topics=n_topics,
                                  deflation_type=deflation_type)

    return sparse_pca_model


def create_spca_result_string(sparse_pca_model, feature_names):

    result = ''

    for i in xrange(len(sparse_pca_model)):
        result += "\n Topic {}\n".format(i)
#        print 'Topic %d:' % i
        topics = ''
        for j in sparse_pca_model[i][1][0]:
            topics += ' %s' % feature_names[j]
        result += topics

    return result

def spca_create_list_of_topic_terms_and_rowids(spca_model, feature_names, rowids, max_docs):
    '''
    I think, if no rowids, it should just use k. fix later.
    :param spca_model:
    :param feature_names:
    :return:
    '''

    topics = []

    for i in xrange(len(spca_model)):
        topic_terms = []
        topic_rowids = []
        for j in spca_model[i][1][0]:
            topic_terms.append(feature_names[j])
        #
        # for k in spca_model[i][0][0]:
        #     topic_rowids.append(rowids[k])
        #     if len(topic_rowids) == max_docs:
        #         break

        for k in spca_model[i][0][1].argsort()[: -max_docs-1 : -1]:
            topic_rowids.append(rowids[spca_model[i][0][0][k]])
            if len(topic_rowids) == max_docs:
                break

        topics.append({
            'terms': topic_terms,
            'rowids': topic_rowids
        })

    return topics

def generate_matrix(sparse_pca_model):
    result = []

    print len(sparse_pca_model)

    for i in xrange(len(sparse_pca_model)):
        l = list(sparse_pca_model[i][1][1])

        print len(l), l

        if len(l) != 20000:
            for _ in range(20000-len(l)):
                l.append(3)
    result.append(l)
    return csr_matrix(np.vstack(result))

def project_spca(matrix, sparse_pca_model):
    topics = generate_matrix(sparse_pca_model)

    print topics.shape
    print matrix.shape
    return matrix*topics.T


def project_spcav2(doc_term_matrix, spca_model):

    # size: n
    topic_term_matrix = dok_matrix((len(spca_model), matrix.shape[1]), dtype=np.float64)

    print topic_term_matrix.shape
    print doc_term_matrix.shape

    for topic_no, _ in enumerate(spca_model):
        for i in range(len(spca_model[topic_no][1][0])):
            index = spca_model[topic_no][1][0][i]
            value = spca_model[topic_no][1][1][i]

            topic_term_matrix[topic_no, index] = value

    print topic_term_matrix.shape, topic_term_matrix.nnz
    topic_term_matrix = topic_term_matrix.tocsr()

    doc_topic_matrix = (doc_term_matrix * topic_term_matrix.T).tocsr()

    print doc_topic_matrix.shape, doc_topic_matrix.nnz

    return doc_topic_matrix, topic_term_matrix

#    print topic_term_matrix

def usage_example():

    # 1. Initialize documents
    documents = db_document_stream(db_path='/home/stephan/tobacco/medline/medline_complete.db',
                                   sql_query='SELECT title, abstract FROM refs order by rowid asc limit 1000000')
    # 2. Tokenize
    matrix, feature_names = tfidf_vectorize(documents, n_features=20000, dtype=np.float32)

    print matrix.shape

    # 3. SPCA
    spca_model = spca(matrix, card_docs=200, card_terms=10, n_topics=10)

    # 4. Print Result
    print create_spca_result_string(spca_model, feature_names)



if __name__ == "__main__":


    matrix = load_csr_matrix_from_file('/tobacco/medline_5mio/medline_5mio_doc_term.npz')
    feature_names = load_feature_names_from_file('/tobacco/medline_5mio/medline_5mio_feature_names.pickle')
    #
    # # 3. SPCA
    # start = time.time()
    # spca_model = spca(matrix, card_docs=2000, card_terms=10, n_topics=10)
    #
    # print create_spca_result_string(spca_model, feature_names)
    # pickle.dump(spca_model, open('spca_10_10_both.pickle', 'wb'))
    # print time.time() - start
    #
    # start = time.time()
    # spca_model = spca(matrix, card_docs=2000, card_terms=10, n_topics=300)
    #
    # print create_spca_result_string(spca_model, feature_names)
    # pickle.dump(spca_model, open('spca_10_300_both.pickle', 'wb'))
    # print time.time() - start
    #
    # start = time.time()
    # spca_model = spca(matrix, card_docs=2000, card_terms=10, n_topics=600, deflation_type=0)
    #
    # print create_spca_result_string(spca_model, feature_names)
    # pickle.dump(spca_model, open('spca_10_600_docs.pickle', 'wb'))
    # print time.time() - start

    model = pickle.load(open('spca_10_300_both.pickle'))

#    print matrix.shape
#    projected = project_spca(matrix, model)
#    print projected.shape

    FOLDER = '/tobacco/'

    dtm, ttm = project_spcav2(matrix, model)

    store_csr_matrix_to_file(dtm, FOLDER + 'medline_5mio_doc_topic.npz')

    store_csr_matrix_to_file(ttm, FOLDER + 'medline_5mio_topic_term.npz')