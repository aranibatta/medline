import time
from sklearn.decomposition import NMF
from medline.topic_modeling.vectorizer import db_document_stream, tfidf_vectorize, load_vectorized_from_file
import scipy
import numpy as np

from scipy.io import savemat, loadmat

def nmf(tfidf_matrix, n_components=10, init='nndsvd', sparseness='components',
        random_state=time.time(), tol=0.001):

    '''
    :param tfidf_matrix:
    :param feature_names:
    :param n_components:
    :param init:
    :param sparseness:
    :param random_state:
    :param tol:
    :return:
    '''

    print "Running NMF"

    nmf_model = NMF(
        n_components=n_components,
        init=init,
        sparseness=sparseness,
        random_state=random_state,
        tol=tol
    )


    # document-topic matrix
    doc_topic_matrix = nmf_model.fit_transform(tfidf_matrix)

    # topic-term matrix
    topic_term_matrix = nmf_model.components_


    return doc_topic_matrix, topic_term_matrix

def create_nmf_result_string(topic_term_matrix, feature_names, terms_per_topic=25):
    '''
    Produces a string of the nmf results. can be used to print or store to file.

    :param topic_term_matrix:
    :param feature_names:
    :return:
    '''

    results = ''

    for topic_idx, topic in enumerate(topic_term_matrix):
        results += 'Topic: {}\n'.format(topic_idx)
        t = ''
        for i in topic.argsort()[:-terms_per_topic - 1:-1]:
            t += '%s; ' % feature_names[i]
        results += t + "\n"

    return results

def nmf_create_list_of_topic_terms_and_rowids(doc_topic_matrix, topic_term_matrix, feature_names, rowids, terms_per_topic,
                                             docs_per_topic):

    '''
    Based on NMF results, this script creates two lists:
        - best_terms: best terms_per_topic for every topic
        - best_docs_rowids: best rowids for every topic

    :param doc_topic_matrix:
    :param topic_term_matrix:
    :param feature_names:
    :param terms_per_topic:
    :param docs_per_topic:
    :return:
    '''

    topics = []

    for topic in topic_term_matrix:
        topic_terms = []
        for i in topic.argsort()[:-terms_per_topic - 1:-1]:
            topic_terms.append(feature_names[i])
        topics.append({
            'terms': topic_terms
        })

    topic_doc_matrix = doc_topic_matrix.transpose()

    for index, topic in enumerate(topic_doc_matrix):
        topic_rowids = []
        for i in topic.argsort()[: -docs_per_topic-1 : -1]:
            topic_rowids.append(rowids[i])
        topics[index]['rowids'] = topic_rowids

    return topics

def store_nmf_results_to_disk(doc_topic_matrix, topic_term_matrix, file_path):
    '''
    Stores nmf results to disk at file_path

    :return:
    '''

    save_to_disk = {"doc_topic_matrix": doc_topic_matrix,
                    "topic_term_matrix": topic_term_matrix}
    savemat(file_path, save_to_disk)

def load_nmf_results_from_file(nmf_mat_path):
    '''
    Loads doc_topic matrix and topic_term_matrix from file

    :param nmf_mat_path: Path to *.mat file
    :return:
    '''

    nmf_matrix = loadmat(nmf_mat_path)
    doc_topic_matrix = nmf_matrix['doc_topic_matrix']
    topic_term_matrix = nmf_matrix['topic_term_matrix']

    return doc_topic_matrix, topic_term_matrix



def usage_example():

    # 1. Initialize documents
    documents = db_document_stream(db_path='/home/stephan/tobacco/medline/medline_complete.db',
                                   sql_query='SELECT title, abstract FROM refs order by rowid asc limit 1000000')
    # 2. Tokenize
    matrix, feature_names = tfidf_vectorize(documents, n_features=20000, dtype=np.float32)

    # 3. NMF
    doc_topic_matrix, topic_term_matrix = nmf(matrix, n_components=10)

    # 4. Print resulting topics
    print create_nmf_result_string(topic_term_matrix, feature_names)

    # 5. Store to disk
    store_nmf_results_to_disk(doc_topic_matrix, topic_term_matrix, 'example.mat')



if __name__ == "__main__":

    matrix, feature_names = load_vectorized_from_file('/home/stephan/tobacco/medline/tfidf_fts_5mio_20k')

    doc_topic_matrix, topic_term_matrix = nmf(matrix, n_components=1)

    print create_nmf_result_string(topic_term_matrix, feature_names)

    print doc_topic_matrix.shape, topic_term_matrix.shape

#    usage_example()
