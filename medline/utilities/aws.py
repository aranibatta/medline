from tobacco.aws.s3 import load_file_from_s3, store_file_in_s3
from medline.utilities.compression import extract
import os

from medline.topic_modeling.vectorizer import db_document_stream, tfidf_vectorize, load_vectorized_from_file, store_vectorized_to_file
from medline.topic_modeling.nmf import nmf, create_nmf_result_string, store_nmf_results_to_disk
from medline.topic_modeling.spca import spca, create_spca_result_string
from medline.utilities.compression import extract
import scipy
import numpy as np
import time
import pickle

import logging
logger = logging.getLogger('tobacco')

def load_file(file_name):

    if not os.path.exists('/tobacco'):
        os.makedirs('/tobacco')
        os.chmod('/tobacco', 0777)


    local_path = '/tobacco/{}'.format(file_name)

    load_file_from_s3(bucket='statnews-medline',
                      s3_path = "/{}".format(file_name),
                      local_path = local_path)

    # if necessary: extract
    if local_path.endswith('.gz'):
        local_path = extract(local_path)

    return local_path



def nmf_on_aws(tfidf_file_name, n_topics=10):
    '''

    :param tfidf_file_name: e.g. 'tfidf_20k' Note: do not enter a directory. that is handled by the script
    :return:
    '''

    # if necessary, download tfidf files
    if not os.path.exists('/tobacco/{}.npz'.format(tfidf_file_name)):
        load_file('{}.npz'.format(tfidf_file_name))
        load_file('{}.pickle'.format(tfidf_file_name))

    matrix, feature_names = load_vectorized_from_file('/tobacco/{}'.format(tfidf_file_name))

    doc_topic_matrix, topic_term_matrix = nmf(matrix, n_components=n_topics)

    logger.info(create_nmf_result_string(topic_term_matrix, feature_names))


    store_nmf_results_to_disk(doc_topic_matrix, topic_term_matrix, '/tobacco/{}_{}.mat'.format(tfidf_file_name, n_topics))

def spca_on_aws(tfidf_file_name, n_topics=10):

    # if necessary, download tfidf files
    if not os.path.exists('/tobacco/{}.npz'.format(tfidf_file_name)):
        load_file('{}.npz'.format(tfidf_file_name))
        load_file('{}.pickle'.format(tfidf_file_name))

    matrix, feature_names = load_vectorized_from_file('/tobacco/{}'.format(tfidf_file_name))

    logger.info("Running spca with {} topics.".format(n_topics))
    spca_model = spca(matrix, feature_names, center='row', card_docs=10000, card_terms=10, n_topics=n_topics,
                      deflation_type=0)
    logger.info(create_spca_result_string(spca_model, feature_names))

    pickle.dump(spca_model, open('/tobacco/spca_{}_{}.pickle'.format(tfidf_file_name, n_topics), 'wb'))
    store_file_in_s3(bucket_name='statnews-medline',
                     local_path='/tobacco/spca_{}_{}.pickle'.format(tfidf_file_name, n_topics),
                     s3_path='/spca/spca_{}_{}.pickle'.format(tfidf_file_name, n_topics))

def load_flask_server_reqs():
    '''
    Load required medline files
    :return:
    '''

    if not os.path.exists('/tobacco/medline_5mio'):
        os.makedirs('/tobacco/medline_5mio')
        os.chmod('/tobacco/medline_5mio', 0777)
    load_file_from_s3('statnews-medline', '/5mio/medline_5mio_doc_term.npz', '/tobacco/medline_5mio/medline_5mio_doc_term.npz')
    load_file_from_s3('statnews-medline', '/5mio/medline_5mio_doc_topic.npz', '/tobacco/medline_5mio/medline_5mio_doc_topic.npz')
    load_file_from_s3('statnews-medline', '/5mio/medline_5mio_topic_term.npz', '/tobacco/medline_5mio/medline_5mio_topic_term.npz')
    load_file_from_s3('statnews-medline', '/5mio/medline_5mio_feature_names.pickle', '/tobacco/medline_5mio/medline_5mio_feature_names.pickle')
    load_file_from_s3('statnews-medline', '/medline_fts_5mio.db.gz', '/tobacco/medline_5mio/medline_5mio.db.gz')

    extract('/tobacco/medline_5mio/medline_5mio.db.gz')



def batch_nmf():

    for i in [100, 200]:
        logger.info("NMF with {} topics".format(i))
        start = time.time()
        nmf_on_aws('tfidf_fts_5mio_20k', i)
        logger.info("NMF with {} topics took {} seconds".format(i, int(time.time() - start)))


if __name__ == "__main__":
#    batch_nmf()

    spca_on_aws('tfidf_20k', n_topics=100)


