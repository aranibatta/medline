import pyximport
pyximport.install()
import numpy as np
import cPickle as pickle
import time
import array

from scipy.sparse import csr_matrix, rand

from medline.topic_modeling.nmf import load_nmf_results_from_file, create_terms_of_topic_dict
#from medline.topic_modeling.query_modified import search_modified
from selection_matrix_cython import create_selection_matrix



def test():
    '''
    Creates a random 1000x1000 matrix, selects 100 rows randomly and stacks them.
    Then compares if they are equal to the original matrix

    :return:
    '''

    # Generate a 1000x1000 random sparse matrix
    A = rand(1000, 1000, density=0.01, format='csr')

    # Generate 100 rowids between 0 and 999 and sort them
    rowids = np.sort(np.random.randint(1000, size=100))
    rowids = np.array(rowids, dtype=np.int32)

    start = time.time()

    # Create the selection matrix
    selection = create_selection_matrix(A.data, A.indices, A.indptr, A.shape, rowids)

    print "Original shape of A: {}. Length of rowids: {}. Size of selection matrix: {}. Duration: {}".format(
        A.shape, len(rowids), selection.shape, time.time() - start)

    for index, rowid in enumerate(rowids):
        # for all rows, it has to hold that A[rowid] = selection[index[rowid]]
        assert np.array_equal(A[rowid].toarray(), selection[index].toarray())

    print "Test passed"





def test2():

    # Path where all necessary files are stored
    FOLDER = '/tobacco/medline_5mio/'
    # Path to the nmf matrices to be used (has to contain both topic-term and document-topic matrices)
    NMF_MAT_PATH = FOLDER + 'tfidf_fts_5mio_20k_50.mat'
    # Path to pickle file containing the feature names

    # Loads doc-topic and topic-term matrices from file
    DOC_TOPIC_MATRIX, TOPIC_TERM_MATRIX = load_nmf_results_from_file(NMF_MAT_PATH)

    DOC_TOPIC_MATRIX = csr_matrix(DOC_TOPIC_MATRIX)

    rowids = pickle.load(open('rowids.pickle'))
    print "loaded"
    start = time.time()
    rowids = np.array(rowids, dtype=np.int32)
    s = create_selection_matrix(DOC_TOPIC_MATRIX.data, DOC_TOPIC_MATRIX.indices, DOC_TOPIC_MATRIX.indptr,
                                DOC_TOPIC_MATRIX.shape, rowids)
    print time.time() - start
    print s.shape



if __name__ == "__main__":

    test()


    # # Path where all necessary files are stored
    # FOLDER = '/tobacco/medline_5mio/'
    # # Path to database to be used
    # DB_PATH = FOLDER + 'medline_fts_5mio.db'
    # # Path to the nmf matrices to be used (has to contain both topic-term and document-topic matrices)
    # NMF_MAT_PATH = FOLDER + 'tfidf_fts_5mio_20k_50.mat'
    # # Path to pickle file containing the feature names
    # FEATURE_NAMES_PATH = FOLDER + 'tfidf_fts_5mio_20k.pickle'
    #
    # # Loads doc-topic and topic-term matrices from file
    # DOC_TOPIC_MATRIX, TOPIC_TERM_MATRIX = load_nmf_results_from_file(NMF_MAT_PATH)
    #
    # DOC_TOPIC_MATRIX = csr_matrix(DOC_TOPIC_MATRIX)
    #
    # print DOC_TOPIC_MATRIX.shape
    #

    #
    #
    # start = time.time()
    # rowids = np.array(rowids, dtype=np.int32)
    # s = create_selection_matrix(DOC_TOPIC_MATRIX.data, DOC_TOPIC_MATRIX.indices, DOC_TOPIC_MATRIX.indptr,
    #                             DOC_TOPIC_MATRIX.shape, rowids)
    # print time.time() - start
    # print s.shape
    # start = time.time()
    # rowids = np.array(rowids, dtype=np.int32)
    # s = create_selection_matrix(DOC_TOPIC_MATRIX.data, DOC_TOPIC_MATRIX.indices, DOC_TOPIC_MATRIX.indptr,
    #                             DOC_TOPIC_MATRIX.shape, rowids)
    # print time.time() - start
    # print s.shape
    # start = time.time()
    # rowids = np.array(rowids, dtype=np.int32)
    # s = create_selection_matrix(DOC_TOPIC_MATRIX.data, DOC_TOPIC_MATRIX.indices, DOC_TOPIC_MATRIX.indptr,
    #                             DOC_TOPIC_MATRIX.shape, rowids)
    # print time.time() - start
    # print s.shape


