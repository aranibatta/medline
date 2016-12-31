import numpy as np
from scipy.sparse import csr_matrix


from medline.topic_modeling.nmf import load_nmf_results_from_file

def enforce_sparsity_level(matrix, sparsity_level=0.1):
    '''
    Null out all values such that only sparsity_level of all entries are nonzero

    :param matrix:
    :param sparsity_level:
    :return:
    '''

def store_csr_matrix_to_file(matrix, file_path):

    np.savez('{}'.format(file_path), data=matrix.data, indices=matrix.indices, indptr=matrix.indptr,
             shape=matrix.shape)

def load_csr_matrix_from_file(file_path):

    y = np.load('{}'.format(file_path))
    matrix = csr_matrix( (y['data'], y['indices'], y['indptr']), shape=y['shape'])
    return matrix

if __name__ == "__main__":

    # Path where all necessary files are stored
    FOLDER = '/tobacco/medline_5mio/'
    # Path to database to be used
    DB_PATH = FOLDER + 'medline_fts_5mio.db'
    # Path to the nmf matrices to be used (has to contain both topic-term and document-topic matrices)
    NMF_MAT_PATH = FOLDER + 'tfidf_fts_5mio_20k_50.mat'
    # Path to pickle file containing the feature names
    FEATURE_NAMES_PATH = FOLDER + 'tfidf_fts_5mio_20k.pickle'

    # Loads doc-topic and topic-term matrices from file
    DOC_TOPIC_MATRIX, TOPIC_TERM_MATRIX = load_nmf_results_from_file(NMF_MAT_PATH)

    dtm = csr_matrix(DOC_TOPIC_MATRIX)
    store_csr_matrix_to_file(dtm, FOLDER + 'medline_5mio_doc_topic.npz')

    ttm = csr_matrix(TOPIC_TERM_MATRIX)
    store_csr_matrix_to_file(ttm, FOLDER + 'medline_5mio_topic_term.npz')