'''
Helper files for query.py, Mostly to help with loading the initial data
'''

import sqlite3
import pickle
import numpy as np

from medline.topic_modeling.vectorizer import load_feature_names_from_file
from medline.utilities.sparse_matrices import load_csr_matrix_from_file

PATH = '/tobacco/medline_5mio/'


# Total counts per year
YEARLY_TOTALS = np.array([53781, 64743, 70460, 62508, 85563, 103640, 107944, 108660, 105619, 108435, 106937, 111396, 109386, 109895, 112089, 120019, 125538, 141265, 161852, 176539, 179586, 191639, 207513, 214498, 218438, 222374, 226599, 230496, 234440, 248184, 254159, 261056, 271474, 280688, 279313, 282048, 294167, 308110, 317130, 327034, 340369, 357936, 375451, 391211, 398767, 399805, 404208, 410126, 420885, 430812, 435813, 446211, 460022, 473931, 501831, 532951, 554086, 583116, 622344, 676592, 698294, 716512, 751258, 777510, 827922, 877690, 905759, 874012, 271606, 16])




def load_query_globals():

    '''
    aws links:
    doc_term_matrix:   https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_doc_term.npz
    topic_term_matrix: https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_topic_term.npz
    doc_topic_matrix:  https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_doc_topic.npz
    feature_names:     https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_feature_names.pickle

    lookup_doc_term_matrix: https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_lookup.npz
    lookup_features:    https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/medline_5mio_lookup.pickle
    rowid_to_year:      https://s3-us-west-2.amazonaws.com/statnews-medline/5mio/rowid_to_year.pickle


    :return:
    '''

    # Total counts per year
    YEARLY_TOTALS = np.array([53781, 64743, 70460, 62508, 85563, 103640, 107944, 108660, 105619, 108435, 106937, 111396, 109386, 109895, 112089, 120019, 125538, 141265, 161852, 176539, 179586, 191639, 207513, 214498, 218438, 222374, 226599, 230496, 234440, 248184, 254159, 261056, 271474, 280688, 279313, 282048, 294167, 308110, 317130, 327034, 340369, 357936, 375451, 391211, 398767, 399805, 404208, 410126, 420885, 430812, 435813, 446211, 460022, 473931, 501831, 532951, 554086, 583116, 622344, 676592, 698294, 716512, 751258, 777510, 827922, 877690, 905759, 874012, 271606, 16])

    # Path where all necessary files are stored
    PATH = '/tobacco/medline_5mio/'
    # Path to database to be used
    DB_PATH = PATH + 'medline_5mio.db'

    NUMBER_TOPICS = 5
    NUMBER_TERMS_PER_TOPIC = 8
    NUMBER_DOCS_PER_TOPIC = 8

    # Loads doc-topic and topic-term matrices from file
    DOC_TOPIC_MATRIX = load_csr_matrix_from_file(PATH + 'medline_5mio_doc_topic.npz')
    TOPIC_TERM_MATRIX = load_csr_matrix_from_file(PATH + 'medline_5mio_topic_term.npz')
    DOC_TERM_MATRIX = load_csr_matrix_from_file(PATH + 'medline_5mio_doc_term.npz')
    FEATURE_NAMES = load_feature_names_from_file(PATH + 'medline_5mio_feature_names.pickle')

    LOOKUP_DOC_TERM_MATRIX = load_csr_matrix_from_file(PATH + 'medline_5mio_lookup.npz').tocsc()
    LOOKUP_FEATURES = load_feature_names_from_file(PATH + 'medline_5mio_lookup.pickle')
    LOOKUP_FEATURES_DICT = {LOOKUP_FEATURES[i]:i for i in range(len(LOOKUP_FEATURES))}

    PUBLICATION_DATES = pickle.load(open(PATH + 'rowid_to_year.pickle'))
    PUB_DATES_TO_ROWIDS = {i:PUBLICATION_DATES[i] for i in range(len(PUBLICATION_DATES))}

    # Constructs a dict that maps topic ids to terms
    TERMS_OF_TOPICS_DICT = create_terms_of_topic_dict(features=FEATURE_NAMES, topic_term_matrix=TOPIC_TERM_MATRIX,
                                                      num_terms=NUMBER_TERMS_PER_TOPIC)

    return {'db_path': DB_PATH,

            'number_topics': NUMBER_TOPICS,
            'number_terms_per_topic': NUMBER_TERMS_PER_TOPIC,
            'number_docs_per_topic': NUMBER_DOCS_PER_TOPIC,

            'doc_term_matrix': DOC_TERM_MATRIX,
            'feature_names': FEATURE_NAMES,

            'doc_topic_matrix': DOC_TOPIC_MATRIX,
            'topic_term_matrix': TOPIC_TERM_MATRIX,
            'terms_of_topics_dict': TERMS_OF_TOPICS_DICT,

            'lookup_doc_term_matrix': LOOKUP_DOC_TERM_MATRIX,
            'lookup_features_dict': LOOKUP_FEATURES_DICT,
            'pub_dates_to_rowids': PUB_DATES_TO_ROWIDS}



def search(search_query, lookup_doc_term_matrix, lookup_features_dict, pub_dates_to_rowids, start_year=1946,
           end_year=2015):
    '''
    Search implemented with a boolean document-term matrix
    For every term of the query, it looks up rows, which contain the term.
    If there are multiple terms, it uses intersect to find rows where both/all appear.

    Then only retains rows where start_year <= year <= end_year and counts number of hits by years
    :param search_query:
    :param start_year:
    :param end_year:
    :return:
    '''

    # 1 : find all rowids matching query
    rowids = None
    for term in search_query.split():

        term_rowids = set()

        # find column of term
        col = lookup_features_dict[term]

        # find start and ending indices from indptr
        start = lookup_doc_term_matrix.indptr[col]
        end = lookup_doc_term_matrix.indptr[col+1]

        # add rowids to set
        for rowid in lookup_doc_term_matrix.indices[start:end]:
            term_rowids.add(rowid)

        if rowids is None:
            rowids = term_rowids
        else:
            rowids = rowids.intersection(term_rowids)

    # 2: retain rowid if start_year <= year <= end_year and count occurrences
    filtered_rowids = []
    yearly_counts = np.zeros(70)
    for rowid in rowids:

        pub_year = pub_dates_to_rowids[rowid]
        if pub_year < start_year or pub_year > end_year:
            continue

        filtered_rowids.append(rowid)
        yearly_counts[pub_year - 1946] += 1

    rowids = np.array(filtered_rowids, dtype=np.int32)
    frequencies = yearly_counts / YEARLY_TOTALS

    return rowids, yearly_counts, frequencies

def create_terms_of_topic_dict(features, topic_term_matrix, num_terms=8):

    """
    Creates a dictionary containing a mapping from topics to a list of terms
    """


    dic = {}

    for r in range(topic_term_matrix.shape[0]):
        terms = []
        for i in topic_term_matrix[r].data.argsort()[: -num_terms-1 :-1]:
            # this gives us the index of the best indices, which we now have to convert to feature indexes
            index = topic_term_matrix[r].indices[i]
            terms.append(features[index])
        dic[r] = terms

    return dic

def store_years_of_docs_as_list(db_path, pickle_file_path):
    '''
    Store the publication years of all documents in a list and store the list to a file
    :param db_path:
    :param pickle_file_path:
    :return:
    '''

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    pub_dates = []
    # add first value (none), so the rows match with the main matrix
    pub_dates.append(None)

    for row in cursor.execute("SELECT date_pub_first_year from refs_lookup;"):
        try:
            pub_dates.append(int(row[0]))
        except TypeError:
            pub_dates.append(None)

    print len(pub_dates)
    pickle.dump(pub_dates, open(pickle_file_path, 'wb'), -1)

def print_yearly_totals(db_path, start_year=1946, end_year=2015):
    '''
    Prints out the yearly totals in a format suitable for a list
    Idea: keep the list in memory instead of loading it from disk for every query.
    -> store it with pickle and load when necessary.
    sample return: np.array([398767, 399805, 404208, 410126, 420885, 430812, 435813, 446211, 460022, 473931, 501831, 532951, 554086, 583116, 622344, 676592, 698294, 716512, 751258, 777510, 827922, 877690, 905759, 874012, 271606, 16])

    :return: a string that can will create a numpy array, holding the total number of articles between start and end year
    '''

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    totals = []
    for year in range(start_year, end_year + 1):
        cursor.execute('SELECT COUNT(*) from refs_lookup WHERE date_pub_first_year MATCH "{}";'.format(year))
        count = cursor.fetchone()[0]
        totals.append(str(count))
        print "{}: {}".format(year, count)
    totals = ', '.join(totals)
    totals = 'np.array([{}])'.format(totals)
    return totals









if __name__ == "__main__":
#    print search('nicotine')
#    store_years_of_docs_as_list(db_path=PATH + 'medline_5mio.db',
#                                pickle_file_path=PATH + 'rowid_to_year.pickle')

    a = load_query_globals()