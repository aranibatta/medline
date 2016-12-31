import sqlite3
import json
import numpy as np
import time

from medline.topic_modeling.spca import spca, spca_create_list_of_topic_terms_and_rowids
from medline.query.selection_matrix_cython import create_selection_matrix
from medline.topic_modeling.nmf import nmf, nmf_create_list_of_topic_terms_and_rowids
from medline.query.query_helpers import create_terms_of_topic_dict, search, load_query_globals

# Preload all relevant matrices and other files
preload = load_query_globals()

DB_PATH = preload['db_path']

NUMBER_TOPICS = preload['number_topics']
NUMBER_TERMS_PER_TOPIC = preload['number_terms_per_topic']
NUMBER_DOCS_PER_TOPIC = preload['number_docs_per_topic']

# Main tfidf doc-term matrix and feature names
DOC_TERM_MATRIX = preload['doc_term_matrix']
FEATURE_NAMES = preload['feature_names']

# Decomposed into doc-topic and topic-term matrix
DOC_TOPIC_MATRIX = preload['doc_topic_matrix']
TOPIC_TERM_MATRIX = preload['topic_term_matrix']
# dict of terms by topic
TERMS_OF_TOPICS_DICT = preload['terms_of_topics_dict']

# boolean doc-term matrix for looking up rowids for search query
LOOKUP_DOC_TERM_MATRIX = preload['lookup_doc_term_matrix']
LOOKUP_FEATURES_DICT = preload['lookup_features_dict']
# mapping of rowids to publication year. Necessary for the search
PUB_DATES_TO_ROWIDS = preload['pub_dates_to_rowids']

print "All matrices and features loaded. \n"


def run_search_query(search_query, start_year=1946, end_year=2015, query_type='standard',
                     number_topics=NUMBER_TOPICS, number_terms_per_topic = NUMBER_TERMS_PER_TOPIC,
                     number_docs_per_topic = NUMBER_DOCS_PER_TOPIC):

    start = time.time()
    print "Searching for {}. Type: {}. Start Year: {}. End Year: {}".format(search_query, query_type, start_year, end_year)

    rowids, hit_counts, frequencies = search(search_query, LOOKUP_DOC_TERM_MATRIX, LOOKUP_FEATURES_DICT,
                                             PUB_DATES_TO_ROWIDS, start_year=start_year, end_year=end_year)

    print "Finding rowids took {} seconds.".format(time.time() - start)

    if query_type == 'standard':
        selection_matrix = create_selection_matrix(DOC_TOPIC_MATRIX.data, DOC_TOPIC_MATRIX.indices,
                                                   DOC_TOPIC_MATRIX.indptr, DOC_TOPIC_MATRIX.shape, rowids)

        topics = standard_create_list_of_topic_terms_and_rowids(selection_matrix, TERMS_OF_TOPICS_DICT, rowids,
                number_topics=number_topics, docs_per_topic=number_docs_per_topic)


    elif query_type == 'nmf':
        # create selection matrix from document-term matrix
        doc_term_selection_matrix = create_selection_matrix(DOC_TERM_MATRIX.data, DOC_TERM_MATRIX.indices,
                                                            DOC_TERM_MATRIX.indptr, DOC_TERM_MATRIX.shape, rowids)
        nmf_dtm, nmf_ttm = nmf(doc_term_selection_matrix, n_components=number_topics)
        topics = nmf_create_list_of_topic_terms_and_rowids(nmf_dtm, nmf_ttm, FEATURE_NAMES, rowids,
                                           terms_per_topic=number_terms_per_topic, docs_per_topic=number_docs_per_topic)

    elif query_type == 'spca':
        # create selection matrix from document-term matrix
        doc_term_selection_matrix = create_selection_matrix(DOC_TERM_MATRIX.data, DOC_TERM_MATRIX.indices,
                                                            DOC_TERM_MATRIX.indptr, DOC_TERM_MATRIX.shape, rowids)

        card_docs = len(rowids)/30
        spca_model = spca(doc_term_selection_matrix, center='row', card_docs=card_docs,
                          card_terms=number_terms_per_topic, n_topics = number_topics, deflation_type=-1)
        topics = spca_create_list_of_topic_terms_and_rowids(spca_model, FEATURE_NAMES, rowids, max_docs=number_docs_per_topic)

    elif query_type == 'kmeans':
        pass

    else:
        raise Exception("Invalid query type")

    print topics

    output_json = to_JSON(topics, frequencies, len(rowids))
    print "Query took: {}".format(time.time() - start)

    return output_json

def standard_create_list_of_topic_terms_and_rowids(selection_matrix, terms_of_topic_dict, rowids, number_topics,
                                                   docs_per_topic):
    topics = []

    # Best topic ids: sum all columns, best topics are those with the highest counts
    sums = selection_matrix.sum(axis=0)
    sums = np.array(sums)[0]
    best_topic_ids = sums.argsort()[:-number_topics-1:-1]

    # transpose to topic-document matrix for easy document selection
    sm_transposed = selection_matrix.transpose().tocsr()

    for topic_id in best_topic_ids:

        topic_rowids = []
        # find selection_matrix indexes of the documents with the highest values
        for i in sm_transposed[topic_id].data.argsort()[: -docs_per_topic-1 : -1]:
            # find rowid within selection matrix
            local_rowid = sm_transposed[topic_id].indices[i]
            # parse selection_matrix indexes to general indexes
            topic_rowids.append(rowids[local_rowid])
        topics.append({
            'terms': terms_of_topic_dict[topic_id],
            'rowids': topic_rowids
        })

    return topics


def to_JSON(topics, frequencies, number_of_hits):
    """
    Takes in a list of topic ids and the frequency (by year) of the search term (also a list)

    Returns a JSON object of the following format:

    {
    'frequencies': [list of frequencies between 1946 and 2015. Length: 70, Values: [0-1]
    'topics':
        [//list of 5 topics, each with 8 terms and 5 documents
            {
                  'terms': ['eight', 'indivdiual', 'test', 'terms', 'to', 'work', 'with', 'here'],
                  'articles': [ //10 articles, string]
            }
        ]
    }

    """
    output = []
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    for topic in topics:
        articles = []

        for ids in topic['rowids']:
            query = "SELECT title, date_pub_first_str FROM refs WHERE rowid=\"" + str(ids) + "\";"
            row = cursor.execute(query)
            row = list(row)
            articles.append(row[0][0] + " - " + row[0][1])

        terms = topic['terms']

        print terms
        print articles

        output.append(
            {
                'terms': terms,
                'articles': articles
            })

    json_stuff = json.dumps(
        {
            'frequencies': frequencies.tolist(),
            'topics': output,
            'number_of_hits': number_of_hits
        })

    return json_stuff


if __name__ == "__main__":

#    run_search_query('cancer', start_year=1940, end_year=2015, query_type='spca')
    run_search_query('nicotine', start_year=1940, end_year=2015, query_type='standard')
    run_search_query('cancer', start_year=1940, end_year=2015, query_type='standard')
    run_search_query('cancer', start_year=1940, end_year=2015, query_type='spca')
#    run_search_query('cancer', start_year=1940, end_year=2015, query_type='standard')
 #   run_search_query('addiction', start_year=1940, end_year=2015, query_type='spca')

