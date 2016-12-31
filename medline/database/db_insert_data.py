#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from collections import Counter
from medline.utilities.compression import extract
import datetime
import os
import re
import sqlite3
import gc
import codecs
from medline.database.db_wrapper import Database
from medline.database.parse_reference import parse_reference
import MySQLdb as mdb
import cPickle as pickle

from calendar import timegm
from time import gmtime, strftime, strptime, time, sleep
from dateutil import parser

from multiprocessing import Manager, Process, cpu_count


# multiprocessing globals
FLAG_ALL_DONE = "WORK FINISHED"
FLAG_WORKER_FINISHED_PROCESSING = "WORKER FINISHED PROCESSING"



def extract_refs_from_xml(source_folder, db_type, mysql_config=None, sqlite_db_path=None, processing_type='basic'):
    '''

    :param source_folder:
    :param db_type:
    :param mysql_config:
    :param sqlite_db_path:
    :param processing_type:
    :return:
    '''

    if db_type == 'mysql':
        db = Database(mysql_config)
    elif db_type == 'sqlite':
        db = sqlite3.connect(sqlite_db_path)
    else:
        raise NameError("Invalid db_type: {}. Valid are: 'mysql' and 'sqlite'.".format(type))

    if not processing_type in ['basic', 'complete', 'complete_normalized']:
        raise NameError("Invalid processing type. Valid are: 'basic', 'complete', 'complete_normalized")

    # Set up multiprocessing queues
    entry_queue = Manager().Queue()
    results_queue = Manager().Queue()
    number_of_processes = 8

    # Start process that will asynchronously add new references to the entry queue
    compressed_files = [source_folder + 'medline15n%04d.xml.gz' % x for x in range(1, 780)]
    #compressed_files = [source_folder + 'medline15n%04d.xml.gz' % x for x in [100, 200, 300, 400, 500, 600, 700]]
    #compressed_files = [source_folder + 'test3.xml.gz']
    async = Process(target = xml_to_soup_async, args=(compressed_files, entry_queue))
    async.start()

    # set up subprocesses
    for process in xrange(number_of_processes):
        p = Process(target=parse_references_worker, args=(entry_queue, results_queue, processing_type))
        p.start()

    processors_finished = 0
    insert_list = []
    while True:
        new_result = results_queue.get()
        if new_result == FLAG_WORKER_FINISHED_PROCESSING:
            processors_finished += 1
            print("Number of processes finished: {}".format(processors_finished))
            if processors_finished == number_of_processes:
                print "Inserting {} refs to {}. Length of results queue: {}. Length of entry queue: {}.".format(
                    len(insert_list), db_type, results_queue.qsize(), entry_queue.qsize() )
                insert_data_to_db(insert_list, db_type, db, processing_type)
                break

        else:
            insert_list.append(new_result)

            # After 1000 references, add to database
            if len(insert_list) == 1000:
                print "Inserting {} refs to {}. Length of results queue: {}. Length of entry queue: {}.".format(
                    len(insert_list), db_type, results_queue.qsize(), entry_queue.qsize() )
                insert_data_to_db(insert_list, db_type, db, processing_type)
                insert_list = []

def xml_to_soup_async(compressed_files_paths, entry_queue):
    '''
    Continuously Adds references (raw, unicode) to the entry queue in chunks of 100.
    Sleeps if there are 40.000 references in the queue

    :param compressed_files_paths: A list of filepaths to the medline .xml.gz files
    :param entry_queue:
    '''

    for compressed_file_path in compressed_files_paths:

        # one xml file contains 30.000 entries -> sleep if the queue already contains more than one to lower memory use
        # every entry is 100 references
        while entry_queue.qsize() > 400:
            sleep(5)

        print "Adding to queue: {}. Queue length: {}".format(compressed_file_path, entry_queue.qsize())
        extracted_file_path = extract(compressed_file_path, keep_original=True)

        # Loading the file as unicode makes beautifulsoup much faster .. who knew?
        with codecs.open(extracted_file_path, encoding='utf-8', mode='rb') as xml_file:
            xml = xml_file.read()

        # Find the start and ending points of all references
        starts = [m.start() for m in re.finditer('<MedlineCitation ', xml)]
        ends = [m.end() for m in re.finditer('</MedlineCitation>', xml)]

        # Add the references to the entry queue in chunks of 100
        start = 0
        end = 99
        while end < len(ends) -1:
            entry_queue.put(xml[starts[start]: ends[end]])
            start += 100
            end += 100

        try:
            entry_queue.put(xml[starts[start]:ends[-1]])
        except IndexError, e:
            print "Length xml: {}. Start: {}. End: {}".format(len(xml), starts[start], ends[-1])
            print e
            raise IndexError

    # In order for the child processes to finish, add finish flags after all files have been processed.
    for i in range(10):
        entry_queue.put(FLAG_ALL_DONE)

def parse_references_worker(entry_queue, results_queue, processing_type):
    '''

    Parses references from unicode xml string to beautiful soup to a dict of just the data that we need

    :param entry_queue:
    :param results_queue:
    :param processing_type:
    :return:
    '''

    while True:
        while entry_queue.qsize() == 0:
            print "sleeping in worker. Entry Queue empty"
            sleep(5)
        new_entry = entry_queue.get()
        if new_entry == FLAG_ALL_DONE:
            results_queue.put(FLAG_WORKER_FINISHED_PROCESSING)
            break
        else:
            # every new_entry is 100 references
            soup_reference = BeautifulSoup(new_entry)
            soup_refs = soup_reference.findAll('medlinecitation')
            for soup_ref in soup_refs:

                reference = parse_reference(soup_ref, processing_type)
                results_queue.put(reference)


def insert_data_to_db(insert_list, db_type, db, processing_type):
    '''
    Insert data to database, periodically called by extract_refs_from_xml

    :param insert_list: List of dicts with insert data
    :param db_type: 'mysql' or 'sqlite'
    :param db: the actual database
    :param processing_type:
    :return:
    '''
    refs_insert_list = []
    authors_insert_list = []
    topics_insert_list = []
    citations_insert_list = []

    for ref in insert_list:
        if processing_type == 'basic':
            refs_insert_list.append((ref['pmid'], ref['title'], ref['abstract'],
                                          ref['date_pub_first_str'], ref['date_pub_first_unix']))
        elif processing_type == 'complete':
            refs_insert_list.append((
                ref['pmid'], ref['title'], ref['abstract'], ref['authors_str'], ref['topics_major'], ref['topics_minor'],
                ref['pub_medium'], ref['journal_name'], ref['journal_volume'], ref['journal_issue'], ref['language'],
                ref['date_pub_first_str'], ref['date_pub_first_unix'], 0))

        elif processing_type == 'complete_normalized':

            refs_insert_list.append((
                ref['pmid'], ref['title'], ref['abstract'], ref['topics_major'], ref['topics_minor'],
                ref['pub_medium'], ref['journal_name'], ref['journal_volume'], ref['journal_issue'], ref['language'],
                ref['date_pub_first_str'], ref['date_pub_first_unix'], 0))

            for author in ref['authors_list']:
                authors_insert_list.append((author['pmid'], author['last_name'], author['fore_name'], author['affiliation']))

            for topic in ref['topics_list']:
                topics_insert_list.append((topic['pmid'], topic['mesh_id'], topic['major_topic']))

            for citation in ref['citations_list']:
                citations_insert_list.append((citation['citing_pmid'], citation['cited_pmid']))


    if db_type == 'sqlite':
        cur = db.cursor()

        if processing_type == 'basic':
            cur.executemany("INSERT OR IGNORE INTO refs VALUES (?, ?, ?, ?, ?)", refs_insert_list)
        elif processing_type == 'complete':
            cur.executemany("INSERT OR IGNORE INTO refs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            refs_insert_list)

        elif processing_type == 'complete_normalized':
            cur.executemany("INSERT OR IGNORE INTO refs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            refs_insert_list)
            cur.executemany("INSERT OR IGNORE INTO ref_authors(pmid, last_name, fore_name, affiliation) VALUES(?, ?, ?, ?)",
                            authors_insert_list)
            cur.executemany("INSERT OR IGNORE INTO ref_topics(pmid, mesh_id, major_topic) VALUES(?, ?, ?)",
                            topics_insert_list)
            cur.executemany("INSERT OR IGNORE INTO ref_citations(citing_pmid, cited_pmid) VALUES (?, ?)",
                            citations_insert_list)

#            y('INSERT INTO q1_person_name(first_name, last_name) VALUES (?,?)', data_person_name)

        db.commit()

if __name__ == "__main__":

    extract_refs_from_xml(source_folder='/home/stephan/tobacco/medline/', db_type='sqlite',
                          sqlite_db_path='/home/stephan/tobacco/medline/medline_complete.db',
                          processing_type='complete')
