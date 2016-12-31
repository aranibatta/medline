import urllib2, base64
from medline.database.db_wrapper import Database
from medline.database.parse_reference import parse_date
from medline.database.db_insert_data import extract_refs_from_xml, insert_data_to_db
from bs4 import BeautifulSoup
import pickle
import sqlite3
import codecs
import re



def download_xml_files(storage_folder, start=1, end=779):

    url = 'https://ftp.nlm.nih.gov/projects/medleasebaseline/gz/'
    files = ['medline15n%04d.xml.gz' % x for x in range(start, end + 1)]
    print files

  #  files = ['medline15n0005.xml.gz']

    for file in files:
        print "Downloading {} to {}".format(file, storage_folder + file)

        request = urllib2.Request(url + file)
        base64string = base64.encodestring('%s:%s' % ('risi', 'stfdu')).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        #result = urllib2.urlopen(request)
        data = urllib2.urlopen(request).read()

        local_file = open(storage_folder + file, 'wb')
        local_file.write(data)
        local_file.close()


def create_medline_db_mysql():

    db = Database('stephan_local')
    con, cur = db.connect(select_no_db=True)


    cur.execute('''CREATE DATABASE IF NOT EXISTS medline
                      DEFAULT CHARACTER SET utf8
                      DEFAULT COLLATE utf8_general_ci;''')
    cur.execute("USE medline;")
    #con.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS refs(
                      pmid          varchar(20)   NOT NULL UNIQUE,
                      pmid_version  int,
                      e_location_id varchar(255),
                      ref_owner     varchar(255),
                      ref_status    varchar(255),
                      date_updated  date,

                      title         varchar(255)  NOT NULL,
                      abstract      text,
                      pages         varchar(255),
                      date_pub_first      date    NOT NULL,
                      date_pub_print      date,
                      date_pub_electronic date,
                      pub_medium    varchar(255),
                      lang          varchar(255),
                      times_cited   int,

                      journal_id    varchar(20)   NOT NULL,
                      journal_volume varchar(10),
                      journal_issue varchar(10),

                      PRIMARY KEY(pmid)

                      )ENGINE=INNODB;''')


    cur.execute('''CREATE TABLE IF NOT EXISTS journals(
                      nlm_id          varchar(20) NOT NULL UNIQUE,
                      medline_ta      varchar(255),
                      issn_print      varchar(20),
                      issn_electronic varchar(20),
                      country         varchar(20),

                      PRIMARY KEY(nlm_id)
                      )ENGINE=INNODB;''')


    cur.execute('''CREATE TABLE IF NOT EXISTS refs_authors(
                      id          int           NOT NULL AUTO_INCREMENT,
                      ref_id      varchar(20)   NOT NULL,
                      last_name   varchar(255),
                      fore_name   varchar(255),
                      affiliation text,

                      PRIMARY KEY(id),

                      FOREIGN KEY(ref_id)
                        REFERENCES refs(pmid)
                        ON UPDATE CASCADE ON DELETE CASCADE,

                      UNIQUE KEY ref_author(ref_id, last_name, fore_name)
                    )ENGINE=INNODB;''')


    cur.execute('''CREATE TABLE IF NOT EXISTS refs_pubtypes(
                        id            int           NOT NULL AUTO_INCREMENT,
                        ref_id        varchar(20)   NOT NULL,
                        pubtype_id    varchar(20)   NOT NULL,
                        pubtype_name  varchar(255)  NOT NULL,

                        PRIMARY KEY(id),

                        FOREIGN KEY(ref_id)
                          REFERENCES refs(pmid)
                          ON UPDATE CASCADE ON DELETE CASCADE,

                        UNIQUE KEY ref_pubtype(ref_id, pubtype_id)
                    )ENGINE=INNODB;''')

    cur.execute('''CREATE TABLE IF NOT EXISTS meshes(
                      id            varchar(20)   NOT NULL UNIQUE,
                      name          varchar(100)  NOT NULL,
                      date_created  date,
                      parent        varchar(20),

                      PRIMARY KEY(id)
                    )ENGINE=INNODB;''')

    cur.execute('''CREATE TABLE IF NOT EXISTS refs_meshes(
                      id          int         NOT NULL AUTO_INCREMENT,
                      ref_id      varchar(20) NOT NULL,
                      mesh_id     varchar(20) NOT NULL,
                      major_topic tinyint(1),

                      PRIMARY KEY(id),

                      FOREIGN KEY(ref_id)
                        REFERENCES refs(pmid)
                        ON UPDATE CASCADE ON DELETE CASCADE,

                      FOREIGN KEY(mesh_id)
                        REFERENCES meshes(id)
                        ON UPDATE CASCADE ON DELETE CASCADE
                    )ENGINE=INNODB;''')


    cur.execute('''CREATE TABLE IF NOT EXISTS citations(
                      id          int         NOT NULL AUTO_INCREMENT,
                      citing_id   varchar(20) NOT NULL,
                      cited_id    varchar(20) NOT NULL,
                      ref_type    varchar(20),
                      ref_source  varchar(255),

                      PRIMARY KEY(id),

                      FOREIGN KEY(citing_id)
                        REFERENCES refs(pmid),

                      FOREIGN KEY(cited_id)
                        REFERENCES refs(pmid)
                    )ENGINE=INNODB;''')


def create_medline_db_sqlite(db_file_path, processing_type='complete'):

    con = sqlite3.connect(db_file_path)

    cur = con.cursor()

    if processing_type == 'basic':
        cur.execute('''CREATE TABLE IF NOT EXISTS refs(
                        pmid                text  NOT NULL UNIQUE,
                        title               text  NOT NULL,
                        abstract            text,
                        date_pub_first_str  text,
                        date_pub_first_unix integer,

                        PRIMARY KEY (pmid)
                        );''')

    if processing_type == 'complete':
        cur.execute('''CREATE TABLE IF NOT EXISTS refs(
                        pmid                text  NOT NULL UNIQUE,
                        title               text  NOT NULL,
                        abstract            text,
                        authors             text,
                        topics_major        text,
                        topics_minor        text,

                        pub_medium          text,
                        journal_name        text,
                        journal_volume      text,
                        journal_issue       text,
                        language            text,
                        
                        date_pub_first_str        text,
                        date_pub_first_unix       integer,

                        times_cited         integer,

                        PRIMARY KEY (pmid)
                        );''')


    if processing_type == 'complete_normalized':
        cur.execute('''CREATE TABLE IF NOT EXISTS refs(
                        pmid                integer  NOT NULL UNIQUE,
                        title               text  NOT NULL,
                        abstract            text,
                        topics_major        text,
                        topics_minor        text,

                        pub_medium          text,
                        journal_name        text,
                        journal_volume      text,
                        journal_issue       text,
                        language            text,

                        date_pub_first_str        text,
                        date_pub_first_unix       integer,

                        times_cited         integer,

                        PRIMARY KEY (pmid)
                        );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ref_authors(
                        id            INTEGER PRIMARY KEY,
                        pmid          INTEGER NOT NULL,
                        last_name     TEXT,
                        fore_name     TEXT,
                        affiliation   TEXT
                        );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ref_topics(
                        id            INTEGER PRIMARY KEY,
                        pmid          INTEGER NOT NULL,
                        mesh_id       TEXT NOT NULL,
                        major_topic   INTEGER NOT NULL
                        );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ref_citations(
                        id            INTEGER PRIMARY KEY,
                        citing_pmid   INTEGER NOT NULL,
                        cited_pmid    INTEGER NOT NULL
                        );''')


        cur.execute('''CREATE TABLE IF NOT EXISTS topics(
                        id            INTEGER PRIMARY KEY,
                        mesh_id       TEXT NOT NULL,
                        tree_id       TEXT NOT NULL,
                        name          TEXT NOT NULL,
                        date_created_str  TEXT,
                        date_created_unix INTEGER
                        );''')


def add_mesh_data_to_db(mesh_folder, db_type='sqlite', sqlite_db_path=None):
    '''
    Adds all mesh topics to the meshes db table

    TODO: add treenumberlist. ?Maybe add just the first?

    :param mesh_folder: folder with mesh.xml (raw data) or meshes_list.pickle (extracted data)
    :return:
    '''


    try:
        meshes_list = pickle.load(open(mesh_folder + 'meshes_list.pickle', 'rb'))
    except IOError:
        print "Meshes_list does not yet exist, creating now..."
        meshes_list = mesh_xml_to_pickle(mesh_folder)

    insert_list = []
    for mesh in meshes_list:
        insert_list.append((mesh['mesh_id'], mesh['tree_id'], mesh['name'], mesh['date_created_str'],
                            mesh['date_created_unix']))

    if db_type == 'sqlite':
        db = sqlite3.connect(sqlite_db_path)

    cur = db.cursor()
    cur.executemany('''INSERT OR IGNORE INTO topics(mesh_id, tree_id, name, date_created_str, date_created_unix)
                        VALUES (?, ?, ?, ?, ?)''',insert_list)
    db.commit()

def add_fulltext_search_to_db(sqlite_db_path):
    '''
    Adds an fts4 reverse lookup table to the database

    :param sqlite_db_path:
    :return:
    '''

    db = sqlite3.connect(sqlite_db_path)
    cur = db.cursor()
    # add reverse lookup table to db, tokenize with porter stemmer
    cur.execute('create virtual table refs_lookup using fts4(pmid, date_pub_first_year, title, abstract, topics_major, '
                'topics_minor, tokenize = porter);')
    cur.execute('insert into refs_lookup select pmid, substr(date_pub_first_str, 1, 4), title, abstract, topics_major,'
                ' topics_minor from refs;')
    db.commit()

#   create virtual table refs_lookup using fts4(pmid, date_pub_first_year, title, abstract, topics_major, topics_minor, tokenize = porter);
# insert into refs_lookup select pmid, substr(date_pub_first_str, 1, 4), title, abstract, topics_major, topics_minor from refs;


def mesh_xml_to_pickle(mesh_folder):

    print "Loading mesh xml into beautiful soup"
    with codecs.open(mesh_folder + 'mesh.xml', encoding='utf-8', mode='rb') as xml_file:
        xml = xml_file.read()

    print len(xml)


    starts = [m.start() for m in re.finditer('<DescriptorRecord ', xml)]
    ends = [m.end() for m in re.finditer('</DescriptorRecord>', xml)]

    meshes_list = []

    for i, _ in enumerate(starts):
        text = xml[starts[i]:ends[i]]
        soup_mesh =  BeautifulSoup(text).findAll('descriptorrecord')[0]

        main_mesh = {'mesh_id':      soup_mesh.descriptorui.text,
                     'name':         soup_mesh.descriptorname.text.strip()}

        try:
            main_mesh['date_created_str'], main_mesh['date_created_unix'] = parse_date(soup_mesh.datecreated)
        except AttributeError:
            print "Problem with date"
            print soup_mesh.descriptorui.text, soup_mesh.descriptorname.text.strip()
            print soup_mesh.dateestablished
            main_mesh['date_created_str'] = None
            main_mesh['date_created_unix'] = None


        for treenumber_mesh in soup_mesh.findAll('treenumber'):
            tree_mesh = main_mesh
            tree_mesh['tree_id'] = treenumber_mesh.text

            meshes_list.append(tree_mesh)

    pickle.dump(meshes_list, open(mesh_folder + 'meshes_list.pickle', 'wb'))
    return meshes_list

def create_sample_db(original_db_path, new_db_path, number_of_docs=1000000, fts=False):
    '''
    Using a complete medline db, this script creates a version with number_of_docs randomly selected ones.

    :param original_db_path:
    :param new_db_path:
    :param number_of_docs: number of documents to be added
    :param fts: True for add fts table
    :return:
    '''

    print "Adding {} randomly selected documents from {} to {}\n\n".format(number_of_docs, original_db_path, new_db_path)

    con_orig = sqlite3.connect(original_db_path)
    cur_orig = con_orig.cursor()
    con_new = sqlite3.connect(new_db_path)

    query = '''SELECT pmid, title, abstract, authors, topics_major, topics_minor, pub_medium, journal_name,
                      journal_volume, journal_issue, language, date_pub_first_str, date_pub_first_unix
                      FROM refs ORDER BY RANDOM() LIMIT {};'''.format(number_of_docs)
    insert_list = []

    print "Running query: {}".format(query)
    for row in cur_orig.execute(query):
        if len(insert_list) >= 1000:
            print "inserting 1000 entries"
            insert_data_to_db(insert_list, db_type='sqlite', db=con_new, processing_type='complete')
            insert_list = []

        insert_list.append({
               'pmid': row[0],
               'title': row[1],
               'abstract': row[2],
               'authors_str': row[3],
               'topics_major': row[4],
               'topics_minor': row[5],
               'pub_medium': row[6],
               'journal_name': row[7],
               'journal_volume': row[8],
               'journal_issue': row[9],
               'language': row[10],
               'date_pub_first_str': row[11],
               'date_pub_first_unix': row[12]})

    insert_data_to_db(insert_list, db_type='sqlite', db=con_new, processing_type='complete')

    if fts:
        print "Adding fts table"
        add_fulltext_search_to_db(new_db_path)



if __name__ == "__main__":

    sqlite_db_path = '/home/stephan/tobacco/medline/medline_fts_5mio.db'
    processing_type = 'complete'
    source_folder = '/home/stephan/tobacco/medline/'
    db_type = 'sqlite'

    create_medline_db_sqlite(sqlite_db_path, processing_type=processing_type)
    # extract_refs_from_xml(source_folder=source_folder, db_type=db_type,
    #                       sqlite_db_path=sqlite_db_path,
    #                       processing_type=processing_type)
    #add_fulltext_search_to_db(sqlite_db_path)

    create_sample_db(original_db_path='/home/stephan/tobacco/medline/medline_complete.db',
                     new_db_path=sqlite_db_path,
                     number_of_docs=5000000,
                     fts=True)

#   create virtual table refs_lookup using fts4(pmid, date_pub_first_year, title, abstract, topics_major, topics_minor, tokenize = porter);
# insert into refs_lookup select pmid, substr(date_pub_first_str, 1, 4), title, abstract, topics_major, topics_minor from refs;