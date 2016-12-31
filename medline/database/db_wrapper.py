#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Wrapper around MySQLdb for establishing connections and creating/dropping indexes.
Configs are stored in db_configs.py
'''
# import logging
# logger = logging.getLogger('tobacco')
import MySQLdb as mdb

from medline.database.db_configs import DB_CONFIGS


class Database:
    # for future reference: this is fucking inefficient, and, yes, you should clean it up (use parameter directly)
    def __init__(self, name):
        self.config = DB_CONFIGS[name]

    def connect(self, select_no_db=False):
        '''
        Connects to database and returns connection and cursor
        :param select_no_db:  set to True if the connection should NOT use the specified database (useful for
        initializing a database
        :return:connection and cursor
        '''

        if select_no_db is True:
            con = mdb.connect(host=self.config['hostname'], port=self.config['port'], user=self.config['user'],
                            passwd=self.config['password'])
            cur = con.cursor(mdb.cursors.SSDictCursor)
        else:
            con = mdb.connect(host=self.config['hostname'], port=self.config['port'], user=self.config['user'],
                            passwd=self.config['password'], db=self.config['database'])
            cur = con.cursor(mdb.cursors.SSDictCursor)
        return con, cur

    def get_connection_dict(self):
        return self.config

    def create_index(self, database, table, column):
        '''
        Creates an index on column if it does not yet exist
        :param table:
        :param column:
        :return: True if index is created, False if index already exists
        '''

        con, cur = self.connect()
        cur.execute("USE {};".format(database))
        try:
            # sample: "CREATE INDEX token on cancer_freq_raw(token)
            print("Creating index {} on {}".format(column, table))
            cur.execute("CREATE INDEX {0} on {1}({0})".format(column, table))
            con.close()
            return True
        except mdb.Error, e:
            print("Index on {} in table {} already exists. {}".format(column, table, e))
            con.close()
            return False

    def drop_index(self, database, table, column):
        con, cur = self.connect()
        cur.execute("USE {};".format(database))

        try:
            cur.execute("DROP INDEX {} on {}".format(column, table))
            print("Dropping index {} from {}".format(column, table))
            con.close()
            return True
        except mdb.Error, e:
            print "Index does not exist"
            con.close()
            return False


    def batch_insert(self, table_name, key_list, values, chunk_size=100):
        '''

        Automated batch inserts. Automatically produces chunks and inserts them

        :param table_name:
        :param key_list: list of keys, e.g. ['db_name', 'total', 'token_id']
        :param values: list of dicts containing the values from the key_list,
                        e.g. [{'db_name': 'pm', 'total': 2342, 'token_id': 23},...]
        :param chunk_size: number of entries to be inserted at once. Default: 100
        :return:
        '''

        con, cur = self.connect()

        # turns the list of dicts into a list of tuples,
        # e.g. [{'key': 1, 'value': 2}, {'key': 3, 'value': 4}] -> [(1,2), (3, 4)]
        insert_list = [tuple(entry[key] for key in key_list) for entry in values]
        # splits the insert list into chunks of size chunk_size (default: 100)
        insert_list_chunked = [insert_list[i:i+chunk_size] for i in xrange(0, len(insert_list), chunk_size)]
        del insert_list

        count = 0
        for chunk in insert_list_chunked:
            count += 1
            if count % 10 == 0: print "Inserted: {}".format(count*chunk_size)
            try:
                cur.executemany('''REPLACE INTO {} ({}) VALUES ({});'''.format(
                    table_name,
                    ", ".join(key_list), # e.g. 'key, value'
                    ", ".join(["%s" for key in key_list])), chunk) # e.g. '%s, %s
            # occasionally, there are lock wait timeout exceeded errors (-> insert takes too long) -> smaller chunks
            except mdb.OperationalError:
                print("batch insert lock wait timeout. reducing chunk size.")
                self.batch_insert(table_name, key_list, values, chunk_size/4)
                return
            except mdb.ProgrammingError:
                print("Insert failed, printing last statement:")
                print(cur._last_executed)
                raise



        con.commit()
        con.close()




if __name__ == "__main__":
    pass
    print "here"
