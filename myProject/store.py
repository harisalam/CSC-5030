import mysql.connector
from mysql.connector import errorcode
from typing import List
from result import Result
from util import language_dict,db_config
import pymysql


class Store(object):
    def __init__(self,dbUser,dbPass,dbHost,dbName):
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.dbHost = dbHost
        self.dbName = dbName
        self.cnx = None

    def dbConnect(self):
        try:
            if self.dbName:
                self.cnx = mysql.connector.connect(
                    user=self.dbUser,
                    password=self.dbPass,
                    host=self.dbHost,
                    database=self.dbName
                )
            else:
                self.cnx = mysql.connector.connect(
                    user=self.dbUser,
                    password=self.dbPass,
                    host=self.dbHost
                )

        except mysql.connector.errorcode as errorcode:
            print(errorcode)
        print('Connection Successful')

        return self.cnx

    def createDatabase(self,cursor):

        try:
            if not self.dbName:
                self.dbName = db_config['db_name']
            cursor.execute(
                "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(self.dbName))
            print('database {} creation succeed'.format(self.dbName))

        except mysql.connector.errorcode as errcode:
            print("Failed creating database: {}".format(errcode))
            exit(1)

    def insertData(self,cursor,rows: List[Result],language):

        add_sentence = ("INSERT INTO " + language + "_sentences "
                                                    "(sentence) "
                                                    "VALUES (%s)")
        add_words = ("INSERT INTO  " + language + "_wordpos "
                                                  "(word, pos_tag, sentence)"
                                                  "VALUES (%s, %s, %s)")

        try:
            cursor.execute(add_sentence," Quebec lawyer and political figure.")
            insertSentenceId = cursor.lastrowid
            # print(insertSentenceId)
            data = [(row.word,row.posTag,insertSentenceId) for row in rows]
            cursor.executemany(add_words,data)
            self.cnx.commit()
            print('Data Insertion Completed')

        except Exception as e:
            self.cnx.rollback()
            print("Data Insertion error occured due to {}".format(e))
            print('insert error ',rows)

    def selectData(self,cursor,word,language):
        try:
            query = ("SELECT word, pos_tag, sentence FROM %s_wordpos WHERE word = %s") % (langauge,word)
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows
        except mysql.connector.errorcode as errorcode:
            print("Query execution error coz of {}".format(errorcode))

    def createTable(self,cursor,TABLES,TABLES_SENTENCES):

        for table_name in TABLES:
            table_description = TABLES[table_name]
            # print(table_describe)
            try:
                print("Creating table {}: \n".format(table_name),end='')
                cursor.execute(table_description)
                print('table %s creation succeed\n' % table_name)
            except Exception as err:
                print(err)

        for table_name in TABLES_SENTENCES:
            table_description = TABLES_SENTENCES[table_name]
            try:
                print("Creating table {}: \n".format(table_name),end='')
                cursor.execute(table_description)
                print('table %s creation succeed\n' % table_name)
            except mysql.connector.errorcode as err:
                print(err)

    # cursor.close()


if __name__ == "__main__":
    TABLES = {}
    for language in language_dict.values():
        TABLES[language + '_wordpos'] = (
                                            "CREATE TABLE IF NOT EXISTS  `%s_wordpos` ("
                                            "  `id` int(11) NOT NULL AUTO_INCREMENT,"
                                            "  `word` varchar(256) NOT NULL,"
                                            "  `pos_tag` varchar(64) NOT NULL,"
                                            "  `sentence` TEXT NOT NULL,"
                                            "  `create_time` timestamp NULL default CURRENT_TIMESTAMP,"
                                            "  PRIMARY KEY (`id`)"
                                            ")") % (language,)

    TABLES_SENTENCES = {}
    for language in language_dict.values():
        TABLES_SENTENCES[language + '_sentences'] = (
                                                        "CREATE TABLE IF NOT EXISTS  `%s_sentences` ("
                                                        "  `id` int(11) NOT NULL AUTO_INCREMENT,"
                                                        "  `sentence` TEXT NOT NULL,"
                                                        "  `create_time` timestamp NULL default CURRENT_TIMESTAMP,"
                                                        "  PRIMARY KEY (`id`)"
                                                        ")") % (language,)

    storeData = Store(db_config['user'],
                      db_config['password'],
                      dbHost=db_config['db_host'],
                      dbName=db_config['db_name'])

    conn = storeData.dbConnect()
    # mycursor = conn.cursor()
    storeData.createDatabase(conn.cursor())
    storeData.createTable(conn.cursor(),TABLES,TABLES_SENTENCES)
    print('Done')
