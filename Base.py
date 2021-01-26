# Import Built-in Module
from sys import exit
from math import ceil
from configparser import ConfigParser, NoSectionError

#import 3rd Party Module
import psycopg2
import pandas as pd

#import Internal Module
from Util import log

class QueryExcel:
    def __init__(self):
        self.__dbconn = None
        self.__config = None

    def __connect_db(self):
        self.__config = ConfigParser()
        self.__config.read("config.ini")

        try:
            host = self.__config.get("Database", "HOST")
            port = self.__config.get("Database", "PORT")
            dbname = self.__config.get("Database", "DBNAME")
            username = self.__config.get("Database", "USERNAME")
            password = self.__config.get("Database", "PASSWORD")
        except NoSectionError as e:
            log("Config Read Error => Cannot find the section %s on config.ini"%(e))
            exit()

        try:
            self.__dbconn = psycopg2.connect(host=host, port=port, dbname=dbname, user=username, password=password)
        except psycopg2.Error as e:
            log("DB connect Error => %s"%(e))
            exit()
    
    def __close_db(self):
        if self.__dbconn:
            self.__dbconn.close()
    
    def executeQuery(self, query):
        self.__connect_db()
        limit = self.__config.getint("Optimize", "LIMIT")
        iter_count = 1

        cursor = self.__dbconn.cursor()
        with self.__dbconn:
            with cursor:
                cursor.execute(query)
                # resultset = cursor.fetchall()
                
                tRows = cursor.rowcount
                if(tRows > limit):
                    iter_count = ceil(tRows/limit)
                
                for loop in range(iter_count):
                    resultset = cursor.fetchmany(limit)
                    with pd.ExcelWriter('Report.xlsx') as writer: 
                        df = pd.DataFrame(resultset)
                        df.to_excel(writer, sheet_name="Sheet%s"%(loop+1))
