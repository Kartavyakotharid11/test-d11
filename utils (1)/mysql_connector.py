# Databricks notebook source
import pymysql
import time
from pymysql import MySQLError, InterfaceError
# from mysql.connector import MySQLConnection, Error, errors


# COMMAND ----------

class PyMYSQLDBConn(object):
    """docstring for PyMYSQLDBConn"""
    def __init__(self, db_conn):
        super(PyMYSQLDBConn, self).__init__()
        self.db_conn = db_conn
        try:
            self.connection = pymysql.connect(**db_conn)
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        except MySQLError as e:
            print(e)
    def _reset_con(self):
        self.connection = pymysql.connect(**self.db_conn)
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
    def execute_query(self,query):
        if not self.connection:
            self._reset_con()
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
            #return self.cursor
             
        except pymysql.err.OperationalError or pymysql.err.InterfaceError or pymysql.err.InternalError as e:
            print('MySQLConnection disconnected.. reconnecting again..')
            time.sleep(2)
            self._reset_con()
            return self.execute_query(query)
        except Exception as e:
            print('Exception in executing query, ', query, self.connection, e)
            raise e
    def execute_proc(self,proc,args):
        try:
            self.cursor.callproc(proc,args)
            return self.cursor.fetchall()
        except Exception as e:
            raise e
    def crud_data(self,insert_stmt):
        try:
            self.cursor.execute(insert_stmt)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("Error in inserting data, {0}".format(insert_stmt))
            raise e
    def __del__(self):
        # self.cursor.close()
        self.connection.close()

