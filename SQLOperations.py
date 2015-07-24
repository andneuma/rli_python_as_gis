# Class used to cleanly handle database operations via psycopg2

import psycopg2
import keyring
import datetime


class DBOperations():
    def __enter__(self):
        try:
            self.connection = psycopg2.connect(
                database=self.db_setup['db'],
                user=self.db_setup['user'],
                host=self.db_setup['host'],
                port=self.db_setup['port'],
                password=self.db_setup['password'])
            self.cur = self.connection.cursor()
        except psycopg2.DatabaseError as e:
            print("Could not connect to Database: ", e)
        return self

    def __init__(self, db, host, user, port=5432):
        self.db_setup = {
            'db': db,
            'host': host,
            'port': port,
            'password': keyring.get_password(db, user),
            'user': user}

    def execute_query(self, query):
        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
            return results
        except psycopg2.Error as e:
            print("ERROR during DB query: {e}".format(e=e.pgerror))
            self.connection.rollback()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.connection.close()
