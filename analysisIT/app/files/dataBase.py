import psycopg2
from psycopg2 import IntegrityError
import json
from loguru import logger
from typing import Dict, Any

class DataBase:
    """
    Сlass with functions for working with a database
    """

    def __init__(self, dbname: str = 'mydatabase', username: str = 'myuser', password: str = 'mypassword', host: str = 'postgres'):
        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.conn = None
        self.cur = None
        self.count_of_data = None

    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(database=self.dbname, user=self.username, host=self.host,
                                         password=self.password)
            self.cur = self.conn.cursor()

            create_table_query = 'CREATE TABLE IF NOT EXISTS vacancies (vacancies_id SERIAL PRIMARY KEY, data_jsonb JSONB);'

            self.cur.execute(create_table_query)
            self.conn.commit()

        except Exception as err:
            logger.error(f'Error creating table: {err}')
            self.close_connection()

    def close_connection(self):
        if self.conn:
            self.cur.close()
            self.conn.close()
            logger.info('Database connection closed.')

    def execute_query(self, vacancy_id: str, vacancy_body: Dict[str, Any]) -> None:
        sql = 'INSERT INTO vacancies (vacancies_id, data_jsonb) VALUES (%s, %s)'
        try:
            json_body = json.dumps(vacancy_body)  # Convert dictionary to JSON string
            self.cur.execute(sql, (vacancy_id, json_body))
            self.conn.commit()
        except IntegrityError:
            self.conn.rollback()
            logger.warning('Integrity error occurred. Rolling back transaction.')
        except Exception as err:
            logger.error(f'Error executing query: {err}')
            self.conn.rollback()

    def count_of_data(self) -> int:
        try:
            self.cur.execute("SELECT COUNT(*) FROM vacancies")
            row_count = self.cur.fetchone()[0]
            self.count_of_data = row_count
        except Exception as e:
            row_count = 0
        return row_count

if __name__ == '__main__':
    db = DataBase(dbname='mydb', username='postgres', host='localhost', password='2280')
    db.connect()
    db.execute_query(vacancy_id=str(123), vacancy_body={'body': '123', 'vacancies_id': 123})
    db.close_connection()
