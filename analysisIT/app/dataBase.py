import psycopg2
from psycopg2 import IntegrityError
import json
from loguru import logger


class DataBase:

    def __init__(self, dbname: str, username: str, password: str, host: str):
        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.conn = None
        self.cur = None

    def connect(self, tableName: str):
        try:
            self.conn = psycopg2.connect(database=self.dbname, user=self.username, host=self.host, password=self.password)
            self.cur = self.conn.cursor()

            create_table_query = f'CREATE TABLE IF NOT EXISTS {tableName} (vacancies_id SERIAL PRIMARY KEY, data_jsonb JSONB);'

            self.cur.execute(create_table_query)
            self.conn.commit()  # Commit the table creation

        except Exception as err:
            logger.error(f'Error creating table: {err}')
            self.closeConnection()  # Close connection on error

    def closeConnection(self):
        if self.conn:
            self.cur.close()
            self.conn.close()
            logger.info('Database connection closed.')

    def executeQuery(self, tableName: str, vacancyId: int, vacancyBody: dict):
        sql = f'INSERT INTO {tableName} (vacancies_id, data_jsonb) VALUES (%s, %s)'
        try:
            json_body = json.dumps(vacancyBody)  # Convert dictionary to JSON string
            self.cur.execute(sql, (vacancyId, json_body))
            self.conn.commit()
            logger.info('Data inserted successfully.')
        except IntegrityError:
            self.conn.rollback()
            logger.warning('Integrity error occurred. Rolling back transaction.')
        except Exception as err:
            logger.error(f'Error executing query: {err}')
            self.conn.rollback()  # Rollback transaction on error

if __name__ == '__main__':
    db = DataBase(dbname='mydb', username='postgres', host='localhost', password='2280')
    db.connect(tableName='vacancies')
    db.executeQuery(tableName='vacancies', vacancyId=123, vacancyBody={'body': '123', 'vacancies_id': 123})
    db.closeConnection()