import time
from datetime import datetime, timedelta, date
from loguru import logger
import queue
import json
from typing import Dict, List, Set, Any, Union
import requests
from dataBase import DataBase

ID_ROLES_LIST = ['118', '114', '164', '126', '112', '10', '25', '38', '171', '84', '104', '172', '96', '166', '125',
                 '170', '87', '116', '55', '86', '113', '107', '48', '160', '1', '165', '12', '121', '36', '69', '124',
                 '2', '8', '37', '26', '155', '156', '68', '3', '163', '117', '157', '49', '150', '73', '80', '34',
                 '128', '53', '148', '135']

DEFAULT_MAX_STEP_SIZE = 60 * 30
DEFAULT_MIN_STEP_SIZE = 300
DEFAULT_MAX_REC_RETURNED = 2000
URL = 'https://api.hh.ru/vacancies/'

class Worker:
    """
    Сlass with functions for splitting the time interval,getting vacancy
    IDs and getting detailed information about each vacancy
    """

    def __init__(
            self,
            date_from: date,
            date_to: date

    ):
        self.date_from = date_from
        self.date_to = date_to
        self.vacancy_ids_set = set()
        self.vacancy_queue = queue.Queue()
        self.time_intervals_list = []
        self.api = ApiClient(URL, ID_ROLES_LIST)
        self.db = DataBase()

    def process_time_interval(self, date_left: date, date_right: date) -> date:
        """
        Function checks the interval for the limit (2000) and, if necessary,
        splits it into smaller intervals
        :return: next date on which the step should be made
        """
        if date_right > self.date_from:
            date_right = self.date_from
        params = {
            'per_page': 100,
            'page': 0,
            'professional_role': ID_ROLES_LIST,
            'date_from': f'{date_left.isoformat()}',
            'date_to': f'{date_right.isoformat()}'}
        data = self.api.make_request(params=params)

        if data == None:
            return date_right

        if data['found'] <= DEFAULT_MAX_REC_RETURNED:
            self.time_intervals_list.append([date_left, date_right])
        else:
            while date_right != date_left:
                self.time_intervals_list.append([date_left, date_left + DEFAULT_MIN_STEP_SIZE])
                date_left += DEFAULT_MIN_STEP_SIZE
        return date_right

    def get_vacancy_ids(self, time_intervals_list: List[List[date]]) -> None:
        """
        A function that goes through each time interval from the list and adds all
        vacancy IDs to the vacancy_ids_set set.
        :param time_intervals_list: list of time intervals into which the original interval was divided
        """
        for time_interval in time_intervals_list:
            for page in range(20):
                params = {
                    'per_page': 100,
                    'page': page,
                    'professional_role': ID_ROLES_LIST,
                    'date_from': f'{time_interval[0].isoformat()}',
                    'date_to': f'{time_interval[1].isoformat()}'}

                data = self.api.make_request(params=params)
                if data == None:
                    break
                ids_set = self.parse_json_ids(data)
                self.vacancy_ids_set.update(ids_set)
                if (data['pages'] - page) <= 1:
                    break

    @staticmethod
    def parse_json_ids(vacancies_dict: Dict[str, Any]) -> Set[str]:
        """
        Selects all ids from the dict
        :return: set of vacancy ids in this dict
        """
        ids_set = set()
        for vacancy in vacancies_dict['items']:
            id = vacancy['id']
            ids_set.add(id)
        return ids_set

    def get_vacancy_body(self, vacancy_ids_set: Set[str]) -> None:
        """
        Gets the vacancy body by id
        """
        for id in vacancy_ids_set:
            vacancy_body = self.api.make_request(vacancy_id=id)
            if vacancy_body == None:
                continue
            self.vacancy_queue.put(vacancy_body)
            if self.vacancy_queue.qsize() == 500:
                self.add_vacancy_to_db(self.vacancy_queue)

    def add_vacancy_to_db(self, vacancy_queue) -> None:
        """
        Adds vacancy bodies to the database
        """
        self.db.connect()
        while not vacancy_queue.empty():
            vacancy_body = vacancy_queue.get()
            vacancy_id = vacancy_body['id']

            self.db.execute_query(vacancy_id=vacancy_id, vacancy_body=vacancy_body)
        self.db.close_connection()

    def run(self) -> None:
        """
        Function to start the parser
        """
        logger.info(f'Time interval: From {self.date_from} to {self.date_to}')

        while self.date_to != self.date_from:
            next_date = self.process_time_interval(self.date_to,
                                                   self.date_to + timedelta(seconds=DEFAULT_MAX_STEP_SIZE))
            self.date_to = next_date

        logger.info(f'The selection of intervals is finished')

        self.get_vacancy_ids(self.time_intervals_list)
        self.get_vacancy_body(self.vacancy_ids_set)
        self.add_vacancy_to_db(self.vacancy_queue)
        logger.info('Процесс закончил свою работу')

class ApiClient:
    """
    A class with functions for sending api requests
    """

    def __init__(self,
                 base_url: str,
                 id_roles: List[int] = None,
                 count_errors: int = 0,
                 max_count_errors: int = 5,
                 requests_send_count: int = 0):

        self.base_url = base_url
        self.id_roles = id_roles
        self.count_errors = count_errors
        self.max_count_errors = max_count_errors
        self.requests_send_count = requests_send_count

    def make_request(self,
                     vacancy_id: str = None,
                     params: Dict[str, Any] = None,
                     proxy: str = None,
                     timeout: int = 5,
                     retry: int = 2) -> Union[Dict[str, Any], str, None]:

        if vacancy_id:
            url = self.base_url + f'{vacancy_id}'
        else:
            url = self.base_url
        try:
            req = requests.get(url, params, proxies=proxy, timeout=timeout)
            req.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            self.count_errors += 1
            if self.count_errors > self.max_count_errors:
                self.count_errors = 0
                return None

            logger.info(f'ConnectionError!! {e} retry {retry} proxy {proxy}')
            time.sleep(15)

            return None

        except Exception as err:
            self.count_errors += 1
            if self.count_errors > self.max_count_errors:
                self.count_errors = 0
                return None

            time.sleep(8)
            if retry:
                logger.info(f'{err}. retry {retry} proxy {proxy}')
                return self.make_request(vacancy_id, params, proxy, retry=(retry - 1))
            else:
                return None

        else:
            self.count_errors = 0
            self.requests_send_count += 1
            data = req.content.decode()
            data = json.loads(data)
            if 'items' in data:
                if data['items'] == [] and retry:
                    data = self.make_request(vacancy_id, params, proxy, retry=(retry - 1))
                elif data['items'] == []:
                    return None
            return data
        finally:
            if req != None:
                req.close()
                if self.requests_send_count >= 10:
                    time.sleep(1)
                    self.requests_send_count = 0


def get_date(left, timedelta_in_seconds):
    """
    Used mostly for testing
    """
    now = datetime.now() - timedelta(seconds=left)
    date_to = (now - timedelta(minutes=now.minute % 5)).replace(second=0, microsecond=0)
    list_time = [date_to, date_to - timedelta(seconds=timedelta_in_seconds)]
    return list_time


def main() -> None:
    """
    Used mostly for testing; this module is not usually run standalone
    """
    li = get_date(0, 2000)
    pars = Worker(*li)
    pars.run()


if __name__ == '__main__':
    main()
