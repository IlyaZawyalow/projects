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


class VacancyParser:
    """
    Осуществляет запрос к API Headhunter для получения списка идентификаторов (ids) вакансий в
    каждом отдельном интервале и получает подробную информацию о каждой вакансии
    """

    def __init__(
            self,
            dateFrom: date,
            dateTo: date
    ):
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.vacancyIdsSet = set()
        self.vacancyQueue = queue.Queue()
        self.timeIntervalsList = []
        self.api = ApiClient(URL, ID_ROLES_LIST)
        self.db = DataBase(dbname='mydb', username='postgres', host='localhost', password='2280')

    def run(self):
        logger.info(f'Временной интервал: От {self.dateFrom} --> до --> {self.dateTo}')

        while self.dateTo != self.dateFrom:
            nextDate = self.processTimeInterval(self.dateTo, self.dateTo + timedelta(seconds=DEFAULT_MAX_STEP_SIZE))
            self.dateTo = nextDate
        print(self.timeIntervalsList)
        self.getVacancyIds(self.timeIntervalsList)
        print(self.vacancyIdsSet)
        self.getVacancyBody(self.vacancyIdsSet)
        self.addVacancyToDB(self.vacancyQueue)
        logger.info('Процесс закончил свою работу')

    def processTimeInterval(self, dateLeft, dateRight):
        if dateRight > self.dateFrom:
            dateRight = self.dateFrom
        params = {
            'per_page': 100,
            'page': 0,
            'professional_role': ID_ROLES_LIST,
            'date_from': f'{dateLeft.isoformat()}',
            'date_to': f'{dateRight.isoformat()}'}

        data = self.api.makeRequest(params=params)
        if data == None:
            return dateRight
        if data['found'] <= DEFAULT_MAX_REC_RETURNED:
            self.timeIntervalsList.append([dateLeft, dateRight])

        else:
            while dateRight != dateLeft:
                self.timeIntervalsList.append([dateLeft, dateLeft + DEFAULT_MIN_STEP_SIZE])
                dateLeft += DEFAULT_MIN_STEP_SIZE
        return dateRight

    def getVacancyIds(self, timeIntervalsList: List):
        """
        Функция, проходящая по каждому интервалу времени из списка и добавляющая все идентификаторы вакансий в множество vacancyIdsSet.

        :param timeIntervalsList: список интервалов времени на которые был разбит изначальный интервал
        :retur
        """
        for timeInterval in timeIntervalsList:

            for page in range(20):
                params = {
                    'per_page': 100,
                    'page': page,
                    'professional_role': ID_ROLES_LIST,
                    'date_from': f'{timeInterval[0].isoformat()}',
                    'date_to': f'{timeInterval[1].isoformat()}'}

                data = self.api.makeRequest(params=params)
                if data == None:
                    break
                idsSet = self.parseJsonIds(data)
                self.vacancyIdsSet.update(idsSet)
                if (data['pages'] - page) <= 1:
                    break

    @staticmethod
    def parseJsonIds(vacanciesJson: Dict) -> Set[int]:
        """
        Функция, распарсивающая JSON и выдающая множество ids
        :param vacanciesJson: список интервалов времени на которые был разбит изначальный интервал
        :retur: множество ids вакансий в данном json
        """
        idsSet = set()
        for vacancy in vacanciesJson['items']:
            id = vacancy['id']
            idsSet.add(id)
        return idsSet

    def getVacancyBody(self, vacancyIdsSet):
        for id in vacancyIdsSet:

            vacancyBody = self.api.makeRequest(vacancyId=int(id))

            if vacancyBody == None:
                continue
            self.vacancyQueue.put(vacancyBody)
            if self.vacancyQueue.qsize() == 500:
                self.addVacancyToDB(self.vacancyQueue)

    def addVacancyToDB(self, vacancyQueue):
        self.db.connect(tableName='vacancies')
        while not vacancyQueue.empty():
            vacancyBody = vacancyQueue.get()
            vacancyId = vacancyBody['id']

            self.db.executeQuery(tableName='vacancies', vacancyId=vacancyId, vacancyBody=vacancyBody)
        self.db.closeConnection()


class ApiClient:

    def __init__(self,
                 base_url: str,
                 idRoles: List[int] = None,
                 countErrors: int = 0,
                 maxCountErrors: int = 5,
                 count: int = 0):
        self.base_url = base_url
        self.idRoles = idRoles
        self.countErrors = countErrors
        self.maxCountErrors = maxCountErrors
        self.count = count

    def makeRequest(self,
                    vacancyId: int = None,
                    params=None,
                    proxy: str = None,
                    timeout: int = 5,
                    retry: int = 2):

        if vacancyId:
            url = self.base_url + f'{vacancyId}'
        else:
            url = self.base_url
        try:
            req = requests.get(url, params, proxies=proxy, timeout=timeout)
            req.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            self.countErrors += 1
            if self.countErrors > self.maxCountErrors:
                self.countErrors = 0
                return None

            logger.info(f'{e} retry {retry} proxy {proxy}')
            time.sleep(15)

            return 'change the proxy'

        except Exception as err:
            self.countErrors += 1
            if self.countErrors > self.maxCountErrors:
                self.countErrors = 0
                return None

            time.sleep(4)
            if retry:
                logger.info(f'{err}. retry {retry} proxy {proxy}')
                return self.makeRequest(vacancyId, params, proxy, retry=(retry - 1))
            else:
                return 'change the proxy'

        else:
            self.countErrors = 0
            self.count += 1
            data = req.content.decode()
            data = json.loads(data)

            if 'items' in data:
                if data['items'] == [] and retry:
                    data = self.makeRequest(vacancyId, params, proxy, retry=(retry - 1))
                elif data['items'] == []:
                    return None
            return data
        finally:
            if req != None:
                req.close()
                if self.count >= 10:
                    time.sleep(1)
                    self.count = 0


def get_date(left, timedelta_in_seconds):
    now = datetime.now() - timedelta(seconds=left)
    date_to = (now - timedelta(minutes=now.minute % 5)).replace(second=0, microsecond=0)
    list_time = [date_to, date_to - timedelta(seconds=timedelta_in_seconds)]
    return list_time


if __name__ == '__main__':
    li = get_date(0, 5000)
    pars = VacancyParser(*li)
    pars.run()
