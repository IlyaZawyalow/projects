import time
from datetime import datetime, timedelta
from loguru import logger
import queue
import json
from fake_useragent import UserAgent
import requests
from typing import Dict, List, Set

#
# def parseJsonIds(vacanciesJson: Dict) -> Set[int]:
#     """
#     Функция, распарсивающая JSON и выдающая множество ids
#     :param vacanciesJson: список интервалов времени на которые был разбит изначальный интервал
#     :retur: множество ids вакансий в данном json
#     """
#     idsSet = set()
#     for vacancy in vacanciesJson['items']:
#         id = vacancy['id']
#         idsSet.add(id)
#     return idsSet
#
# URL = 'https://api.hh.ru/vacancies/95311347'
# req = requests.get(URL, params=None)
# data = req.content.decode()
# data = json.loads(data)
# print(data)
# # print(parseJsonIds(data))
from datetime import datetime, timedelta
vacancyQueue = queue.Queue()
vacancyQueue.put(1)
print(vacancyQueue.qsize())