# -*- coding: utf-8 -*-
from typing import List

import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import datetime
import csv
import pandas as pd

print('Привет, четко следуй инструкциям и все получится.\n'
      'Результатом выполнения программы станет файл в формате .csv с нужным названием, в котором содержатся данные статистики')
# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import sys
#------------------------------------вот этот кусок вообще не понятен-----------------------------------
if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x
#----------------------------------------------------------------------------------------------------------
# --- Входные данные ---
# Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

# OAuth-токен пользователя, от имени которого будут выполняться запросы
token = input('Введите агентский токен\n')

# Логин клиента рекламного агентства
# Обязательный параметр, если запросы выполняются от имени рекламного агентства
clientLogin = input('Введите логин клиента\n')

#запрос необходимых полей для отчета
fieldNamesDictionary = input('Введите необходимые для отчета поля через пробел используя словарь, после чего нажмите Enter\n'
                             'CampaignName	Название кампании\n'
                             'Cost	Расход\n'
                             'Clicks	Клики\n'
                             'AdFormat	Формат показа объявления\n'
                             'AdGroupId	Идентификатор группы объявлений\n'
                             'AdGroupName	Название группы объявлений\n'
                             'AdId	Идентификатор объявления\n'
                             'AdNetworkType	Тип площадки\n'
                             'Age	Возрастная группа пользователя\n'
                             'AvgClickPosition	Средняя позиция клика\n'
                             'AvgCpc	Средняя цена клика\n'
                             'AvgEffectiveBid	Средняя ставка за клик с учетом всех корректировок\n'
                             'AvgImpressionPosition	Средняя позиция показа\n'
                             'AvgPageviews	Средняя глубина просмотра\n'
                             'AvgTrafficVolume	Средний объем трафика\n'
                             'BounceRate	% отказа\n'
                             'Bounces	Количество отказов\n'
                             'CampaignId	ID кампании\n'
                            'CampaignUrlPath	URL-адрес, указанный в настройках рекламной кампании\n'
                            'CampaignType	Тип кампании\n'
                            'CarrierType	Тип связи\n'
                            'ClickType	По какой части объявления кликнул пользователь\n'
                            'ClientLogin	Логин клиента\n'
                            'ConversionRate	% конверсии\n'
                            'Conversions	Количество конверсий\n'
                            'CostPerConversion	Цена конверсии\n'
                            'Criteria	Название или текст условия показа\n'
                            'CriteriaId	Идентификатор условия показа\n'
                            'CriteriaType	Тип условия показа\n'
                            'Criterion	Название или текст условия показа\n'
                            'CriterionId	Идентификатор условия показа\n'
                            'CriterionType	Тип условия показа, заданного рекламодателем\n'
                            'Ctr	CTR %\n'
                            'Date	Дата\n'
                            'Device	Тип устройства\n'
                            'ExternalNetworkName	Наименование внешней сети\n'
                            'Gender	Пол\n'
                            'GoalsRoi	Рентабельность инвестиций в рекламу\n'
                            'Impressions	Показы\n'
                            'IncomeGrade	Уровень платежеспособности пользователя\n'
                            'LocationOfPresenceId	Идентификатор региона местонахождения пользователя\n'
                            'LocationOfPresenceName	Название региона местонахождения пользователя\n'
                            'MatchType	Тип соответствия ключевой фразе\n'
                            'MobilePlatform	Тип операционной системы\n'
                            'Month	Месяц\n'
                            'Placement	Название площадки показов\n'
                            'Profit	Прибыль\n'
                            'Quarter	Дата начала квартала\n'
                            'Revenue	Доход по целям\n'
                            'RlAdjustmentId	Идентификатор условия ретаргетинга и подбора аудитории\n'
                            'Sessions	Количество визитов\n'
                            'Slot	Блок показа объявлений\n'
                            'TargetingCategory	Категория таргетинга\n'
                            'TargetingLocationId	Идентификатор региона таргетинга\n'
                            'TargetingLocationName	Название региона таргетинга\n'
                            'Week	Дата начала недели\n' 
                            'WeightedCtr	Взвешенный CTR, в процентах\n'
                            'WeightedImpressions	Взвешенные показы\n'
                            'Year	Дата начала года\n'
                             )

#превращаю поля для отчета в список
fieldNamesDictionary = str.split(fieldNamesDictionary)

#print(type(fieldNamesDictionary))
#спрашиваю необходимые конверсии, если в полях были выбраны Conversions
if 'Conversions' in fieldNamesDictionary:
    conversions = input('Введите ID необходимых целей из Метрики через пробел. Не более 9 целей\n')
else:
    conversions = ''

#проверяю ввел ли пользователь идентификаторы целей из Метрики, если ввел, то формирую из них список
if conversions == '':
    print('Вы не выбрали цели, отчет будет сформирован без данных о целевых событиях')
else:
    print('Вы выбрали цели: ', conversions)
    conversions = str.split(conversions)
    conversions = conversions[:9]

requestDate = input('Введите необходимый период дат в одном из следующих форматов:\n'
                    'TODAY — текущий день\n'
                    'YESTERDAY — вчера\n'
                    'LAST_3_DAYS, LAST_5_DAYS, LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS, LAST_90_DAYS, LAST_365_DAYS — указанное количество предыдущих дней, не включая текущий день\n'
                    'THIS_WEEK_MON_TODAY — текущая неделя начиная с понедельника, включая текущий день\n'
                    'THIS_WEEK_SUN_TODAY — текущая неделя начиная с воскресенья, включая текущий день\n'
                    'LAST_WEEK — прошлая неделя с понедельника по воскресенье\n'
                    'THIS_MONTH — текущий календарный месяц\n'
                    'LAST_MONTH — полный предыдущий календарный месяц\n'
                    'ALL_TIME — вся доступная статистика, включая текущий день\n'
                    'CUSTOM_DATE — произвольный период. При выборе этого значения необходимо указать даты начала и окончания периода в параметрах в формате  YYYY-MM-DD\n')

#проверяю введеный период дат
if requestDate == 'CUSTOM_DATE':
    startDate = input('Введите дату начала периода в формате YYYY-MM-DD\n')
    endDate = input('Введите дату окончания периода в формате YYYY-MM-DD\n')
else:
    startDate = 0
    endDate = 0

# спрашиваю необходимую модель атрибуции, если пользователь указывал идентификаторы целей, если нет, просто прошу название отчета
if conversions != '':
    attribution = input('Введите необходимую модель атрибуции в формате:\n'
                    'FC — первый переход\n'
                    'LC — последний переход\n'
                    'LSC — последний значимый переход\n'
                    'LYDC — последний переход из Яндекс Директа\n')
    print('Вы выбрали модель атрибуции ', attribution)
    attribution = str.split(attribution)
    reportName = str(input('Введите название отчета\n'))
else:
    attribution = 0
    reportName = str(input('Введите название отчета\n'))


# --- Подготовка запроса ---
# Создание HTTP-заголовков запроса
headers = {
    # OAuth-токен. Использование слова Bearer обязательно
    "Authorization": "Bearer " + token,
    # Логин клиента рекламного агентства
    "Client-Login": clientLogin,
    # Язык ответных сообщений
    "Accept-Language": "ru",
    # Режим формирования отчета
    "processingMode": "auto",
    # Формат денежных значений в отчете
    "returnMoneyInMicros": "false",
    # Не выводить в отчете строку с названием отчета и диапазоном дат
    "skipReportHeader": "true",
    # Не выводить в отчете строку с названиями полей
    # "skipColumnHeader": "true",
    # Не выводить в отчете строку с количеством строк статистики
    "skipReportSummary": "true"
}

# Создание тела запроса
body = {
    "params": {
        "SelectionCriteria": {
        'DateFrom': startDate,
        'DateTo': endDate,
        },
        "Goals": conversions,
        "AttributionModels": attribution,
        "FieldNames": fieldNamesDictionary,
        "Page": {
                    "Limit": 10000000
                },
        "ReportName": u(reportName),
        "ReportType": "CUSTOM_REPORT",
        "DateRangeType": requestDate,
        "Format": "TSV",
        "IncludeVAT": "YES",
        "IncludeDiscount": "NO"
    }
}
# если не было указано идентификаторов конверсий, удаляю из запроса лишние параметры
if conversions == '':
    del (body['params']['Goals'])
    del (body['params']['AttributionModels'])

#если не указано CUSTOM_DATE, то удаляю даты начала и конца периода из отчета
if requestDate != 'CUSTOM_DATE':
    del (body['params']['SelectionCriteria']['DateFrom'])
    del (body['params']['SelectionCriteria']['DateTo'])

# Кодирование тела запроса в JSON
body = json.dumps(body, indent=4)

# --- Запуск цикла для выполнения запросов ---
# Если получен HTTP-код 200, то выводится содержание отчета
# Если получен HTTP-код 201 или 202, выполняются повторные запросы
while True:
    try:
        req = requests.post(ReportsURL, body, headers=headers)
        req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
        if req.status_code == 400:
            print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код запроса: {}".format(u(body)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
#--------------------------------------------вот тут я получаю текст---------------------------------------
        elif req.status_code == 200:
            print("Отчет создан успешно. Эффективной аналитики!")
            format(u(req.text))
            break
#------------------------------------------------------------------------------------------------------------
#----------------------------------------------------вот тут условия цикла, если сервер директа не отдает отчет сразу-------------------------
        elif req.status_code == 201:
            print("Отчет успешно поставлен в очередь в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 60))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            sleep(retryIn)
        elif req.status_code == 202:
            print("Отчет формируется в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 60))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            print("RequestId:  {}".format(req.headers.get("RequestId", False)))
            sleep(retryIn)
        elif req.status_code == 500:
            print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
        elif req.status_code == 502:
            print("Время формирования отчета превысило серверное ограничение.")
            print(
                "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
            print("JSON-код запроса: {}".format(body))
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
        else:
            print("Произошла непредвиденная ошибка")
            print("RequestId:  {}".format(req.headers.get("RequestId", False)))
            print("JSON-код запроса: {}".format(body))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break

    # Обработка ошибки, если не удалось соединиться с сервером API Директа
    except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API")
        # Принудительный выход из цикла
        break

    # Если возникла какая-либо другая ошибка
    except:
        # В данном случае мы рекомендуем проанализировать действия приложения
        print("Произошла непредвиденная ошибка")
        # Принудительный выход из цикла
        break

#----------------------------------- разбираю получившиеся данные в строки
raw_text: list[str] = req.text.split('\n')[:-1]
#------------------------------------- собираю датафрейм
ready_table = pd.DataFrame({'data':raw_text[1:]})['data'].astype(str).str.split('\t',expand=True)
ready_table.columns = raw_text[0].split('\t')


file = open(""+reportName+".csv", "w", encoding='utf_8')
file.write(req.text)
file.close()

#-----------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------собираю и принтую ДФ из csv------------------------------------------------------
yddf = pd.read_csv(""+reportName+".csv", sep='\t')


input('Работа программы выполнена, для завершения нажмите Enter')
