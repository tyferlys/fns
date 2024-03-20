import csv
import datetime
import json

import openpyxl
import requests
from bs4 import BeautifulSoup
from loguru import logger

from db.requestsToDB import DbRequests


class Source27():
    def __init__(self):
        logger.info("Инициализация класса источника 27")
        response = requests.get("https://www.nalog.gov.ru/opendata/7707329152-massfounders/")
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        href = soup.find_all("tr")[8].find("a")["href"]
        self.data = []
        response = requests.get(href)
        with open('./parsers/Source27/source27.csv', 'wb') as file:
            file.write(response.content)

        self.rows = None

        with open('./parsers/Source27/source27.csv', "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            self.rows = list(reader)

        self.dbRequests = DbRequests()

    def start_parse(self):
        logger.info("Старт парсинга")
        for row in self.rows:
            data = row['G1;G2;G3;G4;G5'].split(";")

            newPerson = dict()
            newPerson["inn"] = data[0]
            newPerson["last_name"] = data[1][0].upper() + data[1][1:].lower()
            newPerson["first_name"] = data[2][0].upper() + data[2][1:].lower()
            try:
                newPerson["middle_name"] = data[3][0].upper() + data[3][1:].lower()
            except:
                newPerson["middle_name"] = None
            newPerson["count"] = data[4]
            newPerson["is_relevance"] = True
            newPerson["date_check"] = datetime.datetime.now().strftime("%d-%m-%Y")
            newPerson["link_resource"] = "https://www.nalog.gov.ru/opendata/7707329152-massfounders/"
            newPerson["name_resource"] = "ФНС. Сведения о физических лицах, являющихся учредителями (участниками) нескольких юридических лиц"

            self.data.append(newPerson)

        for person in self.data:
            self.dbRequests.createRecord(person)