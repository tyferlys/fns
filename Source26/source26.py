import csv
import datetime
import json

import requests
from bs4 import BeautifulSoup


class Source26():
    def __init__(self):
        response = requests.get("https://www.nalog.gov.ru/opendata/7707329152-massleaders/")
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        href = soup.find_all("tr")[8].find("a")["href"]

        response = requests.get(href)
        with open('./parsers/Source26/source26.csv', 'wb') as file:
            file.write(response.content)

        self.rows = None

        with open('./parsers/Source26/source26.csv', "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            self.rows = list(reader)

    def start_parse(self):
        newData = []

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
            newPerson["link_resource"] = "https://www.nalog.gov.ru/opendata/7707329152-massleaders/"
            newPerson["name_resource"] = "ФНС. Сведения о физических лицах, являющихся руководителями нескольких юридических лиц"

            newData.append(newPerson)

        with open('./parsers/Source26/source26.json', "r+", encoding="utf-8") as file:
            oldData = json.load(file)

            for oldPerson in oldData:
                isExistArray = filter(lambda newPerson: newPerson["inn"] == oldPerson["inn"] and newPerson["last_name"] == oldPerson["last_name"] and newPerson["first_name"] == oldPerson["first_name"] and newPerson["middle_name"] == oldPerson["middle_name"], newData)

                if len(list(isExistArray)) == 0:
                    oldPerson["is_relevance"] = False
                else:
                    oldPerson["is_relevance"] = True

                oldPerson["date_check"] = datetime.datetime.now().strftime("%d-%m-%Y")

            for newPerson in newData:
                isExistArray = filter(lambda oldPerson: newPerson["inn"] == oldPerson["inn"] and newPerson["last_name"] == oldPerson[ "last_name"] and newPerson["first_name"] == oldPerson["first_name"] and newPerson["middle_name"] == oldPerson["middle_name"], oldData)

                if len(list(isExistArray)) == 0:
                    oldData.append(newPerson)

            file.truncate(0)
            file.seek(0)
            json.dump(oldData, file, ensure_ascii=False, indent=2)