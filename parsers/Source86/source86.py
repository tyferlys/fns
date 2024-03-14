import csv
import datetime
import json
import os

import openpyxl
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


class Source86:
    def __init__(self):
        self.roots = []
        self.currentJson = []

        files = os.listdir("./parsers/Source86/xmlData")

        for file in files:
            tree = ET.parse(f'./parsers/Source86/xmlData/{file}')
            root = tree.getroot()
            self.roots.append(root)

    def start_parse(self):
        indexRoot = 1
        for root in self.roots:
            print(f"root - {indexRoot}")
            indexRoot += 1
            for child in root:
                if child.tag == 'Документ':
                    dataDoc = dict()
                    for childDoc in child:
                        if childDoc.tag == "СведНП":
                            dataDoc["name"] = childDoc.attrib["НаимОрг"]
                            dataDoc["inn"] = childDoc.attrib["ИННЮЛ"]
                        elif childDoc.tag == "СведССЧР":
                            dataDoc["count"] = childDoc.attrib["КолРаб"]
                    self.currentJson.append(dataDoc)

        with open("./parsers/Source86/source86.json", "r+", encoding="utf-8") as file:
            file.truncate(0)
            file.seek(0)
            json.dump(self.currentJson, file, ensure_ascii=False, indent=2)