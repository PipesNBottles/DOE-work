import re
import glob
from tika import parser
import mysql.connector
from bs4 import BeautifulSoup
from urllib.request import urlopen 
import os 


class pdfDataObject:
    keys = ["analysis", "software", "embedded", "database", "spreadsheet", "firmware", "calculation", "programming", "test"]
    
    def __init__(self,path):
        self.data = {}
        self.path = path
    
    def showInfo(self):
        for key, value in self.data.items():
            print(key, " -> ",value)

    
    def build(self):
        files = glob.glob(self.path)
        if len(files) > 1: 
            files.sort()
        for file in files:
            raw = parser.from_file(file)
            text = raw['content']
            title = raw['metadata']['dc:title']
            if "Savannah" in title:
                title = title.split(sep=" ",maxsplit=3)
                self.data["Site"] = title[0] + " " + title[1]
                self.data["Date"] = title[3]
            else:
                title = title.split(sep=" ",maxsplit=3)
                self.data["Site"] = title[0]
                self.data["Date"] = title[3]
            for key in self.keys:
                if text.find(key) != -1:
                    self.data[key] = True
                else:
                    self.data[key] = False
            if not "Savannah" in self.data["Site"]:
                initials = re.findall(r"[A-Z]\.\s[A-Z][a-z]*",text,re.MULTILINE)
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",text,re.MULTILINE)
            else:
                initials = re.findall(r"[A-Z]\.\s[A-Z]\w+",text,re.MULTILINE)
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",text,re.MULTILINE)
            if len(namesWithInitials) < 2:
                if len(initials) >= 2:
                    self.data["author1"] = initials[1]
                    self.data["author2"] = initials[2]
                else:
                    self.data["author1"] = initials[1]
                    self.data["author2"] = ""
            else:
                namesWithInitials.pop(0)
                initials.pop(0)
                for i in range(len(namesWithInitials)):
                    for j in range(len(initials)):
                        if initials[j] in namesWithInitials[i]:
                            self.data["author1"] = namesWithInitials[i]
                self.data["author2"] = ""
    
    def parseURL(self, doesite):
        print("site URL is https://www.dnfsb.gov/")
        URL = doesite 
        page = urlopen(URL)
        parseDoc = BeautifulSoup(page, features="lxml")
        page.close()
        links = parseDoc.find_all(href=re.compile("reports"), limit=2)
        links = links[0]
        
        URL = URL +links["href"]
        page = urlopen(URL)
        parseDoc = BeautifulSoup(page, features="lxml")
        page.close()
        links = parseDoc.find(attrs={"class": "leaf first"})
        another = links.a
        URL = URL + another["href"]
        return URL
    
