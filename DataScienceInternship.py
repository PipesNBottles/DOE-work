import re
import glob
from tika import parser
import sqlite3
from bs4 import BeautifulSoup
import urllib.request
import sys, os
from pathlib import Path
from datetime import datetime

class pdfDataObject:
    keys = ["analysis", "software", "embedded", "database", "spreadsheet", "firmware", "calculation", "programming", "test"]
    
    def __init__(self,link):
        self.data = {}
        self.path = Path(__file__).parent
        self.link = link
    
    def create_connectDB(self):
        path = Path(__file__).parent
        db = "DOE.db" #come up with better name for database later
        path = path.joinpath(db)
        path = str(path)
        conn = sqlite3.connect(path) 
        cursor = conn.cursor()
        try:
            cursor.execute(
            """
            --sql
            create table DOE_BASE(
                ID TEXT PRIMARY KEY,
                DATE TEXT,
                SITE TEXT,
                AUTHOR_1 TEXT,
                AUTHOR_2 TEXT,
                ANALYSIS INTEGER,
                SOFTWARE INTEGER,
                EMBEDDED INTEGER,
                DATABASE INTEGER,
                SPREADSHEET INTEGER,
                FIRMWARE INTEGER,
                CALCULATION INTEGER,
                PROGRAMMING INTEGER,
                TEST INTEGER
            );
            """
            )
            conn.commit()
            conn.close()
        except sqlite3.OperationalError as dbExists:
            print("Data base exists: ", dbExists)
        return path
    
    def insertValues(self,path):
        toople = (self.data["ID"],self.data["Date"],self.data["Site"],self.data["author1"],
                  self.data["author2"],self.data["analysis"],self.data["software"],
                  self.data["embedded"], self.data["database"],self.data["spreadsheet"],
                  self.data["firmware"],self.data["calculation"],self.data["programming"],
                  self.data["test"])
        conn = sqlite3.connect(path) 
        cursor = conn.cursor()
        cursor.execute("""
        --sql
        INSERT OR IGNORE INTO DOE_BASE 
        (ID, DATE, SITE, AUTHOR_1, AUTHOR_2, 
        ANALYSIS, SOFTWARE, EMBEDDED, DATABASE, SPREADSHEET, FIRMWARE, 
        CALCULATION, PROGRAMMING, TEST)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """, (toople))
        conn.commit()
        conn.close()
        
        
    
    def showInfo(self):
        print()
        for key, value in self.data.items():
            print(key, " -> ",value)
        print()
        print("###########################################")

    
    def build(self,allPDFs,dbpath):
        for pdf in allPDFs:
            split = urllib.parse.urlparse(pdf).path
            fileName = urllib.parse.unquote(split)
            fileName = fileName.split("/")[-1]
            path = self.path.joinpath(fileName)
            if not path.exists(): 
                urllib.request.urlretrieve(pdf,path)
            file = str(path)
            
            raw = parser.from_file(file)
            text = raw['content']
            if 'dc:title' in raw['metadata']:
                title = raw['metadata']['dc:title']
            else:
                pseudoTitle = fileName
                pseudoTitle = pseudoTitle.rstrip(".pdf")
                pseudoTitle = pseudoTitle.split(" ")
                pseudoTitle[-2] = pseudoTitle[-2]+ ","
                pseudoTitle = " ".join(pseudoTitle)
                title = pseudoTitle
            title = title.split(sep=" ",maxsplit=3)
            if "Week" in title[1]:
                self.data["Site"] = title[0]
                self.data["Date"] = title[3]
            else:
                self.data["Site"] = title[0] + " " + title[1]
                dateInfo = title[3].split(sep=" ", maxsplit=1)
                self.data["Date"] = dateInfo[1]
            if 'dateInfo' in locals():
                if "Ending" in dateInfo[1]:
                    dateInfo = dateInfo[1].split(sep=" ", maxsplit=1)
                dateobj = datetime.strptime(dateInfo[1].rstrip(),'%B %d, %Y')
            else:
                dateobj = datetime.strptime(title[3].rstrip(),'%B %d, %Y')
            self.data["ID"] = title[0][0] + str(dateobj.date())
            for key in self.keys:
                if text.find(key) != -1:
                    self.data[key] = 1
                else:
                    self.data[key] = 0
            string = re.findall(r"FROM+\W\s\D+$",text,re.MULTILINE)
            if not "Savannah" in self.data["Site"]: 
                initials = re.findall(r"[A-Z]\.\s[A-Z][a-z]*",string[0])
                fullName = re.findall(r"[A-Z][a-z]{6}\s[A-Z]\D\w+",string[0])
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",string[0])
                acting = re.findall(r"[A-za-z]+\s[A-Za-z]+\s",string[0])
            else:
                initials = re.findall(r"[A-Z]\.\s[A-Z]\w+",string[0])
                fullName = re.findall(r"[A-Z][a-z]{6}\s[A-Z]\D\w+",string[0])
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",string[0])
                acting = re.findall(r"[A-za-z]+\s[A-Za-z]+\s",string[0])
            if acting:
                self.data["author1"] = acting[0]
                if len(acting) > 1:
                    self.data["author2"] = acting[1]
                self.data["author2"] = ""
            if initials and namesWithInitials:
                if initials[0] in namesWithInitials[0]:
                    self.data["author1"] = namesWithInitials[0]
                if fullName:
                    self.data["author2"] = fullName[0]
                else:
                    self.data["author2"] = ""
            if initials and not 'author2' in self.data:
                self.data["author1"] = initials[0]
                self.data["author2"] = ""
            if len(fullName) > 1:
                self.data["author1"] = fullName[0]
                self.data["author2"] = fullName[1]
            elif len(initials) == 2:
                self.data["author1"] = initials[0]
                self.data["author2"] = initials[1]
            elif len(namesWithInitials) == 2:
                self.data["author1"] = namesWithInitials[0]
                self.data["author2"] = namesWithInitials[1]
            if not "author1" in self.data:
                if fullName:
                    self.data["author1"] = fullName[0]
                    self.data["author2"] = ""       
            self.showInfo()
            self.insertValues(dbpath)
            self.data.clear()
                
    
    def parseURL(self): #eliminate this, better idea later
        print("site URL is ", self.link)
        URL = self.link 
        page = urllib.request.urlopen(URL).read()
        parseDoc = BeautifulSoup(page, features="lxml")
        links = parseDoc.find_all(href=re.compile("reports"), limit=2)
        links = links[0]
        
        URL = URL +links["href"]
        page = urllib.request.urlopen(URL).read()
        parseDoc = BeautifulSoup(page, features="lxml")
        links = parseDoc.find(attrs={"class": "leaf first"})
        another = links.a
        URL = URL + another["href"]
        return URL
    
    
    def collectAll(self,pdfPage,allPDFs):
        response = urllib.request.urlopen(pdfPage).read()
        soup = BeautifulSoup(response,features="lxml")
        links = soup.find_all("a", href=re.compile(r'(\.pdf)'))
        for link in links:
            allPDFs.append(link["href"])

        links = soup.find("a", string=re.compile("next"))
        if links is None or len(allPDFs) >= 450:
            return allPDFs
        else:
            pdfPage = self.link + links.get("href")
            return self.collectAll(pdfPage,allPDFs)
        
    
def main():
    
    allPDFs = []
    pdfobj = pdfDataObject("https://www.dnfsb.gov/")
    db = pdfobj.create_connectDB()
    URL = pdfobj.parseURL()
    allPdfs = pdfobj.collectAll(URL,allPDFs)
    pdfobj.build(allPDFs,db)
   
    
if __name__ == "__main__":
    main()