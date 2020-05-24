import re
import glob
from tika import parser
import sqlite3
from bs4 import BeautifulSoup
import urllib.request
import sys, os
from pathlib import Path
from datetime import datetime
import scipy
import pandas
import requests
from data_class import dbObject

#pdfDataObject initializes a database and creates the table at the top of the tree structure
#will also perform the http requests for the dnfsb.gov site and download all pdfs
#after an array of pdf links are created, the class will parse the data and insert into the table
class pdfDataObject:
    keys = ["analysis", "software", "embedded", "database", "spreadsheet", "firmware", "calculation", "programming", "test"]
    
    def __init__(self,link):
        self.data = {}
        self.path = Path(__file__).parent
        self.link = link
    
    def create_connectDB(self):
        #will create the database and table DOE_BASE upon initialization if said table doesn't exist already
        #database file is stored in directory of the script
        path = Path(__file__).parent
        db = "DOE.db" #come up with better name for database later
        path = path.joinpath(db)
        path = str(path)
        conn = sqlite3.connect(path) 
        cursor = conn.cursor()
        print("attempting to create database")
        cursor.execute(
        """
        --sql
        create table if not exists DOE_BASE(
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
        return path
    
    def insertValues(self,path):
        #will insert values based on the primary key (ID) of the entry 
        #if the ID already exist then the database will promptly ignore the current entry
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
        #grabs the link of each pdf from the site in memory and downloads if doesn't already exist
        for pdf in allPDFs:
            split = urllib.parse.urlparse(pdf).path
            fileName = urllib.parse.unquote(split)
            fileName = fileName.split("/")[-1]
            path = self.path.joinpath(fileName)
            if not path.exists(): 
                urllib.request.urlretrieve(pdf,path)
            file = str(path)
            
            #due to the fact all pdfs are in the same* format (varying degrees)
            #simply use regex to find the site and date of when the report was written
            #and perform string manipulation to clean it for database entry
            #uses the datetime library to parse the date and generate the ID
            raw = parser.from_file(file)
            text = raw['content']
            siteString = re.findall(r"SUBJECT:[A-Za-z\s]+Ac",text,re.MULTILINE)
            dateString = re.findall(r"[A-Z][a-z]+\s\d{1,2}\,?\s\d+",text,re.MULTILINE)

            if not siteString:
                siteString = re.findall(r"SUBJECT:\s?[A-Z][a-z]+\s[A-Z][a-z]+",text,re.MULTILINE)
                siteString = siteString[0].split(" ")  
            else:
                siteString = siteString[0].split(" ")
                siteString.pop()
            
            siteString.pop(0)

            siteString = " ".join(siteString)
            siteString = siteString.lstrip()

            dateString = dateString[0].split(" ")
            if not ',' in dateString[1]:
                dateString[1] = dateString[1] + ","
            dateString = " ".join(dateString)

            self.data["Site"] = siteString
            self.data["Date"] = dateString
            dateobj = datetime.strptime(dateString.rstrip(),'%B %d, %Y')
            self.data["ID"] = siteString[0] + str(dateobj.date())
            
            #searches the pdf for the apropriate terms to determine if there's a software
            #related QA issue at the DOE site
            for key in self.keys:
                if text.find(key) != -1:
                    self.data[key] = 1
                else:
                    self.data[key] = 0
            
            #same logic as above but to find the authors names
            #unfortunately names can be written any number of ways and 
            #searching for them all can be costly, thus the least likely
            #names/scenarios are checked first and over written accordingly
            string = re.findall(r"FROM+\W\s\D+$",text,re.MULTILINE)
            
            if not "Savannah" in self.data["Site"]: 
                initials = re.findall(r"[A-Z]\.\s[A-Z][a-z]*",string[0])
            else:
                initials = re.findall(r"[A-Z]\.\s[A-Z]\w+",string[0])
            
            fullName = re.findall(r"[A-Z][a-z]+\s[A-Z]\D\w+",string[0])
            namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",string[0])
            acting = re.findall(r"[A-z][a-z]+\s[A-Z][a-z]+\s\(",string[0])
            
            if fullName:
                fullName.pop()
            
            if acting:
                acting[0] = acting[0].rstrip("(")
                self.data["author1"] = acting[0]
                if len(acting) > 1:
                    self.data["author2"] = acting[1]
                else:
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
                
    
    #utilizing requests and beautiful soup we can parse the HTTP address and search
    #for pdfs within a desired time range and keywords
    def parseURL(self, keyword, start_date, end_date):
        print("site URL is ", self.link)
        URL = self.link 
        page = urllib.request.urlopen(URL).read()
        parseDoc = BeautifulSoup(page, features="lxml")
        links = parseDoc.find_all(href=re.compile("reports"), limit=2)
        links = links[0]       
        URL = URL +links["href"]
        search_terms = {"search_api_views_fulltext": keyword, 
                        "field_date_value": start_date, 
                        "field_date_value2": end_date}
        
        with requests.get(URL, params=search_terms) as requestObj: 
            URL = requestObj.url
            page = urllib.request.urlopen(URL).read()
            parseDoc = BeautifulSoup(page,features="lxml")
            link = parseDoc.find(attrs={"class": "leaf first"})
            another = link.a
            URL = self.link + another["href"]
        return URL
    
    #same as previous function but prepares the HTTP address to collect all pdfs from 
    #the current date to whenever the first the report was uploaded
    def parseURLAll(self):
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
    
    #loop is O(n) as no page is longer than 10 pdf links long
    #will constantly manipulate the address until Beautiful Soup fails
    def collectAll(self,pdfPage,allPDFs):
        response = urllib.request.urlopen(pdfPage).read()
        soup = BeautifulSoup(response,features="lxml")
        next_page = soup.find("a", title="Go to next page")
        while next_page != None:
            pdfPage = self.link + next_page.get("href")
            response = urllib.request.urlopen(pdfPage).read()
            soup = BeautifulSoup(response,features="lxml")
            links = soup.find_all("a", href=re.compile(r'(\.pdf)'))
            next_page = soup.find("a", title="Go to next page")
            for link in links:
                allPDFs.append(link["href"])
        return allPDFs

    
def main():
    if len(sys.argv) == 4:
        keyword, start_date, end_date = sys.argv[1], sys.argv[2], sys.argv[3]
    elif len(sys.argv) == 3:
        keyword, start_date, end_date = "", sys.argv[1], sys.argv[2]
    elif len(sys.argv) == 1:
        print("will scrap and download everything, warning will take a while!")
        all_docs = True
    else:
        print("error, syntax is keyword to search for, start_date, end_date or simpley start and end")
        exit()
    
    allPDFs = []
    pdfobj = pdfDataObject("https://www.dnfsb.gov/")
    db = pdfobj.create_connectDB()
    if not 'all_docs' in locals():
        URL = pdfobj.parseURL(keyword,start_date,end_date)
    else:
        URL = pdfobj.parseURLAll()
    allPdfs = pdfobj.collectAll(URL,allPDFs)
    pdfobj.build(allPDFs,db)
    DatO =  dbObject(db)
    
if __name__ == "__main__":
    main()