import re
import glob
from tika import parser
import mysql.connector
from bs4 import BeautifulSoup
import urllib.request
import sys, os
from pathlib import Path


class pdfDataObject:
    keys = ["analysis", "software", "embedded", "database", "spreadsheet", "firmware", "calculation", "programming", "test"]
    
    def __init__(self,link):
        self.data = {}
        self.path = Path(__file__).parent
        self.link = link
    
    def showInfo(self):
        for key, value in self.data.items():
            print(key, " -> ",value)
        print()
        print("###########################################")

    
    def build(self,allPDFs):
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
            title = raw['metadata']['dc:title']
            title = title.split(sep=" ",maxsplit=3)
            if "Week" in title[1]:
                self.data["Site"] = title[0]
                self.data["Date"] = title[3]
            else:
                self.data["Site"] = title[0] + " " + title[1]
                self.data["Date"] = title[3]
            for key in self.keys:
                if text.find(key) != -1:
                    self.data[key] = True
                else:
                    self.data[key] = False
            if not "Savannah" in self.data["Site"]: 
                initials = re.findall(r"[A-Z]\.\s[A-Z][a-z]*",text,re.MULTILINE)
                fullName = re.findall(r"[A-Z][a-z]{6}\s[A-Z]\D\w+",text,re.MULTILINE)
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",text,re.MULTILINE)
            else:
                initials = re.findall(r"[A-Z]\.\s[A-Z]\w+",text,re.MULTILINE)
                fullName = re.findall(r"[A-Z][a-z]{6}\s[A-Z]\D\w+",text,re.MULTILINE)
                namesWithInitials = re.findall(r"[A-Z][a-z]+\s.\.\B\s\w*",text,re.MULTILINE)
            if fullName:
                self.data["author1"] = fullName[0]
                if len(fullName) > 1:
                    self.data["author2"] = fullName[1]
            if len(namesWithInitials) <= 2:
                if len(initials) > 2:
                    self.data["author1"] = initials[1]
                    self.data["author2"] = initials[2]
                elif len(initials) != 1: 
                    self.data["author1"] = initials[1]
                    if fullName:
                        self.data["author2"] = fullName[0]
                    else:
                        self.data["author2"] = ""
            else:
                namesWithInitials.pop(0)
                initials.pop(0)
                for i in range(len(namesWithInitials)):
                    for j in range(len(initials)):
                        if initials[j] in namesWithInitials[i]:
                            self.data["author1"] = namesWithInitials[i]
                if not "author2" in self.data and fullName:
                    self.data["author2"] = fullName[0]
                else:
                    self.data["author2"] = ""
            self.showInfo()
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
        links = soup.find_all("a", href=re.compile(r'(.pdf)'))
        for link in links:
            allPDFs.append(link["href"])

        links = soup.find("a", string=re.compile("next"))
        if links is None or len(allPDFs) >= 10:
            return allPDFs
        else:
            pdfPage = self.link + links.get("href")
            return self.collectAll(pdfPage,allPDFs)
        
    
def main():
    allPDFs = []
    pdfobj = pdfDataObject("https://www.dnfsb.gov/")
    URL = pdfobj.parseURL()
    allPdfs = pdfobj.collectAll(URL,allPDFs)
    pdfobj.build(allPDFs)
    

if __name__ == "__main__":
    main()