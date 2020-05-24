#creates the dbObject and then the rest of the relational database structure based on site location
#can be joined using the IDs, authors or other values
import sqlite3

class dbObject:
    def __init__(self,db):
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        cursor.execute("select distinct site from doe_base;")
        uniqueSites = cursor.fetchall()
        for site in uniqueSites:
            siteName = site[0]
            siteName = siteName.split(" ")
            siteName = "_".join(siteName)
            sql = "create table if not exists " +  siteName + " as "
            sql += """SELECT ID, date, Site, author_1, author_2, ANALYSIS, 
                    SOFTWARE, EMBEDDED, DATABASE, SPREADSHEET, FIRMWARE,
                    CALCULATION, PROGRAMMING, TEST FROM DOE_BASE 
                    where site = ?;"""
            cursor.execute(sql, site)
        cursor.close()

    
    def splitDataBySite(self):
        #work in progress
        #change to lambda
        print("new object")
    
    def splitDataByAuthor(self):
        #work in progress
        #change to lambda
        print("new object")
    
    def splitDataByTerm(self):
        #work in progress
        #change to lambda
        print("new object")