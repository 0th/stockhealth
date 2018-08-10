

import sqlite3



class db_control:

    def __init__(self):
        pass



    def create(self,dir,table,column):

        con = sqlite3.connect(dir)
        cur = con.cursor()
        sql = " Create Table if not exists " + table + column
        cur.execute(sql)



    def viewDBdata(self, dir, table, condStr, condVal):


        conn = sqlite3.connect(dir)
        cur = conn.cursor()
        query = "select * from {0} where {1} {2}".format(table, condStr, condVal)
        cur.execute(query)
        rows = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        cur.close()
        return rows



    def viewDBdata_all(self, db, table):
        conn = sqlite3.connect(db)
        cur = conn.cursor()
        query = "select * from {0}".format(table)
        cur.execute(query)
        rows = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        cur.close()
        return rows


    def viewDBdata_Close(self,dir_db, column, table,date,limit):
        conn = sqlite3.connect(dir_db)
        cur = conn.cursor()
        query = "select distinct {0} from {1} order by {2} desc limit {3}".format(column, table, date, limit)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows


    def viewDBdata_Day(self,dir_db, column, table,date,limit):
        conn = sqlite3.connect(dir_db)
        cur = conn.cursor()
        query = "select distinct {0} from {1} order by {2} desc limit {3}".format(column, table, date, limit)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows



    def viewDBdata_stockCode(self,db,table):

        conn = sqlite3.connect(db)
        cur = conn.cursor()
        query = "select field2, field1 from "+table
        cur.execute(query)
        rows = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        cur.close()
        return rows



    def viewDBdata_selectedColumn(self,db, column, table):
        conn = sqlite3.connect(db)
        cur = conn.cursor()
        query = "select {0} from {1}".format(column, table)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows


    def viewDBdata_recentDay(self, db, table, date):

        conn = sqlite3.connect(db)
        cur = conn.cursor()
        query = "select {1} from {0} order by {1} desc limit 1".format(table, date)
        cur.execute(query)
        rows = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        cur.close()
        return rows







    def insertOrReplaceDB(self, dir_db, table, column, length, data):

        conn = sqlite3.connect(dir_db)
        cur = conn.cursor()
        sql = "insert or replace into " + table + column
        if length == 1:
            cur.execute(sql, data)
        elif length > 1:
            cur.executemany(sql, data)

        conn.commit()
        conn.close()


    def checkDBtable(self,dir,table):

        conn = sqlite3.connect(dir)
        cur = conn.cursor()
        query =  "SELECT count(*) from sqlite_master WHERE name =" + table
        cur.execute(query)
        value = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        conn.close()
        return value




    def delDBtable(self, db,table):

        conn = sqlite3.connect(db)
        cur = conn.cursor()
        sql = "Drop Table If Exists "+table
        cur.execute(sql)
        conn.commit()  # 트랜젝션의 내용을 DB에 반영함
        conn.close()





    def existDBdata(self, dir_db,table):

        conn = sqlite3.connect(dir_db)
        cur = conn.cursor()
        query = "select count(*) from " + table
        cur.execute(query)
        rows = cur.fetchall()  # 모든 데이터를 한번에 클라이언트로 가져옴
        cur.close()

        return rows

