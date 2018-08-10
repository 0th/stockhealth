

import math
import numpy
import sqlite3
import sys
# import pickle
import re
import pandas as pd
import datetime
import DB_Manager
import AL_Manager
import FB_Manager


from pandas_datareader import data
from pandas import Series, DataFrame
import fix_yahoo_finance as yf
yf.pdr_override()
numpy.seterr(divide='ignore', invalid='ignore')




# 정리

# 1. 코스피, 코스닥 종목 코드 데이터는 가지고 있음(갱신없음)
# 2. 네이버로 데이터 가져오기
# 3. 로직 처리
# 4. 파이어베이스에 데이터 보내기
# 5. 1일 1회 오토마타로 진행


# 로직처리

# 1. 일변동 = 오늘 종가 / 어제 종가
# 2. 평균m =  일변동의 평균 - 1
# 3. 변동성s =  일변동의 표준편차
# 4. 평가지표G = m - s 제곱 / 2


# =============================================

# 1. 코스피, 코스닥 종목 가져오기
# 2. 야후DB 초기화 및 종목 시세 가져오기
# 2-1 비어있는 데이터 체크
# 2-2 비어있는 데이터 다시 가져오기
#
# 3. 야후DB에서 건강DB로 복사
# - 건강DB는 오래된 파일 포함 1 year 데이터
# - 야후DB는 최신 파일 위주로 가져오기 10 day
#
# 4. 건강DB에서 영업일(50일)만 추려서 가져오기
# 5. 로직 스타트
# 6. 결과DB에 저장

# =============================================


dir_yahoo_ks = './db/yahoo_stock_ks.db'
dir_yahoo_kq = './db/yahoo_stock_kq.db'

dir_result = './db/RESULT.db'



dir_kospi = './db/kospi_code.db'
dir_kosdaq = './db/kosdaq_code.db'
dir_naver_ks=  './db/naver_ks.db'
dir_naver_kq=  './db/naver_kq.db'


dir_db_ks = './db/kospi.db'
dir_db_kq = './db/kosdaq.db'


con_yahoo_ks = sqlite3.connect("./db/yahoo_stock_ks.db")
con_yahoo_kq = sqlite3.connect("./db/yahoo_stock_kq.db")
# con_kospi = sqlite3.connect("./db/kospi_db.db")
# con_kosdaq = sqlite3.connect("./db/kosdaq_db.db")

con_naver_ks = sqlite3.connect("./db/naver_ks.db")
con_naver_kq = sqlite3.connect("./db/naver_kq.db")


table_kospi = 'KOSPI'
table_kosdaq = 'KOSDAQ'


limit_day = 50


class MainStock:

    list_ks = []
    list_kq = []
    # list_result_ks = []
    # list_result_ks_all = []

    result_ks = []
    result_kq = []

    # list_result_kq = []
    # list_result_kq_all = []

    pre_day = -100
    check_retry = 2
    check_replay = 0

    instance_DB = DB_Manager.db_control()

    now = datetime.datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    pre = now + datetime.timedelta(days=pre_day)
    start_date = pre.strftime('%Y-%m-%d')
    data_source = 'yahoo'
    repalce_positions = '(Date, Open, High, Low, Close,`Adj Close`, Volume )'

    list_Com_ks = []
    list_Com_kq = []
    list_noneCom_ks = []
    list_noneCom_kq = []

    code_ks_re = []
    code_kq_re = []

    count = 0



    # 0. 시작
    def __init__(self):
        pass

    # 1. 코스피, 코스닥 종목 가져오기
    def get_stockCode(self,db, table):
        rows = DB_Manager.db_control().viewDBdata_stockCode(db,table)
        print(rows)
        return rows

    # 2. 네이버에서 데이터 가져오기
    def get_NaverStock(self, code_stock):


        # 초기화
        for row in code_stock:
            code = row[0]
            DB_Manager.db_control().delDBtable(dir_naver_ks, 'kospi_' + code)
            print('1. 테이블 삭제: ', code)


        # 최초 데이터 가져오기
        for row in code_stock:


            df = pd.DataFrame()
            code = row[0]
            company = row[1]
            url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)

            for page in range(1, 38):
                pg_url = '{url}&page={page}'.format(url=url, page=page)
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)
                # self.code_df = self.code_df.rename(columns={'회사명': 'name', '종목코드': 'code'})

            df = df.dropna()

            df.to_sql('kospi_' + code, con_naver_ks, chunksize=1000)
            print(df.head())

            self.count = self.count + 1;

            print('count: ',self.count)
            print('///////////////////////////')

    # 3. 일일 데이터 추가: n분마다 실행되는 함수 추가필요
    def add_OnedayStock(self,code_stock):


        count = 0
        check =[]

        for rows in code_stock:

            try:
                code = rows[0]
                df = pd.DataFrame()
                url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
                pg_url = '{url}&page={page}'.format(url=url, page=1)
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)
                df = df.dropna()
                row = df.ix[0]

            except:
                count = count + 1
                check.append(code)
                pass

            else:
                data = []
                index_ks = self.now
                date_ks = row[0]
                close = str(row[1])
                netChange = str(row[2])
                open = str(row[3])
                high = str(row[4])
                low = str(row[5])
                volume = str(row[6])

                value = '?,?,?,?,?,?,?,?'
                data = [index_ks, date_ks, close, netChange, open, high, low, volume]
                DB_Manager.db_control().replaceDBtable(dir_naver_ks, 'kospi_' + code, value, data)
                print('4-1. 추가종목코드: ', code)


        print("4-2. 에러코드:", check)
        print('4-3. 에러총계 ', count)

    # 4. 전략진행 및 DB저장
    def logic_Start(self,code_stock):

        count = 0

        for row in code_stock:

            print('row ',row)
            code = row[0]
            company = row[1]
            kospi_code = 'kospi_'+code
            column = "날짜, 종가"
            date_ks = "날짜"
            # limit = 50
            temp_list_price = DB_Manager.db_control().viewDBdata_Close(dir_naver_ks,column,kospi_code,date_ks,limit_day)
            list_price = []
            list_daydiff = []
            count = count + 1

            print('1. 회사: ',company)
            print('2. 코드: ',code)

            for value in temp_list_price:

                # value: ('2018-02-05 00:00:00', 7800.0)
                price = value[1]
                list_price.append(price)
                last = len(list_price)

            print("3.종가 리스트: ", list_price)


            for i in range(1, len(list_price)):

                today = i - 1
                yesterday = i
                day_diff = list_price[today]/list_price[yesterday]
                list_daydiff.append(day_diff)


            print("4.일변동: ", list_daydiff)

            list_result = AL_Manager.AlgorithmManager.start_logic(list_daydiff)
            company_ks = company+'('+code+')'
            list_result.insert(0, company_ks)
            self.result_ks.append(list_result)

            print('5. 최종 데이터: ',self.result_ks)
            print(len(self.result_ks))
            print('6. 전체회사 ',count)

            # break

        default_column = '(종목 TEXT ,변동성 INTEGER,평균 INTEGER,평가1 INTEGER,평가2 INTEGER,평가3 INTEGER,수익률_별점 INTEGER,안정성_별점 INTEGER,최종평가 INTEGER,종합점수 INTEGER)'
        str_date = self.now.strftime('%Y%m%d')
        str_date = str_date[2:8]
        table = 'STG1_' + str_date
        DB_Manager.db_control().create(dir_result, table, default_column)

        column = '(\'종목\',\'변동성\',\'평균\',\'평가1\',\'평가2\',\'평가3\',\'수익률_별점\',\'안정성_별점\',\'최종평가\',\'종합점수\') values (?,?,?,?,?,?,?,?,?,?)'
        length = len(self.result_ks)
        value = '?,?,?,?,?,?,?,?,?,?'

        DB_Manager.db_control().saveResult(dir_result,table,column,length,self.result_ks)
        # DB_Manager.db_control().replaceResult(dir_result,table,value,length,self.result_ks)



        print("결과DB 완성")


    # def reqFirebase(self):




def main():

    ms = MainStock()

    code_ks = ms.get_stockCode(dir_kospi, table_kospi)
    # ms.get_NaverStock(code_ks)
    # ms.make_StockDB(code_ks)

    # ms.add_OnedayStock(code_ks)
    ms.logic_Start(code_ks)



if __name__ == '__main__':
    main()








 # # 3. [생략] 네이버에서 가지고 온 데이터 다시 DB 복사: 인덱스 컬럼을 제거하기 위해서
 #    def make_StockDB(self,code_stock):
 #
 #
 #        for row in code_stock:
 #
 #            data_column = '(Date TIMESTAMP ,Open REAL,High REAL,Low REAL,Close REAL,`Net CHange` REAL,Volume INTEGER)'
 #            code = row[0]
 #            DB_Manager.db_control().create(dir_db_ks,'kospi_'+code,data_column)
 #            print('1. 건강DB 테이블 생성: ',code)
 #
 #
 #        for row in code_stock:
 #
 #            code = row[0]
 #            column = '날짜,종가,전일비,시가,고가,저가,거래량'
 #            rows = DB_Manager.db_control().viewDBdata_selectedColumn(dir_naver_ks, column, 'kospi_' + code)
 #            print('1: ',rows)
 #
 #
 #            for row in rows:
 #
 #                data =[]
 #                date_ks = row[0]
 #                open = str(row[3])
 #                high = str(row[4])
 #                low = str(row[5])
 #                close = str(row[1])
 #                netChange = str(row[2])
 #                volume = str(row[6])
 #
 #                value = '?,?,?,?,?,?,?'
 #                data = [date_ks,open,high,low,close,netChange,volume]
 #                DB_Manager.db_control().replaceDBtable(dir_db_ks,'kospi_'+code,value,data)
 #
 #            print('2.건강DB로 복사: ',code)
 #


  #
    #
    # # 2. 야후DB 초기화 및 종목 시세 가져오기
    # def save_stock(self, code_ks, code_kq):
    #
    #     for row in code_ks:
    #         code = row[0]
    #         DB_Manager.db_control().delDBtable(self.dir_yahoo_ks, 'kospi_' + code)
    #         print('1. 야후DB 테이블 삭제: ', code)
    #
    #     for row in code_ks:
    #         code = row[0]
    #         company = row[1]
    #         symbol = code + '.KS'
    #
    #         df = data.get_data_yahoo(symbol, self.start_date, self.end_date)
    #         temp_df = DataFrame(df)
    #         temp_df.to_sql('kospi_' + code, self.con_yahoo_ks, chunksize=1000)
    #         print('2.야후 데이터 가져오기: ', company)

    # #3. 야후에서 받지 못하고 비어있는 테이블 체크
    # def check_empty(code_ks, code_kq):
    #
    #     global list_Com_ks
    #     global list_Com_kq
    #     global list_noneCom_ks
    #     global list_noneCom_kq
    #
    #     list_Company_ks = []
    #     list_Company_kq = []
    #     list_noneCompany_ks = []
    #     list_noneCompany_kq = []
    #
    #
    #     num_ks1 = 0
    #     num_ks2 = 0
    #     num_kq1 = 0
    #     num_kq2 = 0
    #
    #     print("code_ks: ",code_ks)
    #     print("code_kq: ",code_kq)
    #
    #
    #
    #     for row in code_ks:
    #
    #         code = row[0]
    #         company = row[1]
    #         ks_code = 'kospi_'+code
    #
    #
    #         try:
    #
    #             temp_exist = instance_DB.existDBdata(dir_yahoo_ks, ks_code)
    #             exist = temp_exist[0]
    #
    #             # print("temp_exist: ",temp_exist)
    #             # print("exist: ",exist[0])
    #
    #             if exist[0] == 0:
    #
    #                 num_ks1 = num_ks1 + 1
    #                 list_noneCompany_ks.append(code)
    #                 # instance_DB.delDBtable(dir_yahoo_ks, ks_code)
    #                 print('3. 누락된 야후DB 체크(누락): ',company)
    #
    #
    #             else:
    #                 num_ks2 = num_ks2 + 1
    #                 list_ks = [code, company]
    #                 list_Company_ks.append(list_ks)
    #                 print('3. 누락된 야후DB 체크(정상): ', company, exist[0])
    #
    #                 pass
    #
    #         except sqlite3.Error as e:
    #             num_ks1 = num_ks1 + 1
    #             list_noneCompany_ks.append(code)
    #             print('3. 누락된 야후DB 체크(누락): ', company)
    #             print("Database error: %s" % e)
    #
    #     print('3. 누락된 야후DB 체크완료 누락 / 정상  : ',len(list_noneCompany_ks) ,'/',len(list_Company_ks))
    #
    #
    #     # for row in code_kq:
    #     #
    #     #     code = row[0]
    #     #     company = row[1]
    #     #     kq_code = 'kosdaq_'+code
    #     #
    #     #
    #     #     try:
    #     #
    #     #         temp_exist = instance_DB.existDBdata(dir_kosdaq, kq_code)
    #     #         # print("temp_exist: ",temp_exist)
    #     #         # print("temp_exist[0]: ",temp_exist[0])
    #     #         exist = temp_exist[0]
    #     #         # print("exist: ",exist[0])
    #     #
    #     #
    #     #         if exist[0] == 0:
    #     #
    #     #             num_kq1 = num_kq1 + 1
    #     #             print("테이블 내용이 비어있음")
    #     #             print(company)
    #     #             list_noneCompany_kq.append(code)
    #     #             # cursor.execute("drop table " + kospi_code)
    #     #             instance_DB.delDBtable(dir_kosdaq, kq_code)
    #     #
    #     #
    #     #         else:
    #     #             num_kq2 = num_kq2 + 1
    #     #             list_kq = [code, company]
    #     #             list_Company_kq.append(list_kq)
    #     #             print("list_kq: ", list_kq)
    #     #             print("list_Company_kq: ", list_Company_kq)
    #     #             # print("테이블 내용 꽉 차있음")
    #     #             pass
    #     #
    #     #
    #     #     except sqlite3.Error as e:
    #     #         num_kq1 = num_kq1 + 1
    #     #         list_noneCompany_kq.append(code)
    #     #         print("Database error: %s" % e)
    #
    #
    #     list_Com_ks = list_Company_ks
    #     list_noneCom_ks = list_noneCompany_ks
    #
    #
    # #4. 누락된 테이블 삭제
    # def delete_emptyData():
    #
    #     num = 0
    #
    #     # print('list_noneCom_ks: ',list_noneCom_ks)
    #     # print('list_noneCom_ks 개수: ',len(list_noneCom_ks))
    #
    #     for row in list_noneCom_ks:
    #
    #         ks_code = 'kospi_' + row
    #         instance_DB.delDBtable(dir_yahoo_ks, ks_code)
    #         print('4. 누락된 테이블 삭제: ',row)
    #         num = num +1
    #
    #     # print('[0th]삭제된 테이블: ',num)
    #
    #
    # #5. 누락된 테이블 데이터 야후에서 다시 받
    # def reload_empty():
    #
    #     global check_replay
    #     global check_retry
    #
    #     list_noneCom_kq = []
    #
    #
    #     for code in list_noneCom_ks:
    #
    #         if len(list_noneCom_ks) != 0:
    #
    #             kospi_code = 'kospi_'+code
    #             symbol = code + '.KS'
    #             df = data.get_data_yahoo(symbol, start_date, end_date)
    #             temp_df = DataFrame(df)
    #             temp_df.to_sql(kospi_code, con_yahoo_ks, chunksize=1000)
    #
    #             print("4. 야후에서 누락된 데이터 다시 받기: ",code)
    #
    #
    #             try:
    #
    #                 temp_exist = instance_DB.existDBdata(dir_yahoo_ks, kospi_code)
    #                 exist = temp_exist[0]
    #
    #                 print("ks exist[0] : ", exist[0])
    #
    #                 if exist[0] != 0:
    #                     list_noneCom_ks.remove(code)
    #
    #                 else:
    #
    #                     instance_DB.delDBtable(dir_yahoo_ks, kospi_code)
    #
    #                     pass
    #
    #             except sqlite3.Error as e:
    #
    #                 list_noneCom_ks.remove(code)
    #                 print("Database error: %s" % e)
    #
    #         else:
    #             return
    #
    #
    #     # for code in list_noneCompany_kq:
    #     #
    #     #     if len(list_noneCompany_kq) != 0:
    #     #         kosdaq_code = 'kosdaq_'+code
    #     #         symbol = code + '.KQ'
    #     #         df = data.get_data_yahoo(symbol, start_date, end_date)
    #     #         temp_df = DataFrame(df)
    #     #         temp_df.to_sql(kosdaq_code, con_kosdaq, chunksize=1000)
    #     #
    #     #
    #     #         try:
    #     #
    #     #             temp_exist = instance_DB.existDBdata(dir_kosdaq, kosdaq_code)
    #     #             exist = temp_exist[0]
    #     #
    #     #             # print("kq temp_exist : ", temp_exist)
    #     #             # print("kq exist : ", exist)
    #     #             # print("kq exist[0] : ", exist[0])
    #     #
    #     #
    #     #             if exist[0] != 0:
    #     #                 list_noneCompany_kq.remove(code)
    #     #             else:
    #     #                 # print("테이블 내용 꽉 차있음")
    #     #                 pass
    #     #
    #     #         except sqlite3.Error as e:
    #     #             print("Database error: %s" % e)
    #     #
    #     #     else:
    #     #         return
    #
    #     check_replay = check_replay + 1
    #
    #
    #     if check_replay > check_retry:
    #         print("누락된 테이블: ",list_noneCom_ks )
    #         print("check_replay 횟수 초과")
    #         return
    #
    #
    #     if len(list_noneCom_ks) != 0 or len(list_noneCom_kq) != 0:
    #
    #         print("4. 야후에서 누락된 데이터 다시 받기 횟수: ", check_replay)
    #         reload_empty()
    #     else:
    #         return
    #
    #
    # #6. 전체 종목 코드에서 누락된 코드 제외
    # def set_listCdoe(code_ks,code_kq):
    #
    #
    #     global code_ks_re
    #     global code_kq_re
    #     code_ks_recent = []
    #     code_kq_recent = []
    #
    #     for row in code_ks:
    #         code = row[0]
    #         code_ks_recent.append(code)
    #
    #     for row in list_noneCom_ks:
    #         code_ks_recent.remove(row)
    #
    #     print("5. 전체 코드에서 누락된 회사 코드 종목 제외")
    #
    #     code_ks_re = code_ks_recent
    #     code_kq_re = code_kq_recent
    #
    #
    # #7. 야후DB에 있는 데이터를 건강DB로 복사: 테이블 생성 포함
    # def makd_heathDB(code_ks_re,code_kq_re):
    #
    #     data_column = '(Date TIMESTAMP ,Open REAL,High REAL,Low REAL,Close REAL,`Adj Close` REAL,Volume INTEGER)'
    #
    #     for row in code_ks_re:
    #         code = row
    #         instance_DB.create(dir_ks_db,'kospi_'+code,data_column)
    #         print('6. 건강DB 테이블 생성: ',code)
    #
    #
    #     for row in code_ks_re:
    #         code = row
    #         rows_yahoo_ks = instance_DB.viewDBdata_all(dir_yahoo_ks,'kospi_'+code)
    #
    #         for row in rows_yahoo_ks:
    #
    #             data =[]
    #             date_ks = row[0]
    #             open = str(row[1])
    #             high = str(row[2])
    #             low = str(row[3])
    #             close = str(row[4])
    #             adjClose = str(row[5])
    #             volume = str(row[6])
    #
    #             value = '?,?,?,?,?,?,?'
    #             data = [date_ks,open,high,low,close,adjClose,volume]
    #             instance_DB.replaceDBtable(dir_ks_db,'kospi_'+code,value,data)
    #
    #         print('6.야후DB 데이터를 건강DB로 복사: ',code)
    #
    #
    #
    #
    #     # for row in code_kq:
    #     #     code = row[0]
    #     #     company = row[1]
    #     #     # print(code)
    #     #     # print(company)
    #     #
    #     #     symbol = code + '.KQ'
    #     #     df = data.get_data_yahoo(symbol, start_date, end_date)
    #     #     temp_df = DataFrame(df)
    #     #     temp_df.to_sql('kosdaq_' + code, con_yahoo_kq, chunksize=1000)
    #