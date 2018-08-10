

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
import json
import threading
import time

from datetime import date

from pandas_datareader import data
from pandas import Series, DataFrame
import fix_yahoo_finance as yf
yf.pdr_override()
numpy.seterr(divide='ignore', invalid='ignore')



# 로직처리

# 1. 일변동 = 오늘 종가 / 어제 종가
# 2. 평균m =  일변동의 평균 - 1
# 3. 변동성s =  일변동의 표준편차
# 4. 평가지표G = m - s 제곱 / 2



# dir_yahoo_ks = './db/yahoo_stock_ks.db'
# dir_yahoo_kq = './db/yahoo_stock_kq.db'
dir_result = './db/RESULT.db'



dir_kospi = './db/kospi_code.db'
# dir_kosdaq = './db/kosdaq_code.db'
dir_naver_ks=  './db/naver_ks.db'
# dir_naver_kq=  './db/naver_kq.db'



dir_naver_ks_Realtime= './db/naver_ks_Realtime.db'



# dir_db_ks = './db/kospi.db'
# dir_db_kq = './db/kosdaq.db'


# con_yahoo_ks = sqlite3.connect("./db/yahoo_stock_ks.db")
# con_yahoo_kq = sqlite3.connect("./db/yahoo_stock_kq.db")
# con_kospi = sqlite3.connect("./db/kospi_db.db")
# con_kosdaq = sqlite3.connect("./db/kosdaq_db.db")

con_naver_ks = sqlite3.connect("./db/naver_ks.db")
# con_naver_kq = sqlite3.connect("./db/naver_kq.db")

con_naver_ks_Realtime = sqlite3.connect("./db/naver_ks_Realtime.db")


table_kospi = 'KOSPI'
# table_kosdaq = 'KOSDAQ'


limit_day = 50
start_time = time.time()





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

    url_day = 'http://finance.naver.com/item/sise_day.nhn?code={code}'
    url_realtime = 'http://finance.naver.com/item/sise_time.nhn?code={code}&thistime={realtime}'



# /////////////////////////// START ///////////////////////////

    # 0. 시작
    def __init__(self):
        pass


    # 1. 코스피, 코스닥 종목 가져오기
    def get_stockCode(self,db, table):
        rows = DB_Manager.db_control().viewDBdata_stockCode(db,table)
        print(rows)
        return rows


    # 2. 네이버에서 데이터 가져오기: 최초 한번만 진행
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


    # 3. 전략진행 및 DB저장
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
            # company_ks = company+'('+code+')'
            list_result.insert(0, company)
            list_result.insert(1, code)
            self.result_ks.append(list_result)

            print('5. 최종 데이터: ',self.result_ks)
            print(len(self.result_ks))
            print('6. 전체회사 ',count)

            # break


        # 1. DB 테이블 생성: 오늘 날짜
        default_column = '(종목 TEXT ,코드 TEXT ,변동성 INTEGER,평균 INTEGER,평가1 INTEGER,평가2 INTEGER,평가3 INTEGER,수익률_별점 INTEGER,안정성_별점 INTEGER,최종평가 INTEGER,종합점수 INTEGER)'
        str_date = self.now.strftime('%Y%m%d')
        str_date = str_date[2:8]
        table = 'STG1_' + str_date
        DB_Manager.db_control().create(dir_result, table, default_column)

        # 2. DB에 결과값 넣기
        column = '(\'종목\',\'코드\',\'변동성\',\'평균\',\'평가1\',\'평가2\',\'평가3\',\'수익률_별점\',\'안정성_별점\',\'최종평가\',\'종합점수\') values (?,?,?,?,?,?,?,?,?,?,?)'
        length = len(self.result_ks)
        # value = '?,?,?,?,?,?,?,?,?,?'

        # 0th
        DB_Manager.db_control().saveResult(dir_result,table,column,length,self.result_ks)
        print("결과DB 완성")

        return self.result_ks






    #   1. 기존 데이터의 최근 날짜를 가져온다.
    #   2. 현재 날짜를 가져온다.
    #   3. 날짜를 비교해서 가져와야 하는 페이지를 계산
    #   4. 부족한 데이터만 가져오기


    # 3. 일별시세 데이터 추가: 하루에 한번 실행
    def addDayStock(self,code_stock):

        # 1. 기존 sqlite DB에 있는 샘플 코드 날짜 2개를 추출 : kospi_000020, kospi_900140
        # 2. 날짜를 비교해서 가장 업데이트 안된 날짜를 기준으로 네이버 최신 데이터와 비교

        column = '날짜'
        table1 = 'kospi_000020'
        table2 = 'kospi_900140'
        limit = 3
        code = '000020'



        check_day1 = DB_Manager.db_control().viewDBdata_Day(dir_naver_ks, column, table1, column, limit)
        check_day2 = DB_Manager.db_control().viewDBdata_Day(dir_naver_ks, column, table2, column, limit)

        day1 = check_day1[0]
        day2 = check_day2[0]

        d1 = datetime.datetime.strptime(day1[0], "%Y.%m.%d").date()
        d2 = datetime.datetime.strptime(day2[0], "%Y.%m.%d").date()
        print('d1: ', d1)
        print('d2: ', d2)

        delta = d1 - d2
        check_delta = delta.days

        diff_day = d1

        if check_delta == 0 :
            diff_day = d1

        elif check_delta > 0:
            diff_day = d2

        elif check_delta < 0:
            diff_day = d1


        # 1. sqlite DB에 있는 데이터 가져와서 마지막 날짜를 네이버 날짜랑 비교

        df = pd.DataFrame()
        url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
        pg_url = '{url}&page={page}'.format(url=url, page=1)
        df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)
        data = df.ix[1]
        print('data: ', data)



        print(type(data))
        temp = '날짜'
        print(data[0])
        day3 = data[0]
        print('day3: ',day3)
        d3 = datetime.datetime.strptime(day3, "%Y.%m.%d").date()
        print('d3: ',d3)
        delta = d3 - diff_day
        check_delta = delta.days
        print('check_delta: ',check_delta)
        cnt_page = check_delta//10

        print('cnt_page: ',cnt_page)

        cnt = 0

        for row in code_stock:

            cnt = cnt + 1
            df = pd.DataFrame()
            code = row[0]
            company = row[1]
            url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)

            print('------------------------------------------------------------')
            print('1. 순번: ', cnt)
            print('2. 코드: ', code)
            print('3. 회사: ', company)

            for page in range(1, cnt_page):
                pg_url = '{url}&page={page}'.format(url=url, page=page)
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

            df = df.dropna()
            df.to_sql('kospi_' + code, con_naver_ks, if_exists='replace')
            print('4. 일별 데이터: ', df)

    # print("start_time", start_time)  # 출력해보면, 시간형식이 사람이 읽기 힘든 일련번호형식입니다.
    # print("--- %s seconds ---" % (time.time() - start_time))

    # 1. 처음 데이터베이스 > 테이블 만들자
    # 2. 신규 데이터 삽입
    # 3. 데이터 업데이트


    def createRealtimeStock(self):

        column = '(\'종목\' TEXT PRIMARY KEY ,\'코드\' TEXT ,\'체결가\' INTEGER,\'체결시각\' TEXT)'
        table = 'Stock_Realtime'
        DB_Manager.db_control().create(dir_naver_ks_Realtime,table,column)




    def addRealtimeStock(self,code_stock):

        cnt = 0
        start = time.time()
        dropCompany = []
        dropCode = []


        for row in code_stock:

            df = pd.DataFrame()
            cnt += 1

            code = row[0]
            company = row[1]
            print('01. 순서: ',cnt)
            print('02 회사: ',company)

            print('03 코드: ',code)


            # code = '000020'
            # company = 'youngsu'

            now = datetime.datetime.now()
            nowDate = now.strftime('%Y%m%d%H%M%S')
            url= 'https://finance.naver.com/item/sise_time.nhn?code={code}&thistime={thistime}'.format(code=code, thistime = nowDate)
            pg_url = '{url}&page={page}'.format(url=url, page=1)

            try:
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)
                df = df.dropna()
                data = df.ix[1]

                nowDay = now.strftime('%Y-%m-%d')
                Engagement_time = nowDay + ' ' + data[0]

                # time = data[0]

                data_column = []
                data_column.append(company)
                data_column.append(code)
                data_column.append(data[1])
                data_column.append(Engagement_time)

                table = 'Stock_Realtime'
                column = "종목, 코드, 체결가, 체결시각"
                column = '(\'종목\',\'코드\',\'체결가\',\'체결시각\') values (?,?,?,?)'
                length = len(data_column)
                length = 1
                DB_Manager.db_control().insertOrReplaceDB(dir_naver_ks_Realtime, table, column, length, data_column)

                print('04. DB 저장 완료')
                print('--------------------------------------------')

            except Exception as e:
                print('데이터가 없습니다.', e)
                dropCompany.append(company)
                dropCode.append(code)

        done = time.time()
        elapsed = done - start
        print('05. 시간: ', elapsed)
        print('06. 사라진 회사 ', dropCompany)
        print('07. 사라진 회사 수', len(dropCompany))

        # 반올림해서 시간(초)을 구함
        print(int(round(float(elapsed))))









    # 5. 파이어베이스에 데이터 업로드
    def upload_totalDB(self):

        count = 0

        rows = DB_Manager.db_control().viewDBdata_all(dir_result,'STG1_180525')
        for row in rows:

            count = count + 1
            print('회사 카운트: ',count)

            dir = '/Stock/Evaluation/'+row[0]+'/'
            data = {'Name': row[0],
                    'Code': row[1],
                    'Volatility': row[2],
                    'Average': row[3],
                    'Eval1': row[4],
                    'Eval2': row[5],
                    'Eval3': row[6],
                    'StockReturn': row[7],
                    'Stability': row[8],
                    'Result': row[9],
                    'Total': row[10]
                    }

            FB_Manager.FirebaseManager().patch(dir,data)
            print(row[0])

        print("마무리")

    def upload_RankDB(self):


        # 제이슨 파일로 만들어서 보냄

        count = 0

        rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')

        for row in rows:
            count = count + 1
            print('회사 카운트: ', count)

            dir = '/Stock/Rank/'
            data = {
                     row[0]+'('+row[1]+')': row[10]
                    }

            FB_Manager.FirebaseManager().patch(dir, data)
            print(row[0])

        print("마무리")

    def upload_RankDB_JSON(self):

        # 방법: 딕셔너리로 만들고 그 다음 dump를 통해서 json 문자열로 변경하면된다.
        # 제이슨 파일로 만들어서 보냄
        # json_val = json.dump(dict1)

        count = 0

        rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')

        data_json =''

        for row in rows:
            count = count + 1
            print('회사 카운트: ', count)

            data = '\''+row[0]+'('+str(row[1])+')'+'\''+':' + str(row[10]) + ', '
            data_json = data_json + data

            print(data_json)


        dir = '/Stock/Rank/'
        data_json = data_json[:-2]
        print(data_json)


        data = '{'+ data_json +'}'
        data_dic = eval(data)
        print('data_dic: ',data_dic)
        print(len(data_dic))

        # dic 파일로 해도 전달됨
        # data_json = json.dumps(data_dic, ensure_ascii=False)
        # print(data_json)
        # print(type(data_json))

        FB_Manager.FirebaseManager().patch(dir, data_dic)

    def upload_totalDB_JSON(self):


        count = 0

        rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')

        data_json = ''

        for row in rows:
            count = count + 1
            print('회사 카운트: ', count)


            data = '\''+ row[0] +'\'' + ': ' + '{'\
                   + '\'' + 'Name'+'\'' +':'+'\'' + row[0] +'\'' +','\
                   + '\'' + 'Code'+ '\'' +':'+ '\'' +str(row[1]) +'\'' +','\
                   + '\'' + 'Volatility'+'\'' + ':' + str(row[2]) +','\
                   + '\'' + 'Average' +'\'' + ':' + str(row[3]) + ','\
                   + '\'' + 'Eval1' + '\'' +':' + str(row[4]) + ','\
                   + '\'' + 'Eval2' + '\'' +':' + str(row[5]) + ','\
                   + '\'' + 'Eval3' + '\'' +':' + str(row[6]) + ','\
                   + '\'' + 'StockReturn' +'\'' + ':' + str(row[7]) + ','\
                   + '\'' + 'Stability' +'\'' + ':' + str(row[8]) + ','\
                   + '\'' + 'Result' +'\'' + ':' + str(row[9]) + ','\
                   + '\'' + 'Total' + '\'' + ':' + str(row[10]) + '}'+','


            data_json = data_json + data
            print(data_json)

        data_json = data_json[:-1]
        print('1. ',data_json)

        data = '{'+ data_json +'}'
        print('2. ',data)
        print('3. ',type(data))

        data_dic = eval(data)
        print('4. ',type(data_dic))

        dir = '/Stock/Evaluation/'

        FB_Manager.FirebaseManager().patch(dir, data_dic)




def main():

    ms = MainStock()
    code_ks = ms.get_stockCode(dir_kospi, table_kospi)
    # ms.get_NaverStock(code_ks)
    # ms.make_StockDB(code_ks)
    # ms.add_OnedayStock(code_ks)
    # ms.logic_Start(code_ks)
    # ms.addDayStock(code_ks)
    # ms.createRealtimeStock()
    ms.addRealtimeStock(code_ks)
    # ms.request_Firebase()
    # ms.upload_totalDB_JSON()

if __name__ == '__main__':
    main()



