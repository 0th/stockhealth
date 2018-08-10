# coding: utf8

import math
import numpy
import sqlite3
import sys
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
from Log_File import LogPrint
from pandas_datareader import data
from pandas import Series, DataFrame





numpy.seterr(divide='ignore', invalid='ignore')





dir_result = './db/RESULT.db'
dir_kospi = './db/kospi_code.db'
dir_naver_ks=  './db/naver_ks.db'
dir_naver_ks_Realtime= './db/naver_ks_Realtime.db'
con_naver_ks = sqlite3.connect("./db/naver_ks.db")
con_naver_ks_Realtime = sqlite3.connect("./db/naver_ks_Realtime.db")
table_kospi = 'KOSPI'

LIMIT_DAY = 60
start_time = time.time()

GET_PAGERANGE = 20




STATUS_INIT = True
RELEASE_TIME = 0


class MainStock:

    list_ks = []
    list_kq = []
    result_ks = []
    result_kq = []

    pre_day = -100
    check_retry = 2
    check_replay = 0


    DEF_ALIVE_TM = 300
    DEF_PRO_THR = 0
    REPEAT_TIME = 300
    COUNT_TIME = 0



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
    aliveCnt = [0]




    # /////////////////////////// START ///////////////////////////

    # 0. 시작
    def __init__(self):
        self.aliveCnt = [self.DEF_ALIVE_TM]
        pass


    #1-1. [세팅] 코스피, 코스닥 종목 가져오기
    def get_stockCode(self,db, table):
        rows = DB_Manager.db_control().viewDBdata_stockCode(db,table)
        # print(rows)
        return rows


    #1-2. [세팅] 네이버에서 코스피 데이터 가져오기
    def getNaverStock(self, code_stock):

        location = 'get_NaverStock'
        start = time.time()

        for row in code_stock:

            df = pd.DataFrame()
            code = row[0]
            company = row[1]
            url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)

            for page in range(1, GET_PAGERANGE):
                pg_url = '{url}&page={page}'.format(url=url, page=page)
                df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

            df = df.dropna()
            df.to_sql('kospi_' + code, con_naver_ks, if_exists='replace', index=False)
            self.count = self.count + 1;

            print('///////////////////////////')
            print('1. 순서: ',self.count)
            print('2. 회사: ',company)
            print('3. 코드: ',code)
            print('4. 요약: ', df.head())

        self.timeCheck(start,location)


    #2-1. [프로세스] 일별시세 데이터
    def addDayStock(self,code_stock):

        location = 'addDayStock'
        cnt = 0
        now = datetime.datetime.now().strftime("%Y.%m.%d")
        start = time.time()
        LogPrint(curr_date(),'1. addDayStock 시작')
        for row in code_stock:

            cnt += 1
            code = row[0]
            company = row[1]
            table = 'kospi_'+ code
            limit = 1
            df = pd.DataFrame()
            column = '날짜'
            olds = DB_Manager.db_control().viewDBdata_Day(dir_naver_ks, column, table, column, limit)
            old = olds[0][0]

            value = self.cal_elapsedTime(now,old)

            quotient = value//10
            remainder = value%10
            pages = quotient + 1


            # print('//////////////디버그용/////////////////////////')
            # print('1. 순서: ',cnt)
            # print('2. 회사: ',company)
            # print('3. 코드: ',code)
            # print('4. DB 최근 데이터 :',old)
            # print('5. 현재와 DB 날짜 차이 :',value)

            if quotient == 0 and remainder == 0:
                pass
            else:

                if(remainder > 0):
                    pages = 2

                # print('6. 가져올 웹 페이지 수 :', pages)
                url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)

                for page in range(1, pages):
                    pg_url = '{url}&page={page}'.format(url=url, page=page)
                    df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

                df = df.dropna()
                # print('7. 웹에서 가져온 데이터 :', df)
                days = df[['날짜']]
                arr_days = days['날짜'].values


                for day in arr_days:

                    value = self.cal_elapsedTime(day,old)
                    if value > 0 :
                        indexOfday = df[df['날짜'] == day].index.values.astype(int)
                        unit_df = df.ix[indexOfday]
                        unit_df.to_sql('kospi_' + code, con_naver_ks, if_exists='append', index=False)
                    else:
                        pass

        self.timeCheck(start, location)


    #2-2. [프로세스] 실시간 데이터
    def addRealtimeStock(self,code_stock):

        location = 'addRealtimeStock'
        cnt = 0
        start = time.time()
        dropCompany = []
        dropCode = []
        table = '\'Stock_Realtime\''
        value = DB_Manager.db_control().checkDBtable(dir_naver_ks_Realtime, table)
        temp_val = value[0]

        LogPrint(curr_date(),'2. addRealtimeStock 시작')

        if (temp_val[0] == 0):
            column = '(\'종목\' TEXT PRIMARY KEY ,\'코드\' TEXT ,\'체결가\' INTEGER,\'체결시각\' TEXT)'
            DB_Manager.db_control().create(dir_naver_ks_Realtime, table, column)


        for row in code_stock:

            df = pd.DataFrame()
            cnt += 1

            code = row[0]
            company = row[1]

            # print('///////////디버그용/////////////////////')
            # print('1. 순서: ',cnt)
            # print('2 회사: ',company)
            # print('3 코드: ',code)

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

                data_column = []
                data_column.append(company)
                data_column.append(code)
                data_column.append(data[1])
                data_column.append(Engagement_time)

                table = 'Stock_Realtime'
                column = "종목, 코드, 체결가, 체결시각"
                column = '(\'종목\',\'코드\',\'체결가\',\'체결시각\') values (?,?,?,?)'
                length = 1

                DB_Manager.db_control().insertOrReplaceDB(dir_naver_ks_Realtime, table, column, length, data_column)
                # print('04. DB 저장 완료')


            except Exception as e:
                # print('데이터가 없습니다.', e)
                dropCompany.append(company)
                dropCode.append(code)


        # print('----------------------------------')
        # LogPrint('1. 사라진 회사 ', dropCompany)
        # LogPrint('2. 사라진 회사 수', len(dropCompany))

        self.timeCheck(start,location)

    #2-3. [프로세스] 전략진행
    def startAlgorithm(self,code_stock):

        count = 0
        start = time.time()
        location = 'startAlgorithm'
        LogPrint(curr_date(),'3. startAlgorithm 시작')

        for row in code_stock:

            # print('row ',row)
            code = row[0]
            company = row[1]
            kospi_code = 'kospi_'+code
            column = "날짜, 종가"
            date_ks = "날짜"

            temp_list_price = DB_Manager.db_control().viewDBdata_Close(dir_naver_ks,column,kospi_code,date_ks,LIMIT_DAY)

            list_price = []
            list_daydiff = []
            count = count + 1




            for value in temp_list_price:

                price = value[1]
                list_price.append(price)
                last = len(list_price)


            for i in range(1, len(list_price)):

                today = i - 1
                yesterday = i
                day_diff = list_price[today]/list_price[yesterday]
                list_daydiff.append(day_diff)


            list_result = AL_Manager.AlgorithmManager.start_logic(list_daydiff)
            list_result.insert(0, company)
            list_result.insert(1, code)
            self.result_ks.append(list_result)

            # print('1. 회사: ',company)
            # print('2. 코드: ',code)
            # print("3. 종가 리스트: ", list_price)
            # print("4. 일변동: ", list_daydiff)
            # print('5. 최종 데이터: ',self.result_ks)
            # print('6. 최종 데이터 수: ',len(self.result_ks))
            # print('7. 전체회사 ',count)

        str_date = self.now.strftime('%Y%m%d')
        str_date = str_date[2:8]
        table = 'STG1_' + str_date
        table = '\'' + table + '\''
        val = DB_Manager.db_control().checkDBtable(dir_result, table)
        value = val[0][0]
        default_column = '(\'종목\' TEXT PRIMARY KEY,\'코드\' TEXT,\'변동성\' INTEGER,\'평균\' INTEGER,\'수익지수\' INTEGER,\'샤프지수\' INTEGER,\'켈리지수\' INTEGER,\'수익률_점수(100)\' INTEGER,\'안정성_점수(100)\' INTEGER,\'켈리_점수(100)\' INTEGER,\'타이밍지표\' INTEGER,\'종합점수(평균)\' INTEGER)'
        length = len(self.result_ks)
        column = '(\'종목\',\'코드\',\'변동성\',\'평균\',\'수익지수\',\'샤프지수\',\'켈리지수\',\'수익률_점수(100)\',\'안정성_점수(100)\',\'켈리_점수(100)\',\'타이밍지표\',\'종합점수(평균)\') values (?,?,?,?,?,?,?,?,?,?,?,?)'

        if value == 0:
            DB_Manager.db_control().create(dir_result, table, default_column)
            DB_Manager.db_control().insertOrReplaceDB(dir_result, table, column, length, self.result_ks)
            # print('8. 테이블 생성 후 DB 저장 ')

        else:
            DB_Manager.db_control().insertOrReplaceDB(dir_result, table, column, length, self.result_ks)
            # print('8. DB 저장 ')

        self.timeCheck(start,location)


    # 2-4. 파이어베이스에 데이터 업로드
    def uploadRankDB(self):

        # 1. 방법: 딕셔너리로 만들고 그 다음 dump를 통해서 json 문자열로 변경하면된다.
        # 2. 제이슨 파일로 만들어서 보냄
        # 3. json_val = json.dump(dict1)

        # 1. 회사이름
        # 2. 종목
        # 3. 현재가
        # 4. 점수
        # 5. 상태


        count = 0
        start = time.time()
        locaion = 'uploadRankDB'

        now_date = self.now.strftime('%Y%m%d')
        now_date = now_date[2:8]
        table = 'STG1_' + now_date
        table = '\'' + table + '\''
        data_json =''
        rows = DB_Manager.db_control().viewDBdata_all(dir_result, table)
        LogPrint(curr_date(),'4. uploadRankDB 시작')


        for row in rows:

            count = count + 1
            table = 'Stock_Realtime'
            con1 = '\"체결가\"'
            con2 = 'Stock_Realtime where \"종목\"=' +'\"'+ row[0] + '\"'
            obj_price = DB_Manager.db_control().viewDBdata_selectedColumn(dir_naver_ks_Realtime,con1, con2)


            if not obj_price:
                # print('값이 없음')
                pass


            else:
                company = row[0]
                if '/' in company:
                    company = company.replace('/', ' %2F ')
                    # company = '\"'+company + '\"'
                    # print('company: ',company )
                else:
                    price = obj_price[0][0]
                    status = 'hold'
                    data = '\''+ company +'\'' + ': ' + '{' \
                           + '\'' + 'Name'+'\'' +':'+'\'' + company +'\'' +',' \
                           + '\'' + 'Code'+ '\'' +':'+ '\'' +str(row[1]) +'\'' +',' \
                           + '\'' + 'Price'+'\'' + ':' + str(price) +',' \
                           + '\'' + 'Score' +'\'' + ':' + str(row[11]) + ',' \
                           + '\'' + 'Status' + '\'' + ':'+'\'' + str(row[10]) +'\'' + '}'+','

                    data_json = data_json + data


        dir = '/Stock/Rank/'

        data_json = data_json[:-1]
        # print('1. ',data_json)

        data = '{'+ data_json +'}'
        # print('2. ',data)
        # print('3. ',type(data))

        data_dic = eval(data)
        # print('4. ',type(data_dic))


        FB_Manager.FirebaseManager().patch(dir, data_dic)
        self.timeCheck(start,locaion)

    def startStatus(self,code_stock):


        val = DB_Manager.db_control().checkDBtable(dir_result, table)
        value = val[0][0]
        default_column = '(\'종목\' TEXT PRIMARY KEY,\'코드\' TEXT,\'변동성\' INTEGER,\'평균\' INTEGER,\'평가1\' INTEGER,\'평가2\' INTEGER,\'평가3\' INTEGER,\'수익률_별점\' INTEGER,\'안정성_별점\' INTEGER,\'최종평가\' INTEGER,\'종합점수\' INTEGER)'
        column = '(\'종목\',\'코드\',\'변동성\',\'평균\',\'평가1\',\'평가2\',\'평가3\',\'수익률_별점\',\'안정성_별점\',\'최종평가\',\'종합점수\') values (?,?,?,?,?,?,?,?,?,?,?)'

    def timeCheck(self,start,location):

        done = time.time()
        elapsed = done - start
        sec = int(round(float(elapsed)))
        # LogPrint('1. 위치: ', location)
        LogPrint('- 러닝타임: ', sec)
        print('--------------------------')

        return sec

    def cal_elapsedTime(self, current, old):

        d1 = datetime.datetime.strptime(current, "%Y.%m.%d").date()
        d2 = datetime.datetime.strptime(old, "%Y.%m.%d").date()
        delta = d1 - d2
        value = delta.days
        return value

    def monitorTask(self):  # operate every 1sec

        cnt = 0
        start = time.time()
        location = 'monitorTask'

        print('*********** monitorTask 실행 ***********')

        for i in range(len(self.aliveCnt)):
            self.aliveCnt[i] = self.aliveCnt[i] - 1
            # print('프로세스 테스크 재실행: ,',self.aliveCnt[i])

        if (self.aliveCnt[self.DEF_PRO_THR] <= 0):
            self.aliveCnt[self.DEF_PRO_THR] = self.DEF_ALIVE_TM
            self.processTask()

        # for log
        # if (self.aliveCnt[DEF_PRO_THR] <= DEF_ALIVE_TM):
        #     print("### Ready Recover Thread Count: ", self.aliveCnt[DEF_PRO_THR])

        threading.Timer(self.REPEAT_TIME, self.monitorTask).start()

    def processTask(self):  # operate every 1sec

        print('*********** processTask 실행 ***********')
        LogPrint(curr_date(),'실행')

        start = time.time()
        location = 'main'


        cnt = 0
        init_code = self.get_stockCode(dir_kospi, table_kospi)

        if STATUS_INIT == False:
            self.get_NaverStock(init_code)

        # self.addDayStock(init_code)
        # self.addRealtimeStock(init_code)
        self.startAlgorithm(init_code)
        self.uploadRankDB()

        sec = self.timeCheck(start, location)
        print('sec: ',sec)

        self.aliveCnt[self.DEF_PRO_THR] = self.DEF_ALIVE_TM

        threading.Timer(1, self.processTask).start()


    def countTime(self):

        global RELEASE_TIME

        RELEASE_TIME = self.REPEAT_TIME - self.COUNT_TIME
        # RELEASE_TIME = 10

        for i in range(RELEASE_TIME):
            RELEASE_TIME -= 1
            print('재실행 시간', RELEASE_TIME)
            time.sleep(1)





def curr_date():
    now = datetime.datetime.now()
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    return now


def main():

    LogPrint(curr_date(), "MoonStock Program Started!!")
    LogPrint(datetime.datetime.now(), "Started!!")
    ms = MainStock()
    # ms.monitorTask()
    ms.processTask()

if __name__ == '__main__':
    main()



