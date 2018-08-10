

import math
import numpy
import sqlite3
import sys
import pickle
import re
import pandas as pd
import datetime
import DB_Manager
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



list_ks = []
list_kq = []



list_result_ks = []
list_result_ks_all = []
list_result_kq = []
list_result_kq_all = []



dir_yahoo_ks ='./yahoo_stock_ks.db'
dir_yahoo_kq ='./yahoo_stock_kq.db'
dir_ks_code = './kospi_code.db'
dir_kq_code = './kosdaq_code.db'
dir_ks_db = './kospi_db.db'
dir_kq_db = './kosdaq_db.db'
dir_result = './RESULT.db'


pre_day = -100
check_retry = 2
check_replay = 0

table_kospi = 'KOSPI'
table_kosdaq = 'KOSDAQ'
con_yahoo_ks = sqlite3.connect("./yahoo_stock_ks.db")
con_yahoo_kq = sqlite3.connect("./yahoo_stock_kq.db")
con_kospi = sqlite3.connect("./kospi_db.db")
con_kosdaq = sqlite3.connect("./kosdaq_db.db")

# cur_ks = con_kospi.cursor()
instance_DB = DB_Manager.db_control()


now = datetime.datetime.now()
end_date = now.strftime('%Y-%m-%d')
pre = now + datetime.timedelta(days = pre_day)
start_date = pre.strftime('%Y-%m-%d')
data_source = 'yahoo'
repalce_positions ='(Date, Open, High, Low, Close,`Adj Close`, Volume )'



list_Com_ks = []
list_Com_kq = []
list_noneCom_ks = []
list_noneCom_kq = []

code_ks_re = []
code_kq_re = []


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



def __init__(self):
    pass


# 1. 코스피, 코스닥 종목 가져오기
def get_stockCode(dir_db,table):

    rows = instance_DB.viewDBdata_stockCode(dir_db,table)
    print(rows)
    return rows


# 2. 야후DB 초기화 및 종목 시세 가져오기
def save_stock(code_ks, code_kq):


    for row in code_ks:
        code = row[0]
        instance_DB.delDBtable(dir_yahoo_ks,'kospi_'+code)
        print('1. 야후DB 테이블 삭제: ',code)

    for row in code_ks:

        code = row[0]
        company = row[1]
        symbol = code + '.KS'

        df = data.get_data_yahoo(symbol, start_date, end_date)
        temp_df = DataFrame(df)
        temp_df.to_sql('kospi_'+code, con_yahoo_ks, chunksize=1000)
        print('2.야후 데이터 가져오기: ',company)


#3. 야후에서 받지 못하고 비어있는 테이블 체크
def check_empty(code_ks, code_kq):

    global list_Com_ks
    global list_Com_kq
    global list_noneCom_ks
    global list_noneCom_kq

    list_Company_ks = []
    list_Company_kq = []
    list_noneCompany_ks = []
    list_noneCompany_kq = []


    num_ks1 = 0
    num_ks2 = 0
    num_kq1 = 0
    num_kq2 = 0

    print("code_ks: ",code_ks)
    print("code_kq: ",code_kq)



    for row in code_ks:

        code = row[0]
        company = row[1]
        ks_code = 'kospi_'+code


        try:

            temp_exist = instance_DB.existDBdata(dir_yahoo_ks, ks_code)
            exist = temp_exist[0]

            # print("temp_exist: ",temp_exist)
            # print("exist: ",exist[0])

            if exist[0] == 0:

                num_ks1 = num_ks1 + 1
                list_noneCompany_ks.append(code)
                # instance_DB.delDBtable(dir_yahoo_ks, ks_code)
                print('3. 누락된 야후DB 체크(누락): ',company)


            else:
                num_ks2 = num_ks2 + 1
                list_ks = [code, company]
                list_Company_ks.append(list_ks)
                print('3. 누락된 야후DB 체크(정상): ', company, exist[0])

                pass

        except sqlite3.Error as e:
            num_ks1 = num_ks1 + 1
            list_noneCompany_ks.append(code)
            print('3. 누락된 야후DB 체크(누락): ', company)
            print("Database error: %s" % e)

    print('3. 누락된 야후DB 체크완료 누락 / 정상  : ',len(list_noneCompany_ks) ,'/',len(list_Company_ks))


    # for row in code_kq:
    #
    #     code = row[0]
    #     company = row[1]
    #     kq_code = 'kosdaq_'+code
    #
    #
    #     try:
    #
    #         temp_exist = instance_DB.existDBdata(dir_kosdaq, kq_code)
    #         # print("temp_exist: ",temp_exist)
    #         # print("temp_exist[0]: ",temp_exist[0])
    #         exist = temp_exist[0]
    #         # print("exist: ",exist[0])
    #
    #
    #         if exist[0] == 0:
    #
    #             num_kq1 = num_kq1 + 1
    #             print("테이블 내용이 비어있음")
    #             print(company)
    #             list_noneCompany_kq.append(code)
    #             # cursor.execute("drop table " + kospi_code)
    #             instance_DB.delDBtable(dir_kosdaq, kq_code)
    #
    #
    #         else:
    #             num_kq2 = num_kq2 + 1
    #             list_kq = [code, company]
    #             list_Company_kq.append(list_kq)
    #             print("list_kq: ", list_kq)
    #             print("list_Company_kq: ", list_Company_kq)
    #             # print("테이블 내용 꽉 차있음")
    #             pass
    #
    #
    #     except sqlite3.Error as e:
    #         num_kq1 = num_kq1 + 1
    #         list_noneCompany_kq.append(code)
    #         print("Database error: %s" % e)


    list_Com_ks = list_Company_ks
    list_noneCom_ks = list_noneCompany_ks


#4. 누락된 테이블 삭제
def delete_emptyData():

    num = 0

    # print('list_noneCom_ks: ',list_noneCom_ks)
    # print('list_noneCom_ks 개수: ',len(list_noneCom_ks))

    for row in list_noneCom_ks:

        ks_code = 'kospi_' + row
        instance_DB.delDBtable(dir_yahoo_ks, ks_code)
        print('4. 누락된 테이블 삭제: ',row)
        num = num +1

    # print('[0th]삭제된 테이블: ',num)


#5. 누락된 테이블 데이터 야후에서 다시 받
def reload_empty():

    global check_replay
    global check_retry

    list_noneCom_kq = []


    for code in list_noneCom_ks:

        if len(list_noneCom_ks) != 0:

            kospi_code = 'kospi_'+code
            symbol = code + '.KS'
            df = data.get_data_yahoo(symbol, start_date, end_date)
            temp_df = DataFrame(df)
            temp_df.to_sql(kospi_code, con_yahoo_ks, chunksize=1000)

            print("4. 야후에서 누락된 데이터 다시 받기: ",code)


            try:

                temp_exist = instance_DB.existDBdata(dir_yahoo_ks, kospi_code)
                exist = temp_exist[0]

                print("ks exist[0] : ", exist[0])

                if exist[0] != 0:
                    list_noneCom_ks.remove(code)

                else:

                    instance_DB.delDBtable(dir_yahoo_ks, kospi_code)

                    pass

            except sqlite3.Error as e:

                list_noneCom_ks.remove(code)
                print("Database error: %s" % e)

        else:
            return


    # for code in list_noneCompany_kq:
    #
    #     if len(list_noneCompany_kq) != 0:
    #         kosdaq_code = 'kosdaq_'+code
    #         symbol = code + '.KQ'
    #         df = data.get_data_yahoo(symbol, start_date, end_date)
    #         temp_df = DataFrame(df)
    #         temp_df.to_sql(kosdaq_code, con_kosdaq, chunksize=1000)
    #
    #
    #         try:
    #
    #             temp_exist = instance_DB.existDBdata(dir_kosdaq, kosdaq_code)
    #             exist = temp_exist[0]
    #
    #             # print("kq temp_exist : ", temp_exist)
    #             # print("kq exist : ", exist)
    #             # print("kq exist[0] : ", exist[0])
    #
    #
    #             if exist[0] != 0:
    #                 list_noneCompany_kq.remove(code)
    #             else:
    #                 # print("테이블 내용 꽉 차있음")
    #                 pass
    #
    #         except sqlite3.Error as e:
    #             print("Database error: %s" % e)
    #
    #     else:
    #         return

    check_replay = check_replay + 1


    if check_replay > check_retry:
        print("누락된 테이블: ",list_noneCom_ks )
        print("check_replay 횟수 초과")
        return


    if len(list_noneCom_ks) != 0 or len(list_noneCom_kq) != 0:

        print("4. 야후에서 누락된 데이터 다시 받기 횟수: ", check_replay)
        reload_empty()
    else:
        return


#6. 전체 종목 코드에서 누락된 코드 제외
def set_listCdoe(code_ks,code_kq):


    global code_ks_re
    global code_kq_re
    code_ks_recent = []
    code_kq_recent = []

    for row in code_ks:
        code = row[0]
        code_ks_recent.append(code)

    for row in list_noneCom_ks:
        code_ks_recent.remove(row)

    print("5. 전체 코드에서 누락된 회사 코드 종목 제외")

    code_ks_re = code_ks_recent
    code_kq_re = code_kq_recent


#7. 야후DB에 있는 데이터를 건강DB로 복사: 테이블 생성 포함
def makd_heathDB(code_ks_re,code_kq_re):

    data_column = '(Date TIMESTAMP ,Open REAL,High REAL,Low REAL,Close REAL,`Adj Close` REAL,Volume INTEGER)'

    for row in code_ks_re:
        code = row
        instance_DB.create(dir_ks_db,'kospi_'+code,data_column)
        print('6. 건강DB 테이블 생성: ',code)


    for row in code_ks_re:
        code = row
        rows_yahoo_ks = instance_DB.viewDBdata_all(dir_yahoo_ks,'kospi_'+code)

        for row in rows_yahoo_ks:

            data =[]
            date_ks = row[0]
            open = str(row[1])
            high = str(row[2])
            low = str(row[3])
            close = str(row[4])
            adjClose = str(row[5])
            volume = str(row[6])

            value = '?,?,?,?,?,?,?'
            data = [date_ks,open,high,low,close,adjClose,volume]
            instance_DB.replaceDBtable(dir_ks_db,'kospi_'+code,value,data)

        print('6.야후DB 데이터를 건강DB로 복사: ',code)




    # for row in code_kq:
    #     code = row[0]
    #     company = row[1]
    #     # print(code)
    #     # print(company)
    #
    #     symbol = code + '.KQ'
    #     df = data.get_data_yahoo(symbol, start_date, end_date)
    #     temp_df = DataFrame(df)
    #     temp_df.to_sql('kosdaq_' + code, con_yahoo_kq, chunksize=1000)

#8. 전략1 진행
def LOGIC_START(code_ks,code_kq):

    column = "Date,`Adj Close`"
    num = 0

    print('//////////////////////////////////////')
    print('code_ks: ', code_ks)
    print("code_ks 개수: ",len(code_ks))
    print("list_noneCom_ks" ,list_noneCom_ks)
    print("list_noneCom_ks 개수: ",len(list_noneCom_ks))
    print('//////////////////////////////////////')



    for row1 in code_ks:

        code_origin = row1[0]
        check = row1[1]

        if check == '하나니켈2호':
            print("하나니켈2호 입니다.")

        if code_origin == '004020':
            print("004020 입니다.")

        for row2 in list_noneCom_ks:
            code_var = row2
            if code_origin == code_var:
                print("row1: ",row1)
                code_ks.remove(row1)
                num = num + 1
    print("code_ks 개수: ",len(code_ks))
    print("num 개수: ",num)


    for row in code_ks:

        list_price = []
        list_daydiff = []

        code = row[0]
        company = row[1]
        print("1.회사: ",company)
        print("2.종목: ",code)

        kospi_code = 'kospi_'+code

        temp_list_price = instance_DB.viewDBdata_adjClose(dir_ks_db,column,kospi_code)

        if len(temp_list_price) > 50:
            temp_list_price = temp_list_price[-51:]

            print("3.수정종가 개수: ",len(temp_list_price))

            #if len(temp_list_price) == 1:
            #    return



            for value in temp_list_price:

               # value: ('2018-02-05 00:00:00', 7800.0)
               price = value[1]
               list_price.append(price)

            last = len(list_price)
            print("4.수정종가 리스트: ",list_price)

            for i in range(1, last):
                today = last - 1
                yesterday = today - 1
                # print("today ", today)
                # print("yesterday: ", yesterday)
                last = last - 1
                day_diff = list_price[today]/list_price[yesterday]
                list_daydiff.append(day_diff)

            # print("list_daydiff: ",list_daydiff)
            print("4-1.일변동 리스트 개수: ", len(list_daydiff))

            list_result = LOGIC_ALGORITHM1(list_daydiff)
            print("list_result",list_result)
            company_ks = company+'('+code+')'
            list_result.insert(0, company_ks)
            list_result_ks_all.append(list_result)

        else:
            print("종가 데이터 수가 부족합니다")

    print(list_result_ks_all)
    saveDB_result(list_result_ks_all) # 위치가 맞나요


def LOGIC_ALGORITHM1(list_daydiff):


    # 1. 일변동 = 오늘 종가 / 어제 종가 배열
    # print("1. 일변동: ", list_daydiff)

    # 2. 평균m =  일변동의 평균 - 1
    print('1.일변동: ',list_daydiff)
    day_mean = numpy.mean(list_daydiff)
    M_average = day_mean - 1
    print("2.평균m: ", M_average)

    # 3. 변동성s =  일변동의 표준편차
    S_volatility = numpy.std(list_daydiff)
    print("3.변동성s: ", S_volatility)

    # 4. 평가지표G
    G_Result1 = M_average - pow(S_volatility,2)
    print("4-1.평가지표G1>  m - s 제곱: ", G_Result1)

    G_Result2 = M_average - pow(S_volatility,2)/2
    print("4-2.평가지표G2> m - s 제곱 / 2: ", G_Result2)

    G_Result3 = numpy.divide(M_average,S_volatility+0.000000000001)
    print("4-3.평가지표G3> m/s: ", G_Result3)

    G_Abs_Result = G_Result2 * G_Result3
    print("4-4.최종 평가지표 G2xG3: ", G_Abs_Result)

    # 5. 별점지표T
    EVAL_2 = numpy.clip(0.5 * numpy.int(200 * (G_Result2+0.005)/0.5),0,5)
    EVAL_3 = numpy.clip(0.5 * numpy.int(10 * (G_Result3 + 0.1) / 0.5),0,5)
    if G_Result1 > 0 and G_Abs_Result > 0:
        EVAL_TOTAL = 10*(10+numpy.log(G_Result2 * G_Result3))
    else:
        print(' * EVAL_TOTAL = 0 나옴')
        EVAL_TOTAL = 0

    print("5.수익률_별점(EVAL_2)",EVAL_2)
    print("6.안정성_별점(EVAL_3)",EVAL_3)
    print("7.종합점수(EVAL_TOTAL)",EVAL_TOTAL)

    print(".....................START...........................")

    list_result_G = [S_volatility,M_average,G_Result1,G_Result2,EVAL_2,G_Result3,EVAL_3,G_Abs_Result,EVAL_TOTAL]
    # valueStr = '(변동성,평균,평가1,평가2,평가3,최종평가)

    return list_result_G


def saveDB_result(result_ks):

    table = 'Standard_STG1'
    valueStr = '(\'종목\',\'변동성\',\'평균\',\'평가1\',\'평가2\',\'수익률_별점\',\'평가3\',\'안정성_별점\',\'최종평가\',\'종합점수\') values (?,?,?,?,?,?,?,?,?,?)'
    lenData_ks = len(result_ks)
    instance_DB.saveDBtable(dir_result,table,valueStr,lenData_ks,result_ks)
    # lenData_kq = len(result_kq)
    # instance_DB.saveDBtable(dir_result, table, valueStr, lenData_kq, result_kq)
    print("결과DB 완성")


def main():

    code_ks = get_stockCode(dir_ks_code,table_kospi)
    # code_kq = get_stockCode(dir_kq_code,table_kosdaq)
    code_kq = []

    save_stock(code_ks,code_kq)
    check_empty(code_ks,code_kq)
    delete_emptyData()
    reload_empty()
    set_listCdoe(code_ks,code_kq)
    makd_heathDB(code_ks_re,code_kq_re)
    LOGIC_START(code_ks,code_kq)


if __name__ == '__main__':
    main()