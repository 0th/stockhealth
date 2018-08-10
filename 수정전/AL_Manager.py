
import math
import numpy
import sqlite3
import sys
import re
import pandas as pd
import datetime
import DB_Manager
from pandas_datareader import data
from pandas import Series, DataFrame





class AlgorithmManager:



    def start_logic(list_daydiff):



        print('')
        print('********* START logic *********')

        # 1. 일변동 = 오늘 종가 / 어제 종가 배열
        print("1. 일변동: ", list_daydiff)

        # 2. 평균m =  일변동의 평균 - 1
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

        print('********* END logic *********')
        print('')

        list_result_G = [S_volatility,M_average,G_Result1,G_Result2,G_Result3,EVAL_2,EVAL_3,G_Abs_Result,EVAL_TOTAL]

        # valueStr = '(\'종목\',\'변동성\',\'평균\',\'평가1\',\'평가2\',\'평가3\',\'수익률_별점\',\'안정성_별점\',\'최종평가\',\'종합점수\') values (?,?,?,?,?,?,?,?,?,?)'

        return list_result_G
