
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


def moving_average(a, n=5):
    ret = numpy.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n

class AlgorithmManager:


    def start_logic(list_daydiff):


        # 1. 일변동 = 오늘 종가 / 어제 종가 배열
        list_daydiff_adjust = numpy.copy(list_daydiff)
        for i in range(len(list_daydiff_adjust)):
            list_daydiff_adjust[i] = (2-i/(len(list_daydiff_adjust)-1)) * (list_daydiff_adjust[i] - 1) + 1

        list_day_adjust = [0]*(len(list_daydiff_adjust)+1)
        list_day_adjust[-1] = 1
        for i in range(len(list_day_adjust)-1):
            list_day_adjust[-2-i] = list_day_adjust[-1-i] * list_daydiff_adjust[-1-i]

        timing_20 = numpy.sign(list_day_adjust[0]-moving_average(list_day_adjust)[20])
        timing_40 = numpy.sign(list_day_adjust[0]-moving_average(list_day_adjust)[40])
        timing_60 = numpy.sign(list_day_adjust[0]-moving_average(list_day_adjust)[-1])
        timing_list = ['good','normal','bad','danger']

        # 2. 평균m =  일변동의 평균 - 1
        day_mean = numpy.mean(list_daydiff_adjust)
        M_average = day_mean - 1

        # 3. 변동성s =  일변동의 표준편차
        S_volatility = numpy.std(list_daydiff_adjust)

        # 4. 평가지표G
        # G_Result1 = M_average - pow(S_volatility,2)
        G_Result2 = M_average - pow(S_volatility,2)/2
        G_Result3 = numpy.divide(M_average,S_volatility+0.000000000001)
        G_Result4 = numpy.divide(M_average,pow(S_volatility,2)+0.000000000001)
        #G_Result4 = numpy.log(abs(G_Result4)+1) ** 0.5 * numpy.sign(G_Result4)

        # 5. 별점지표T
        #EVAL_2 = numpy.float64(numpy.clip(numpy.int(400 * (G_Result2+0.005)/0.5)*5,-100,100))
        #EVAL_3 = numpy.float64(numpy.clip(numpy.int(20 * (G_Result3 + 0.2) / 0.5)*5,-100,100))
        EVAL_2 = numpy.float64(numpy.int(5000 * numpy.clip(G_Result2,-0.02,0.02)))
        EVAL_3 = numpy.float64(numpy.int(1000/3 * numpy.clip(G_Result3,-0.3,0.3)))
        EVAL_4 = numpy.float64(numpy.int(20 * numpy.clip(G_Result4,-5,5)))
        # EVAL_TOTAL = (abs(EVAL_2 * EVAL_3 * EVAL_4)**(1/3)) * numpy.sign(G_Result2)
        EVAL_TOTAL = numpy.int(5 * (100 + (abs(EVAL_2 * EVAL_3 * EVAL_4) ** (1 / 3)) * numpy.sign(G_Result2)))/10 #형준180815
        # * numpy.log(abs(G_Result4) + 1) ** 0.5 * numpy.sign(G_Result4))
        # print(type(EVAL_2),type(EVAL_TOTAL))

        '''
        if G_Result1 > 0 and G_Abs_Result > 0:
            EVAL_TOTAL = 10*(10+numpy.log(G_Result2 * G_Result3))
        else:
            print(' * EVAL_TOTAL = 0 나옴')
            EVAL_TOTAL = 0
        '''

        if timing_20 > 0:
            if timing_40 > 0:
                if timing_60 > 0:
                    if EVAL_2 > 5:
                        STATUS = timing_list[0]
                    else:
                        STATUS = timing_list[1]
                else:
                    STATUS = timing_list[1]
            else:
                if timing_60 > 0:
                    STATUS = timing_list[1]
                else:
                    STATUS = timing_list[2]
        else:
            if timing_40 > 0:
                if timing_60 > 0:
                    STATUS = timing_list[1]
                else:
                    STATUS = timing_list[2]
            else:
                if timing_60 > 0:
                    STATUS = timing_list[2]
                else:
                    STATUS = timing_list[3]


        # print('')
        # print('********* START logic *********')
        # print("1. 일변동: ", list_daydiff)
        # print("1-1. 수정_일변동: ", list_daydiff_adjust)
        # print("2.평균m: ", M_average)
        # print("3.변동성s: ", S_volatility)
        # print("4-1.평가지표G1>  m - s 제곱: ", G_Result1)
        # print("4-2.수익지수> m - s 제곱 / 2: ", G_Result2)
        # print("4-3.샤프지수> m/s: ", G_Result3)
        # print("4-4.켈리지수> m / s 제곱: ", G_Result4)
        # print("5.수익률_점수(EVAL_2)",EVAL_2)
        # print("6.안정성_점수(EVAL_3)",EVAL_3)
        # print("7.종합점수(EVAL_TOTAL)",EVAL_TOTAL)
        # print("8.타이밍지표(TIMING_INDEX)", STATUS)
        # print('********* END logic *********')
        # print('')
        # #input()

        list_result_G = [S_volatility,M_average,G_Result2,G_Result3,G_Result4,EVAL_2,EVAL_3,EVAL_4,STATUS,EVAL_TOTAL]

        return list_result_G
