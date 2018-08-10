
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





class STManager:


    # 1. 기록할 테이블 만들기
    # 2. 데이터 가져오기
    # 3. 연산작업 해서 상태를 알려줌
    # 4. 데이터를 기록

    buy = 'buy'
    sell = 'sell'
    hold = 'hold'
    status = ''

    def checkStatus(self, d60, d40, d20):

        if d60 > 0 and d40 > 0 and d20 > 0:
            self.status = self.buy
        elif d60 < 0 and d40 > 0 and d20 > 0:
            self.status = self.buy
        elif d60 > 0 and d40 < 0 and d20 > 0:
            self.status = self.hold
        elif d60 < 0 and d40 < 0 and d20 > 0:
            self.status = self.hold

        elif d60 > 0 and d40 > 0 and d20 < 0:
            self.status = self.hold
        elif d60 < 0 and d40 > 0 and d20 < 0:
            self.status = self.sell
        elif d60 > 0 and d40 < 0 and d20 < 0:
            self.status = self.sell
        elif d60 < 0 and d40 < 0 and d20 < 0:
            self.status = self.sell
        else:
            self.status = self.hold

        return self.status