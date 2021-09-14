import pandas as pd
from pandas.core.series import Series
import pymysql
from datetime import datetime
from datetime import timedelta
from datetime import date
import re


class MarketDB:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root',
                                    password='arfrom00', db='investar', charset='utf8')
        self.codes = {}
        self.get_comp_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def get_all_comp(self) :
        """company_info 테이블에서 모든레코드를 읽어서 code, company 칼럼을 리스트로 리턴한다"""
        sql = "SELECT * FROM company_info WHERE company NOT LIKE '%스팩%'"
        curs = self.conn.cursor()
        curs.execute(sql)
        rows =  curs.fetchall()
        return rows

    def get_comp_info(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
        sql = "SELECT * FROM company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목의 일별 시세를 데이터프레임 형태로 반환
            - code       : KRX 종목코드('005930') 또는 상장기업명('삼성전자')
            - start_date : 조회 시작일('2020-01-01'), 미입력 시 1년 전 오늘
            - end_date   : 조회 종료일('2020-12-31'), 미입력 시 오늘 날짜
        """
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            # print("start_date is initialized to '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if start_lst[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1900 or start_year > 2200:
                print(f"ValueError: start_year({start_year:d}) is wrong.")
                return
            if start_month < 1 or start_month > 12:
                print(f"ValueError: start_month({start_month:d}) is wrong.")
                return
            if start_day < 1 or start_day > 31:
                print(f"ValueError: start_day({start_day:d}) is wrong.")
                return
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            # print("end_date is initialized to '{}'".format(end_date))
        else:
            end_lst = re.split('\D+', end_date)
            if end_lst[0] == '':
                end_lst = end_lst[1:]
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])
            if end_year < 1800 or end_year > 2200:
                print(f"ValueError: end_year({end_year:d}) is wrong.")
                return
            if end_month < 1 or end_month > 12:
                print(f"ValueError: end_month({end_month:d}) is wrong.")
                return
            if end_day < 1 or end_day > 31:
                print(f"ValueError: end_day({end_day:d}) is wrong.")
                return
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())

        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn't exist.")
        sql = f"SELECT * FROM daily_price WHERE code = '{code}'" \
              f" and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        return df


def  testing(code, fromdate) :
    mk = MarketDB()
    df = mk.get_daily_price(code, fromdate)
    # 기초지표 세팅
    df['MA5']  = df['close'].rolling(window=5).mean()           # 5일이평
    df['MA20'] = df['close'].rolling(window=20).mean()          # 20일이평
    df['stddev'] = df['close'].rolling(window=20).std() 
    df['upper'] = df['MA20'] + (df['stddev'] * 2)               # 볼벤상단값
    df['lower'] = df['MA20'] - (df['stddev'] * 2)               # 볼벤하단값
    df['V20'] = df['volume'].rolling(window=20).mean()          # 거래량20일이평
    df['base'] = ( df['high'].rolling(window=26).max() + df['low'].rolling(window=26).min() ) / 2
    df['rttow'] =  df['low'].rolling(window=9).min()  - ( df['low'].rolling(window=26).min() * 1.005 )
    df = df[['date','open','high','low','close','upper','MA5','MA20','base','volume','V20','rttow']]

    # 전략별 확장칼럼 세팅
    df['bl20_D1'] = Series([], dtype=str)            # 전일 주가가 20일선아래에 있는가
    df['bl20_D5'] = Series([], dtype=str)            # 5일전 주가가 20일선아래에 있는가
    df['bl20_D']  = Series([], dtype=str)            # 당일 주가가 20일선아래에 있는가
    df['upbase']  = Series([], dtype=str)            # 당일 주가가 기준선위에 있는가
    df['bpb']     = Series([], dtype=str)            # 당일 주가가 볼렌져상단선 아래에 있는가
    df['redst']   = Series([], dtype=str)            # 당일 양봉이 발생하였는가
    df['upV20']   = Series([], dtype=str)            # 거래량이 20이평을 돌파하였는가
    df['upclose'] = Series([], dtype=str)            # 전일시가보다 오늘 종가가 높은가
    df['jjMA20']  = Series([], dtype=str)            # 20이평이 상승추세인가
    df['jjMA5']   = Series([], dtype=str)            # 5일이평이 상승추세인가
    df['confYN']  = Series([], dtype=str)            # 모던조건에 부합되는지 여뷰
    df['rttowYN'] = Series([], dtype=str)            # 최근7영업일의 저점 - 최근20영업일의 저점, 양수면 우상향
    df['V20amt']  = Series([], dtype=str)            # 하루평균거래대금 10억이상
    df['up5p']    = Series([], dtype=str)            # 당일상승율5P 이상이면제외

    df['tgt']     = Series([], dtype=int)                # 매수목표가
    df['ctp']     = Series([], dtype=int)                # 손절가


    # 조건에 부합하는지 일일이 체크
    for i in range(len(df.close)) :

        # if df.open.values[i] * 1.03 < df.close.values[i] :  # 당일 상승율이 5%이상이면 제외
        #     df.up5p.values[i] = 'Y'
        # else :
        #     df.up5p.values[i] = 'N'           

        if  (df.close.values[i] + df.open.values[i])/2 < df.MA20.values[i] : 
            df.bl20_D.values[i] = 'N'
        else :
            df.bl20_D.values[i] = 'Y'               
        
        if df.close.values[i] > df.base.values[i] :  # 당일 주가가 기준선위에
            df.upbase.values[i] = 'Y'
        else :
            df.upbase.values[i] = 'N'
        
        if df.close.values[i] < df.upper.values[i] :  # 종가가 볼벤하단에 위치
            df.bpb.values[i] = 'Y'                  
        else :
            df.bpb.values[i] = 'N'

        if  df.open.values[i] < df.close.values[i] :  # 당일양봉
            df.redst.values[i] = 'Y'                  
        else :
            df.redst.values[i] = 'N'    

        if  df.volume.values[i] > df.V20.values[i] :  # 거래량 20평돌파
            df.upV20.values[i] = 'Y'                  
        else :
            df.upV20.values[i] = 'N'    

        if  df.V20.values[i] * df.open.values[i] > 1000000000:  # 평균 하루거래대금 10억이상
            df.V20amt.values[i] = 'Y'                  
        else :
            df.V20amt.values[i] = 'N'    

        if  df.rttow.values[i] > 0  :
            df.rttowYN.values[i] = 'Y'
        else :
            df.rttowYN.values[i] = 'N'
        
    for i in range(len(df.close)-1) :
        if df.close.values[i] < df.MA20.values[i] :  # 전일및 5일전 20이평선아래 주가위치
            df.bl20_D1.values[i+1] = 'Y'             # 전일 20이평선아래 주가위치
        else :
            df.bl20_D1.values[i+1] = 'N'            

        if  df.open.values[i] < df.close.values[i+1] :  # 전일시가보다 높은 종가
            df.upclose.values[i+1] = 'Y'                  
        else :
            df.upclose.values[i+1] = 'N'        

        if  df.MA5.values[i] <= df.MA5.values[i+1]  :  # 5이평우상향
            df.jjMA5.values[i+1] = 'Y'                  
        else :
            df.jjMA5.values[i+1] = 'N'

    for i in range(len(df.close)-2) :
        if  df.MA20.values[i] <= df.MA20.values[i+1] <= df.MA20.values[i+2] :  # 20이평우상향
            df.jjMA20.values[i+2] = 'Y'                  
        else :
            df.jjMA20.values[i+2] = 'N'

    for i in range(len(df.close)-5) :
        if df.close.values[i] < df.MA20.values[i] :  
            df.bl20_D5.values[i+5] = 'Y'             # 5일전 20이평선아래 주가위치
        else :          
            df.bl20_D5.values[i+5] = 'N'            

        #     df.bl20_D1.values[i] == 'Y'      전일주가가 20이평선 아래
        #     df.bl20_D5.values[i] == 'Y'      5일전주가가 20이평선 아래
        #     df.bl20_D.values[i]  == 'Y'      당일주가의 중간(시가+종가/2)점이 20이평위로.
        #     df.upbase.values[i]  == 'Y'      당일 주가가 기준선위에  DISABLE
        #     df.bpb.values[i]     == 'Y'      종가가 볼벤상단선 아래에 위치  DISABLE
        #     df.redst.values[i]   == 'Y'      당일양봉
        #     df.upV20.values[i]   == 'Y'      거래량 20이평 초과
        #     df.V20amt.values[i]  == 'Y'      하루평균거래대금 10억이상
        #     df.upclose.values[i] == 'Y'      전일시가보다 높은 종가
        #     df.rttowYN.values[i] == 'Y'      최근 9일저점 - 최근20일저점*1.005 양수면 우상향
        #     df.up5p.values[i]    == 'Y'      당일상승율5P이상이면제외  DISABLE
        #     df.jjMA20.values[i]  == 'Y'      20이평우상향(연속2회)  DISABLE
        #     df.jjMA5.values[i]   == 'Y'      5이평우상향(연속1회)

    for i in range(len(df.close)) :
        if  df.bl20_D1.values[i] == 'Y'  and  \
            df.bl20_D5.values[i] == 'Y'  and  \
            df.bl20_D.values[i]  == 'Y'  and  \
            df.redst.values[i]   == 'Y'  and  \
            df.upV20.values[i]   =='Y'   and  \
            df.V20amt.values[i]  =='Y'   and  \
            df.upclose.values[i] == 'Y'  and  \
            df.rttowYN.values[i] == 'Y'  and  \
            df.jjMA5.values[i]   == 'Y'    :
            df.confYN.values[i]  = 'Y'
        else :
            df.confYN.values[i] = 'N'

        # 익절가격 세팅
        # if  df.close.values[i] * 1.03 < df.upper.values[i] :
        #     df.tgt.values[i] = df.close.values[i] * 1.03
        # else :    
        #     df.tgt.values[i] = df.upper.values[i]
        df.tgt.values[i] = df.close.values[i] * 1.03

        # 손절가 세팅
        # df.ctp.values[i] = min([df.low.values[i],df.MA20.values[i],df.base.values[i]])
        df.ctp.values[i] = df.MA20.values[i]

    # dataframe을 가볍게 하기 위하여 필요한 칼럼을 제외하고 지운다.
    df.reset_index(drop=True,inplace=True)
    df = df[['date','high','close','confYN','tgt','ctp','MA5','MA20']]
    df.dropna()

    # 매매결과를 확인하고 손익을 계산한다.
    hitdate = []
    for idx, row in df.iterrows() :
        if row[3] == 'Y' :
            hitdate.append(row[0])

    founded = False
    statslist = []
    tmpidx = 0
    for idx, row in df.iterrows() :
        if  not founded and row[0] in hitdate and int(row[6]) * 1.02 < int(row[2]):
            history = {}
            founded = True
            history['매수일'] = row[0]
            history['매수가'] = int(row[2]) 
            history['목표가'] = int(row[4]) 
            history['손절가'] = int(row[5]) 
            tmpidx = idx
            continue
        if founded :
            if  history['목표가'] <=  int(row[1]) :
                history['결과'] = '성공'
                history['청산일'] = row[0]
                history['보유기간'] = idx - tmpidx
                history['수익(%)'] = (history['목표가'] - history['매수가'])/history['매수가'] *100
                founded = False
                statslist.append(history)
            elif  int(row[7]) > int(row[2])*1.005 :   # 20이평보다 종가가 내려가면 매도..0.5P 정도는 매도안하고 봐준다..
                history['결과'] = '실패'
                history['청산일'] = row[0]
                history['보유기간'] = idx - tmpidx
                history['손절가'] = int(row[2])
                history['수익(%)'] = ( history['손절가'] - history['매수가'] )/history['매수가'] *100
                founded = False
                statslist.append(history)
    return statslist
