""" variation
이 프로그램은 돌파봉이 발생한날 바로 매수하는것이 아니라..
종목포착 -> 매수 -> 매도  로 3단계 전략을 구사하기 위한 프로그램임
추가조건 : 거래량 150%높이고..매수를 다음날 1%눌림구간에서 해보자 
 """
import pandas as pd
from pandas.core.series import Series
import Analyzer
from datetime import date

def  test2(code, fromdate) :
    mk = Analyzer.MarketDB()
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

        if  df.volume.values[i] > df.V20.values[i] * 1.5 :  # 거래량 20평돌파
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
        #     df.bpb.values[i]     == 'Y'      종가가 볼벤상단선 아래에 위치 disable
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
        df.tgt.values[i] = df.close.values[i] * 1.04

        # 손절가 세팅
        # df.ctp.values[i] = min([df.low.values[i],df.MA20.values[i],df.base.values[i]])
        df.ctp.values[i] = df.MA20.values[i]

    # dataframe을 가볍게 하기 위하여 필요한 칼럼을 제외하고 지운다.
    df.reset_index(drop=True,inplace=True)
    df = df[['date','high','close','confYN','tgt','ctp','MA20','low']]
    df.dropna()

    # 매매결과를 확인하고 손익을 계산한다.
    hitdate = []
    for idx, row in df.iterrows() :
        if row[3] == 'Y' :
            hitdate.append(row[0])

    steps =  0        #  0: 20선돌파봉 발견전, 1:발견후매수대기 , 2:매수완료 매도대기
    statslist = []
    tmpidx = 0
    for idx, row in df.iterrows() :
        if  steps == 0 and row[0] in hitdate  and int(row[6]) * 1.02 < int(row[2]) :  #종가가 20이평보다 2%이상 높게 형성되었을경우
            history = {}
            steps = 1
            history['매수가'] = int(row[2]) * 0.99      #종가매수
            history['목표가'] = int(row[4]) 
            history['손절가'] = int(row[5])
            tmpidx = idx
            continue
        if steps == 1 and idx == tmpidx + 1  and history['매수가'] > int(row[7]):  # 돌파봉종가보다 당일저가가 1%눌림구간을 주면매수
            history['매수일'] = row[0]
            tmpidx = idx
            steps = 2
        if steps == 2 :
            if  history['목표가'] <=  int(row[1]) :
                history['결과'] = '성공'
                history['청산일'] = row[0]
                history['보유기간'] = idx - tmpidx
                history['수익(%)'] = (history['목표가'] - history['매수가'])/history['매수가'] *100
                steps = 0
                statslist.append(history)
            elif  int(row[6]) > int(row[2])*1.005 :   # 20이평보다 종가가 내려가면 매도..0.5P 정도는 매도안하고 봐준다..
                history['결과'] = '실패'
                history['청산일'] = row[0]
                history['보유기간'] = idx - tmpidx
                history['손절가'] = int(row[2])
                history['수익(%)'] = ( history['손절가'] - history['매수가'] )/history['매수가'] *100
                steps = 0
                statslist.append(history)
    return statslist

if __name__ == '__main__':
    mkdb = Analyzer.MarketDB()
    resultset = mkdb.get_all_comp()

    gcnt =0
    gprofit = 0.0
    gsucccnt = 0
    glosecnt = 0
    for codetuple in resultset:
        code = codetuple[0]
        company = codetuple[1]
        testresult = test2(code,'2018-01-01')
        totprofit =0.0
        succcnt = 0
        losecnt = 0
        for h in testresult :
            # print(f"{h['매수일']} 매수가:{h['매수가']:7,d} === 청산 => {h['청산일']}:{h['결과']} 보유기간: {h['보유기간']:2d} \
            #         수익 : {h['수익(%)']:5.2f}")
            if  h['결과'] == '성공'  :
                succcnt += 1 
            else :
                losecnt += 1
            totprofit += h['수익(%)']
        gcnt     += len(testresult)
        gprofit  += totprofit
        gsucccnt += succcnt
        glosecnt += losecnt
        # print(f"SUB TOTAL : 종목:{company}[{code}] 매매횟수:{len(testresult)} 총수익율(%) :{totprofit:5.2f}")
        # print(f"TOTAL :  매매횟수:{gcnt} 성공:{gsucccnt} 실패:{glosecnt} 총수익율(%) :{gprofit:5.2f}                     \r",end='')
        print(f"TOTAL :  매매횟수:{gcnt} 성공율:{gsucccnt}/{glosecnt}({gsucccnt/gcnt*100:5.2f}) 총수익율(%) :{gprofit:5.2f} 건당수익율:{gprofit/gcnt:5.2f}        \r",end='')
    print('\nTest over!')