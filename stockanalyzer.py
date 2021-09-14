import Analyzer

mkdb = Analyzer.MarketDB()
resultset = mkdb.get_all_comp()

gcnt =0
gprofit = 0.0
gsucccnt = 0
glosecnt = 0
gsucccnt_1 = 0
gsucccnt_2 = 0
glosecnt_1 = 0  
glosecnt_2 = 0
for codetuple in resultset:
    code = codetuple[0]
    company = codetuple[1]
    testresult = Analyzer.testing(code,'2018-01-01')
    totprofit =0.0
    succcnt = 0
    losecnt = 0
    for h in testresult :
        # print(f"{h['매수일']} 매수가:{h['매수가']:7,d} === 청산 => {h['청산일']}:{h['결과']} 보유기간: {h['보유기간']:2d} \
        #         수익 : {h['수익(%)']:5.2f}")
        if  h['결과'] == '성공'  :
            succcnt += 1 
            if h['보유기간'] <= 2 :
                gsucccnt_1 +=1
            else :
                gsucccnt_2 +=1
        else :
            losecnt += 1
            if h['보유기간'] <= 2 :
                glosecnt_1 +=1
            else :
                glosecnt_2 +=1

        totprofit += h['수익(%)']
    gcnt     += len(testresult)
    gprofit  += totprofit
    gsucccnt += succcnt
    glosecnt += losecnt
    print(f"SUB TOTAL : 종목:{company}[{code}] 매매횟수:{len(testresult)} 총수익율(%) :{totprofit:5.2f}")
    # print(f"TOTAL :  매매횟수:{gcnt} 성공율:{gsucccnt}/{glosecnt}({gsucccnt/gcnt*100:5.2f}) 총수익율(%) :{gprofit:5.2f} 건당수익율:{gprofit/gcnt:5.2f}        \r",end='')  
print(f"TOTAL :  매매횟수:{gcnt} 성공율:{gsucccnt}/{glosecnt}({gsucccnt/gcnt*100:5.2f}) 총수익율(%) :{gprofit:5.2f} 건당수익율:{gprofit/gcnt:5.2f}")  
print(f"\ngsucccnt_1:{gsucccnt_1} gsucccnt_2:{gsucccnt_2} glosecnt_1:{glosecnt_1} glosecnt_2:{glosecnt_2}")