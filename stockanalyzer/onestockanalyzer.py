import Analyzer

code = '217730'
company = '강스템바이오텍'

testresult = Analyzer.testing(code,'2018-01-01')
totprofit =0.0
succcnt = 0
losecnt = 0
for h in testresult :
    print(f"{h['매수일']} 매수가:{h['매수가']:7,d} === 청산 => {h['청산일']}:{h['결과']} 보유기간: {h['보유기간']:2d} \
            수익 : {h['수익(%)']:5.2f}")
    if  h['결과'] == '성공'  :
        succcnt += 1 
    else :
        losecnt += 1
    totprofit += h['수익(%)']
print(f"SUB TOTAL : 종목:{company}[{code}] 매매횟수:{len(testresult)} 총수익율(%) :{totprofit:5.2f}")