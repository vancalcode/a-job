import tushare as ts
print(ts.__version__)


pro = ts.pro_api('53e34221244c3d8fa6472df1c1c484b26516be0f7080fc270b0cc164')
df = pro.trade_cal(exchange='SSE', is_open='1', 
                            start_date='20200101', 
                            end_date='20200401', 
                            fields='cal_date')

print(df.head())
