'''
Created on 2020年1月30日
@author: HYG
查询历史成交信息，写入mysql表中
'''
import time
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')

# 初始化pro接口
pro = ts.pro_api('53e34221244c3d8fa6472df1c1c484b26516be0f7080fc270b0cc164')

def write_data(df):
    res = df.to_sql('stock_deal_history_2023', engine_ts, index=False, if_exists='append', chunksize=5000, index_label='id')
    print(res)

# 批量获取历史交易数据
def record_detail_data():
    sql = """SELECT ts_code FROM stock_basic WHERE (symbol > 000000 and symbol < 300000) or (symbol > 600000 and symbol < 680000)"""
    df = pd.read_sql_query(sql, engine_ts)
    ts_codes = df['ts_code'].tolist()
    for i in range(0, len(ts_codes),200):
        codes = ','.join(ts_codes[i:i+200])
        start_date = '20230101'
        end_date = '20230616'
        date_list = pd.date_range(start=start_date, end=end_date, freq='1M')
        for j in range(len(date_list)-1):
            start = date_list[j].strftime('%Y%m%d')
            end = date_list[j+1].strftime('%Y%m%d')
            try:
                df = pro.daily(**{
                   "ts_code": codes,"trade_date": "","start_date": start,"end_date": end,"offset": "","limit": ""
                }, fields=["ts_code","trade_date","open","high","low","close","pre_close","change","pct_chg","vol","amount"
                ])
                write_data(df)
                print("开始记录数据...")
                time.sleep(0.2)
            except Exception as e:
                print(e)

# 每天下午4点半获取收盘数据并入库
def refresh_deal_history():
    sql = """SELECT ts_code FROM stock_basic"""
    df = pd.read_sql_query(sql, engine_ts)
    ts_codes = df['ts_code'].tolist()
    for i in range(0, len(ts_codes), 500):
        codes = ','.join(ts_codes[i:i+500])   
        today = time.strftime('%Y%m%d',time.localtime(time.time()))
        df = pro.daily(**{
            "ts_code": codes,
            "trade_date": "",
            "start_date": today,
            "end_date": today,
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])   
        write_data(df)
        print("开始记录数据...")
        time.sleep(0.3)

if __name__ == '__main__':
    # write_data(df)
    record_detail_data()
    # print(get_ts_codes())
