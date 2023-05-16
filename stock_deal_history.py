'''
Created on 2020年1月30日
@author: HYG
查询股票历史成交信息，写入mysql表中
'''
import time
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')

# 初始化pro接口
pro = ts.pro_api('53e34221244c3d8fa6472df1c1c484b26516be0f7080fc270b0cc164')

def read_data():
    sql = """SELECT * FROM stock_deal_history LIMIT 20"""
    df = pd.read_sql_query(sql, engine_ts)
    return df

def write_data(df):
    res = df.to_sql('stock_deal_history', engine_ts, index=False, if_exists='append', chunksize=5000, index_label='id')
    print(res)

# 批量获取数据
def get_data():
    sql = """SELECT ts_code FROM stock_basic"""
    df = pd.read_sql_query(sql, engine_ts)
    ts_codes = df['ts_code'].tolist()
    for i in range(0, len(ts_codes), 70):
        codes = ','.join(ts_codes[i:i+70])
        start_date = '20000101'
        end_date = '20230815'
        date_list = pd.date_range(start=start_date, end=end_date, freq='2M')
        for j in range(len(date_list)-1):
            start = date_list[j].strftime('%Y%m%d')
            end = date_list[j+1].strftime('%Y%m%d')
            df = pro.daily(**{
                "ts_code": codes,
                "trade_date": "",
                "start_date": start,
                "end_date": end,
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
    # df = read_data()
    # df = get_data()
    # write_data(df)
    get_data()
    # print(get_ts_codes())