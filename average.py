'''
Created on 2020年1月30日
@author: HYG
查询均线数据
'''
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')

def read_data():
    sql = """SELECT * FROM stock_deal_histoy ORDER BY trade_date DESC"""
    df = pd.read_sql_query(sql, engine_ts)
    return df

def close_price(df, num):
    # 获取昨天的收盘价
    yesterday_close = df['close'].shift(num).iloc[-1]
    # 输出结果
    # print("yesterday_close: ", yesterday_close)
    return yesterday_close

hit_ts_code = []
'''
df: mysql 查询的数据
num: 计算 num 天内，价格持续大于 5日线，10 日线，20 日线的票
'''
def higher_average_price(df, num):
    last_5_days['5_day_ma'] = df['close'].rolling(window=5).mean()
    last_5_days['10_day_ma'] = df['close'].rolling(window=10).mean()
    last_5_days['20_day_ma'] = df['close'].rolling(window=20).mean()
    for i in range(num):
        print(df['close'].iloc(i))
        print(last_5_days['5_day_ma'].iloc(i))
        # df['close'].iloc(i) > last_5_days['5_day_ma'].iloc(i)
    

if __name__ == '__main__':
    df = read_data()
    # average_price(df, 5)
    # close_price(df, 1)
    higher_average_price(df, 3)
