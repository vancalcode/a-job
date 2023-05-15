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

'''
df: mysql 查询的数据
num: 计算出最近num天内收盘持续大于 5日线,10日线,20日线的票
'''
def higher_average_price(df, num):
    hit_ts_code = []
    average_data = {}
    average_data['5_day_ma'] = df['close'].rolling(window=5).mean().dropna()   
    average_data['10_day_ma'] = df['close'].rolling(window=10).mean().dropna()
    average_data['20_day_ma'] = df['close'].rolling(window=20).mean().dropna()
    for index, row in df.iterrows():
        if index < num and row['close'] > average_data['5_day_ma'].iloc[index] \
        and row['close'] > average_data['10_day_ma'].iloc[index] \
        and row['close'] > average_data['20_day_ma'].iloc[index]: 
            hit_ts_code.append(row['ts_code'])
        if index < num:
            print("index:",index,"日期:", row['trade_date'], 
            "5日均线:", average_data['5_day_ma'].iloc[index], 
            "10日均线:",average_data['10_day_ma'].iloc[index], 
            "20日均线:",average_data['20_day_ma'].iloc[index],
            "收盘价:", row['close'])
    print("hit_ts_code: ",hit_ts_code)

def send_email():
    

if __name__ == '__main__':
    df = read_data()
    higher_average_price(df, 1)
