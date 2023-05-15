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

def average_price(df, num):
    # 计算num日均线
    ma = df['close'].rolling(window=num).mean()
    # 获取第num天的num日均线数据
    average_price = ma.iloc[num-1]
    # 输出结果
    # print("average_price: ", average_price)
    return average_price

hit_ts_code = []
def higher_average_price(df, num):
    # start_day = 1
    # for item in range(num):
    #     print("close_price: ", close_price(df, start_day), "average_price: ",average_price(df, start_day))
    #     if close_price(df, start_day) > average_price(df, start_day):
    #          start_day += 1
    # if start_day - 1 == num: 
    #     print(df['ts_code'][0])


if __name__ == '__main__':
    df = read_data()
    # average_price(df, 5)
    # close_price(df, 1)
    higher_average_price(df, 3)
