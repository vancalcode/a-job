'''
Created on 2020年1月30日
@author: JM
调用接口获取数据后写入mysql表中
'''
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')

def read_data():
    sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine_ts)
    return df


def write_data(df):
    res = df.to_sql('stock_basic', engine_ts, index=True, if_exists='append', chunksize=5000, index_label='id')
    print(res)


def get_data():
    pro = ts.pro_api('53e34221244c3d8fa6472df1c1c484b26516be0f7080fc270b0cc164')
    df = pro.stock_basic()
    return df


if __name__ == '__main__':
    # df = read_data()
    df = get_data()
    write_data(df)
    print(df)



