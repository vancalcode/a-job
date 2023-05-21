'''
Created on 2020年1月30日
@author: HYG
查询历史成交信息，写入mysql表中
'''
import time
from datetime import datetime, timedelta
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import pymysql
import date_util

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='huang2009', database='stocks')

# 初始化pro接口
pro = ts.pro_api('53e34221244c3d8fa6472df1c1c484b26516be0f7080fc270b0cc164')

def write_data(df):
    res = df.to_sql('stock_deal_history_2023', engine_ts, index=False, if_exists='append', chunksize=5000, index_label='id')
    return res

# 初始化时批量获取历史交易数据（后面不用执行）
def init_detail_data():
    sql = """SELECT ts_code FROM stock_basic WHERE (symbol > 000000 and symbol < 300000) or (symbol > 600000 and symbol < 680000)"""
    df = pd.read_sql_query(sql, engine_ts)
    ts_codes = df['ts_code'].tolist()
    count = 0
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
                count += len(df)
                print("开始记录数据...")
                time.sleep(0.2)
            except Exception as e:
                print(e)
    print("插入总数为: ",count)

# 每天下午4点半获取收盘数据并入库（定时任务）
def refresh_deal_history():
    sql = "SELECT trade_date FROM stock_deal_history_2023 ORDER BY trade_date DESC LIMIT 1"
    df = pd.read_sql_query(sql, engine_ts)
    date = datetime.strptime(df['trade_date'][0], '%Y%m%d')

    next_day = date + timedelta(days=1)
    start_day = next_day.strftime('%Y%m%d')
    today = time.strftime('%Y%m%d',time.localtime(time.time()))
    if start_day > today:
        start_day = today
    
    print("开始更新最新数据...start_day:{},today:{}".format(start_day,today))
    
    sql = """SELECT ts_code FROM stock_basic WHERE (symbol > 000000 and symbol < 300000) or (symbol > 600000 and symbol < 680000)"""
    df = pd.read_sql_query(sql, engine_ts)
    ts_codes = df['ts_code'].tolist()
    for i in range(0, len(ts_codes), 500):
        codes = ','.join(ts_codes[i:i+500])   
        df = pro.daily(**{
            "ts_code": codes,
            "trade_date": "",
            "start_date": start_day,
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
        res = write_data(df)
        print("开始记录最新数据...:{}".format(res))
    print("记录完毕，准备计算日平均数...start_day:{}",start_day)    
    record_average(start_day)

# 计算日平均数
def record_average(start_day):
    print("开始计算日平均数...start_day:{}",start_day)    
    # 先不计算科创板
    sql = """SELECT ts_code FROM stock_basic WHERE (symbol > 000000 and symbol < 300000) or (symbol > 600000 and symbol < 680000)"""
    base_df = pd.read_sql_query(sql, engine_ts)
    intervel_day = date_util.get_intervel_day(start_day)

    for _, base_row in base_df.iterrows():
        # 每次取只要取几十条数据即可
        sql = "SELECT * FROM stock_deal_history_2023 WHERE ts_code = '{}' ORDER BY trade_date DESC limit {}".format(base_row['ts_code'], intervel_day + 20)
        df = pd.read_sql_query(sql, engine_ts)

        average_data = {'5_day_ma': df['close'].rolling(window=5).mean().dropna(),
                        '10_day_ma': df['close'].rolling(window=10).mean().dropna(),
                        '20_day_ma': df['close'].rolling(window=20).mean().dropna()}

        update_data = []
        for index, row in df.iterrows():
            if datetime.strptime(row['trade_date'], '%Y%m%d') < datetime.strptime(start_day, '%Y%m%d'):
                continue
            avg_5 = None
            avg_10 = None
            avg_20 = None
            print("row:",row)
            if index < len(average_data['20_day_ma']) and index < len(average_data['10_day_ma']) and index < len(average_data['5_day_ma']):
                if row['avg_5'] == 0:
                    avg_5 = round(average_data['5_day_ma'].iloc[index], 2)
                if row['avg_10'] == 0:
                    avg_10 = round(average_data['10_day_ma'].iloc[index], 2)
                if row['avg_20'] == 0:
                    avg_20 = round(average_data['20_day_ma'].iloc[index], 2)
                
                update_data.append((avg_5, avg_10, avg_20, row['id']))
        
        if len(update_data) >= 1:
            cursor = conn.cursor()
            sql = "update stock_deal_history_2023 set avg_5 = %s, avg_10 = %s, avg_20 = %s where id = %s"
            res = cursor.executemany(sql, update_data)          
            conn.commit()
            cursor.close()   
            print("==== sql 执行内容:{},res:{}".format(update_data,res))
    print("更新日平均数完毕...关闭数据库链接...")
    conn.close()



# if __name__ == '__main__':
    # caculate_average('20230516')
    # refresh_deal_history()
    # write_data(df)
    # record_detail_data()
    # print(get_ts_codes())
