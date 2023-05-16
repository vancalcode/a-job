'''
Created on 2020年1月30日
@author: HYG
查询均线数据
'''
import pandas as pd
import pymysql
import concurrent.futures

conn = pymysql.connect(host='127.0.0.1', port = 3306, user='root', password='huang2009', database='stocks')
cursor = conn.cursor()
'''
num: 计算出最近num天内收盘持续大于 5日线,10日线,20日线的票

def higher_average_price(num):
    hit_ts_code = []
    sql = """SELECT ts_code FROM stock_basic WHERE symbol > 000000 and symbol < 680000"""
    base_df = pd.read_sql_query(sql, engine_ts)

    for row in base_df.iterrows():
        sql = "SELECT * FROM stock_deal_history_2023 WHERE ts_code = '{}' ORDER BY trade_date DESC".format(row[1]['ts_code'])
        print(sql)
        df = pd.read_sql_query(sql, engine_ts)
        average_data = {'5_day_ma': df['close'].rolling(window=5).mean().dropna(),
                        '10_day_ma': df['close'].rolling(window=10).mean().dropna(),
                        '20_day_ma': df['close'].rolling(window=20).mean().dropna()}

        for index, row in df.iterrows():
            if index < num and row['close'] > average_data['5_day_ma'].iloc[index] \
                    and row['close'] > average_data['10_day_ma'].iloc[index] \
                    and row['close'] > average_data['20_day_ma'].iloc[index]:
                hit_ts_code.append(row['ts_code'])

            if index < num:
                print("index:", index, "日期:", row['trade_date'],
                      "5日均线:", average_data['5_day_ma'].iloc[index],
                      "10日均线:", average_data['10_day_ma'].iloc[index],
                      "20日均线:", average_data['20_day_ma'].iloc[index],
                      "收盘价:", row['close'])
    print("hit_ts_code: ", hit_ts_code)
'''

def improve_update_average():
    sql = """SELECT ts_code FROM stock_basic WHERE (symbol > 000000 and symbol < 300000) or (symbol > 600000 and symbol < 680000)"""
    base_df = pd.read_sql_query(sql, engine_ts)

    def update_row(row):
        sql = "SELECT * FROM stock_deal_history_2023 WHERE ts_code = '{}' ORDER BY trade_date DESC".format(
            row['ts_code'])
        df = pd.read_sql_query(sql, engine_ts)

        average_data = {'5_day_ma': df['close'].rolling(window=5).mean().dropna(),
                        '10_day_ma': df['close'].rolling(window=10).mean().dropna(),
                        '20_day_ma': df['close'].rolling(window=20).mean().dropna()}

        # 遍历数据，获取5日、10日、20日均线值并插入表中

        update_data = []
        total_sql = ""
        for index, row in df.iterrows():
            update = {}
            # update_sql = ""
            if index < len(average_data['5_day_ma']):
                update['5_avg'] = round(average_data['5_day_ma'].iloc[index], 2)
                # update_sql = "Set 5_avg = {}".format(average_data['5_day_ma'].iloc[index])
            if index < len(average_data['10_day_ma']):
                update['10_day_ma'] = round(average_data['10_day_ma'].iloc[index], 2)
                # update_sql = update_sql + ", 10_avg = {}".format(average_data['10_day_ma'].iloc[index])
            if index < len(average_data['20_day_ma']):
                update['20_day_ma'] = round(average_data['20_day_ma'].iloc[index], 2)
                # update_sql = update_sql + ", 20_avg = {}".format(average_data['20_day_ma'].iloc[index])
            # if update != "":
            if update:
                update['id'] = row['id']
                # update_sql = "UPDATE stock_deal_history_2023 {} WHERE id = {};".format(update_sql, row['id'])
                # total_sql += update_sql + "\r\n"
                update_data.append(update)
        print(update_data)


    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for _, row in base_df.iterrows():
            futures.append(executor.submit(update_row, row))
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(e)



if __name__ == '__main__':
    improve_update_average()

