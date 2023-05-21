'''
Created on 2020年1月30日
@author: HYG
查询均线数据
'''

import pandas as pd
import date_util
import email_util

from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')


# 计算日平均线
def higher_average_price(num):
    sql = "SELECT trade_date FROM stock_deal_history_2023 ORDER BY trade_date DESC LIMIT 1;"
    df = pd.read_sql_query(sql, engine_ts)
    max_trade_date = df['trade_date'][0]
    sql = """select b.trade_date from (SELECT trade_date from stock_deal_history_2023 
            where trade_date <= {} 
            group by trade_date 
            order by trade_date 
            desc limit {}) as b 
            order by b.trade_date asc 
            limit 1;""".format(max_trade_date, num)
    df = pd.read_sql_query(sql, engine_ts)
    min_trade_date = df['trade_date'][0]

    sql = "select * from stock_deal_history_2023 where trade_date >= {} order by trade_date desc".format(min_trade_date)
    df = pd.read_sql_query(sql, engine_ts)
    hit_ts_code = []
    # 判断 DataFrame 是否为空
    if not df.empty:
        # 按照 ts_code 列进行分组
        grouped = df.groupby(['ts_code'])
        # 遍历每个分组
        for name, group in grouped:
            hit = 0
            deal_amount = 0
            first_item = next(group.itertuples())
            for item in group.itertuples():
                deal_amount += item.vol
                # print("code:{}, close:{},avg_5:{}, avg_10:{}, avg_20:{}".format(item.ts_code, item.close, item.avg_5, item.avg_10, item.avg_20))
                # 均线上方
                if item.close >= item.avg_5 and item.close >= item.avg_10 and item.close >= item.avg_20 and item.avg_5 >= item.avg_10 >= item.avg_20:
                    hit += 1
                else:
                    break
            # 成交量放大，并且连续价格位于均线上分
            if hit == num and first_item.vol > (deal_amount/num):
                hit_ts_code.append(name)
    else:
        print('result is empty')
        
    print("hit_ts_code: ", hit_ts_code)
    if len(hit_ts_code) != 0:
        message = '\n'.join(map(str, hit_ts_code)).replace(',', '').replace("'", "").replace("(", "").replace(")", "")
        print(message)
        email_util.send(message, "连续{}天收盘价都在5、10、20日均线之上".format(num))


if __name__ == '__main__':
    higher_average_price(7)
