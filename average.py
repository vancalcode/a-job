'''
Created on 2020年1月30日
@author: HYG
查询均线数据
'''

import pandas as pd
import date_util
import email_util
import tonghs_util
import re

from sqlalchemy import create_engine

engine_ts = create_engine('mysql+pymysql://root:huang2009@127.0.0.1:3306/stocks?charset=utf8&use_unicode=1')


# 计算日平均线
def higher_average_price(num):
    sql = "SELECT trade_date FROM stock_deal ORDER BY trade_date DESC LIMIT 1;"
    df = pd.read_sql_query(sql, engine_ts)
    max_trade_date = df['trade_date'][0]
    sql = """select b.trade_date from (SELECT trade_date from stock_deal 
            where trade_date <= {} 
            group by trade_date 
            order by trade_date 
            desc limit {}) as b 
            order by b.trade_date asc 
            limit 1;""".format(max_trade_date, num)
    df = pd.read_sql_query(sql, engine_ts)
    min_trade_date = df['trade_date'][0]
    sql = "select * from stock_deal where trade_date >= {} order by trade_date desc".format(min_trade_date)
    df = pd.read_sql_query(sql, engine_ts)
    # 判断 DataFrame 是否为空
    if df.empty:
        print('result is empty')
        return
    # 按照 ts_code 列进行分组
    grouped = df.groupby(['ts_code'])
    get_hit_bull_code(grouped, num)
    # get_hit_stable_code(grouped, num)


def get_hit_bull_code(grouped, num):
    hit_bull_code = []
    # 遍历每个分组
    for name, group in grouped:
        # 最新的成交数据
        newest_item = next(group.itertuples())
        # 平均值
        avg_vol = group['vol'].mean()
        # 过滤成交量不符合要求
        if newest_item.vol < avg_vol * 1.3:
            continue
        # 收盘价在5、10、20均线之上，中间有回踩过20日线，确认过支撑位
        prev_avg_5 = None  # 保存上一个元素的 avg_5 值
        prev_avg_10 = None  # 保存上一个元素的 prev_avg_10 值
        prev_avg_20 = None  # 保存上一个元素的 prev_avg_20 值
        is_decreasing = True  # 是否递减的标志位
        for item in group.itertuples():
            # 均线多头向上
            if (prev_avg_5 is not None and item.avg_5 >= prev_avg_5) or (
                    prev_avg_10 is not None and item.avg_10 >= prev_avg_10) or (
                    prev_avg_20 is not None and item.avg_20 >= prev_avg_20):
                is_decreasing = False
                break
            prev_avg_5 = item.avg_5
            prev_avg_10 = item.avg_10
            prev_avg_20 = item.avg_20

        # 均线多头向上
        if is_decreasing is False:
            continue
        # 最近一天是均线向上,且最低价大于5日线
        if newest_item.avg_20 <= newest_item.avg_10 <= newest_item.avg_5 < newest_item.low:
            for item in group.itertuples():
                # 有回踩过20日线
                if item.low <= item.avg_10:
                    # 使用正则表达式匹配字符串中的数字部分
                    hit_bull_code.append(item.ts_code)
                    break

    if len(hit_bull_code) > 0:
        print("hit_bull_code: ", hit_bull_code)
        # 直接添加自选
        tonghs_util.add_stock(hit_bull_code)

        # 发送邮件
        # message = '\n'.join(map(str, hit_bull_code)).replace(',', '').replace("'", "").replace("(", "").replace(")", "")
        # print(message)
        # email_util.send(message, "连续{}天收盘价都在5、10、20日均线之上".format(num))


def get_hit_stable_code(grouped, num):
    hit_stable_code = []
    # 遍历每个分组
    for name, group in grouped:
        # 计算最小值和最大值之间的差值占最小值的百分比
        diff_percent = ((group[['pct_chg']].max() - group[['pct_chg']].min()) / group[['pct_chg']].min() * 100).max()
        if diff_percent > 1:
            continue
        hit = 0
        prev_avg_5 = None  # 保存上一个元素的 avg_5 值
        prev_avg_10 = None  # 保存上一个元素的 prev_avg_10 值
        prev_avg_20 = None  # 保存上一个元素的 prev_avg_20 值
        is_decreasing = True  # 是否递减的标志位
        for item in group.itertuples():
            # 多头向上
            if item.avg_5 >= item.avg_10 >= item.avg_20:
                hit += 1
            if (prev_avg_5 is not None and item.avg_5 >= prev_avg_5) or (
                    prev_avg_10 is not None and item.avg_10 >= prev_avg_10) or (
                    prev_avg_20 is not None and item.avg_20 >= prev_avg_20):
                is_decreasing = False
                break
            prev_avg_5 = item.avg_5
            prev_avg_10 = item.avg_10
            prev_avg_20 = item.avg_20
        if is_decreasing and hit == num:
            hit_stable_code.append(name)
            print(name)
    print("hit_stable_code: ", len(hit_stable_code))


if __name__ == '__main__':
    higher_average_price(5)
