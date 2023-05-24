import datetime

# 计算前几个工作日的日期，这里会过滤周末，但其实还是有问题，因为还有节假日影响
def get_previous_workday(n):
    count = 1
    date = datetime.date.today()
    # 如果计算的当天是周末，需要过滤
    while date.weekday() >= 5:
        date -= datetime.timedelta(days=1)
    while count < n:
        date -= datetime.timedelta(days=1)
        if date.weekday() >= 5: # 如果是周六或周日，则跳过
            continue
        count += 1
    return date.strftime('%Y%m%d')

def get_interval_day(date_str):
    # 将 yyyyMMdd 格式的日期转换为 datetime 对象
    date = datetime.datetime.strptime(date_str, '%Y%m%d')
    # 获取当前时间的 datetime 对象
    now = datetime.datetime.now()
    # 计算两个 datetime 对象之间的差值，并提取出相差的天数
    delta = now - date
    return delta.days