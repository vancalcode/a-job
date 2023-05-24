import datetime

def get_previous_workday(n):
    count = 1
    date = datetime.date.today()
    while count < n:
        date -= datetime.timedelta(days=1)
        if date.weekday() >= 5: # 如果是周六或周日，则跳过
            continue
        count += 1
    return date.strftime('%Y%m%d')

n = int(input('请输入天数：'))
date = get_previous_workday(n)
print(f'前{n}天的工作日是：{date}')
