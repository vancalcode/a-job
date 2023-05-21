from email.mime.text import MIMEText
from smtplib import SMTP_SSL


def send(message="", subject="N日均线上方"):
    # 填写真实的发邮件服务器用户名、密码
    user = 'yans67@163.com'
    # 授权码
    password = 'NPHLWLGAHOEFRUKD'
    to_addrs = 'hygcode@163.com'

    # 邮件内容
    msg = MIMEText(message, 'plain', _charset="utf-8")
    # 邮件主题描述
    msg["Subject"] = subject
    msg["from"] = "Robot"
    msg["to"] = "hygcode"
    try:
        with SMTP_SSL(host="smtp.163.com", port=465) as smtp:
            # with SMTP_SSL(host="smtp.qq.com", port=465) as smtp:
            # 登录发邮件服务器
            smtp.login(user=user, password=password)
            # 实际发送、接收邮件配置
            smtp.sendmail(from_addr=user, to_addrs=to_addrs.split(','), msg=msg.as_string())
            print("【{}】邮件发送成功".format(subject))
    except Exception as e:
        print("邮件发送失败：", e)


if __name__ == '__main__':
    message = 'Python 测试邮件...'
    subject = '主题测试'
    send(message, subject)
