import requests
import json

headers = {
    'Accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
    'Cookie': 'Hm_lvt_722143063e4892925903024537075d0d=1684815105; Hm_lvt_929f8b362150b1f77b477230541dbbc2=1684815105; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1682502375,1684815105; u_dpass=ikvJpIx2MXE9DdRBW3d%2FBDCjCjacRhDR%2Ff%2BSLp0bHNokNvZrPSkfbQ6G2pIvu5nz%2FsBAGfA5tlbuzYBqqcUNFA%3D%3D; u_did=40A80DA16E4E48A59B8A879572D35ADB; u_ttype=WEB; u_ukey=A10702B8689642C6BE607730E11E6E4A; u_uver=1.0.0; Hm_lvt_da7579fd91e2c6fa5aeb9d1620a9b333=1684815170; ttype=WEB; user_status=0; user=MDpteF80NzcxNzk3Mzc6Ok5vbmU6NTAwOjQ4NzE3OTczNzo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoxOTo6OjQ3NzE3OTczNzoxNjg0ODE2NDY0Ojo6MTU1MTI4NDQ2MDo2MDQ4MDA6MDoxMDYyOTFhMmI1NjBkMTc5N2YwY2YxMDkyNTY5YjYxOGI6ZGVmYXVsdF80OjA%3D; userid=477179737; u_name=mx_477179737; escapename=mx_477179737; ticket=73af391487d9dd16cc45d37a7558c961; utk=c451193c4a4e85aa52181dafd20b0927; Hm_lpvt_929f8b362150b1f77b477230541dbbc2=1684816762; Hm_lpvt_722143063e4892925903024537075d0d=1684816762; Hm_lpvt_da7579fd91e2c6fa5aeb9d1620a9b333=1684817112; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1684817112; v=A5IqLazGIkG8f1492vQoXl_N5VNxo5-8yLOKaFxruuUw4TzNRDPmTZg32vEv',
    'Referer': 'http://t.10jqka.com.cn/newcircle/user/userPersonal/?from=circle',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.39',
    'X-Requested-With': 'XMLHttpRequest',
    'hexin-v': 'A5IqLazGIkG8f1492vQoXl_N5VNxo5-8yLOKaFxruuUw4TzNRDPmTZg32vEv'
}


def remove_stock():
    response = requests.get(
        'http://t.10jqka.com.cn/newcircle/group/getSelfStockWithMarket/?callback=selfStock&_=1684817111458',
        headers=headers, verify=False)
    # 将返回的字符串转化为json格式
    json_data = json.loads(response.text.replace('selfStock(', '').replace(');', ''))
    # 获取code字段
    for item in json_data['result']:
        # 移除自选
        url = 'http://t.10jqka.com.cn/newcircle/group/modifySelfStock/?op=del&stockcode={}_{}'.format(item['code'],
                                                                                                      item['marketid'])
        requests.post(url, headers=headers, verify=False)
    print("移除成功")


def add_stock(code_list):
    remove_stock()
    for code in code_list:
        # 添加自选
        url = 'http://t.10jqka.com.cn/newcircle/group/modifySelfStock/?callback=modifyStock&op=add&stockcode={}&_=1684817111421'.format(
            code)
        requests.post(url, headers=headers, verify=False)
    print("添加成功")


if __name__ == '__main__':
    remove_stock()
    add_stock()
