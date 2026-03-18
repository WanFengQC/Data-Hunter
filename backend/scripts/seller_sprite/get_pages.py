import os
import json
import time
import requests

cookies = {
    'current_guest': 'EYiHg0NJK4gX_251106-095964',
    'k_size': '50',
    '_ga': 'GA1.1.921509791.1762393243',
    'MEIQIA_TRACK_ID': '355HvhtFLSsb3ha6EtaoXI8REvq',
    'MEIQIA_VISIT_ID': '355Hvm1gs4lE4tOTnOmRwYDAyxH',
    'ecookie': 'L5DbLzsfdQi5eXZM_CN',
    't_size': '50',
    't_order_field': 'created_time',
    't_order_flag': '2',
    'p_c_size': '50',
    '_gcl_au': '1.1.1318762765.1773740930',
    'f406f3cf90e7c11a8569': '773812463fbe5203f1dc781974f13535',
    '_fp': '6bdb7315c2b18cd40408207b8baba3a9',
    '_clck': '1flo5tj%5E2%5Eg4g%5E0%5E2190',
    '7ba84d0b95f668d77aff': '8e047504da5b4d64dc49bd1815cc7daa',
    '6b7146199ef392cb4449': '2dc923726476977ee4c5fef1731e30ed',
    'JSESSIONID': 'E02A1A51A5783C39D97A6E6547E59E12',
    '_gaf_fp': '3de85f61cdf23e9c55a66e849ed41c78',
    'rank-login-user': '4255583771dFgIRimZ8koYY1UiJdoXmBWSPbUrJ1DJZ9y+uhp37D/sD2FvHiwEmLwx5HdVPwMX',
    'rank-login-user-info': '"eyJuaWNrbmFtZSI6IlhJQTE3MzI4ODc4ODg5IiwiaXNBZG1pbiI6ZmFsc2UsImFjY291bnQiOiIxNzMqKioqODg4OSIsInRva2VuIjoiNDI1NTU4Mzc3MWRGZ0lSaW1aOGtvWVkxVWlKZG9YbUJXU1BiVXJKMURKWjl5K3VocDM3RC9zRDJGdkhpd0VtTHd4NUhkVlB3TVgifQ=="',
    'Sprite-X-Token': 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiJEM3NPeEZNZC1FV2lxSmowbFBzV3FRIiwiaWF0IjoxNzczNzk3OTI0LCJleHAiOjE3NzM4ODQzMjQsIm5iZiI6MTc3Mzc5Nzg2NCwic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6MTAwMTIxOCwicGkiOjk5NjkyNywibm4iOiJOaXVuaXVkYWRkeSIsInN5cyI6IlNTX0NOIiwiZWQiOiJOIiwicGhuIjoiMTczMjg4Nzg4ODkiLCJlbSI6IlhJQTE3MzI4ODc4ODg5QHNlbGxlcnNwcml0ZS5jb20iLCJtbCI6IlMiLCJlbmQiOjE3ODI4Njk5MjQ1NDJ9.kOqRdtY0BIKqdBtKILcKDPHWL04gLeqOJ-c56yCOwzLLvh38Q_YjLFFQORW3LxZ-f7jA-Lx84sgYMY9At3-urqKJepEOFbNCqVAYZ6P7oDec8qYn4mzNgov9NnKO_Yt2yrtdA2ARAA001ErlqjRmR3EODtUq_Uf-BtIT3GEgs9uNmSGZ4JkY1ybx7doqUAyo_8GY8udECVlga5l6nZXwHa46PJjddTp3UcHxQc3CfGeb60HyU45pziSgr_flW9nc6wTdH8PHr6VcXY9R2drKkQCH62pPR55zWB4gsRqdrDFau9i26CNLEgmyIPluJScPM166l-UeOiujZEG5dgXbUw',
    'ao_lo_to_n': '"4255583771dFgIRimZ8koYY1UiJdoXmL5PnETTbo8PcLJW7m0QbZNY3veWRKVayZStddXEyqUZKsjJNQJUkAfmeT4h9UQdkc0Y1kuNg1p/UnufvsPC4kA="',
    'Hm_lvt_e0dfc78949a2d7c553713cb5c573a486': '1773740929,1773797927',
    'Hm_lpvt_e0dfc78949a2d7c553713cb5c573a486': '1773797927',
    'HMACCOUNT': 'DB02FE95993721CC',
    '_ga_CN0F80S6GL': 'GS2.1.s1773795884$o15$g1$t1773797936$j51$l0$h0',
    '_clsk': 'l631p9%5E1773797959849%5E30%5E1%5El.clarity.ms%2Fcollect',
    '_ga_38NCVF2XST': 'GS2.1.s1773795884$o15$g1$t1773797959$j20$l0$h821635753',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'cache-control': 'no-cache',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://www.sellersprite.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.sellersprite.com/v3/aba-research?rankGrowthType=W1&size=100&page=2&movementMarket=&market=COM&q=&table=ara_202602&reverseType=M&keywordBidMatchType=exact&order%5Bfield%5D=searchfrequencyrank&order%5Bdesc%5D=false',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
    # 'cookie': 'current_guest=EYiHg0NJK4gX_251106-095964; k_size=50; _ga=GA1.1.921509791.1762393243; MEIQIA_TRACK_ID=355HvhtFLSsb3ha6EtaoXI8REvq; MEIQIA_VISIT_ID=355Hvm1gs4lE4tOTnOmRwYDAyxH; ecookie=L5DbLzsfdQi5eXZM_CN; t_size=50; t_order_field=created_time; t_order_flag=2; p_c_size=50; _gcl_au=1.1.1318762765.1773740930; f406f3cf90e7c11a8569=773812463fbe5203f1dc781974f13535; _fp=6bdb7315c2b18cd40408207b8baba3a9; _clck=1flo5tj%5E2%5Eg4g%5E0%5E2190; 7ba84d0b95f668d77aff=8e047504da5b4d64dc49bd1815cc7daa; 6b7146199ef392cb4449=2dc923726476977ee4c5fef1731e30ed; JSESSIONID=E02A1A51A5783C39D97A6E6547E59E12; _gaf_fp=3de85f61cdf23e9c55a66e849ed41c78; rank-login-user=4255583771dFgIRimZ8koYY1UiJdoXmBWSPbUrJ1DJZ9y+uhp37D/sD2FvHiwEmLwx5HdVPwMX; rank-login-user-info="eyJuaWNrbmFtZSI6IlhJQTE3MzI4ODc4ODg5IiwiaXNBZG1pbiI6ZmFsc2UsImFjY291bnQiOiIxNzMqKioqODg4OSIsInRva2VuIjoiNDI1NTU4Mzc3MWRGZ0lSaW1aOGtvWVkxVWlKZG9YbUJXU1BiVXJKMURKWjl5K3VocDM3RC9zRDJGdkhpd0VtTHd4NUhkVlB3TVgifQ=="; Sprite-X-Token=eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiJEM3NPeEZNZC1FV2lxSmowbFBzV3FRIiwiaWF0IjoxNzczNzk3OTI0LCJleHAiOjE3NzM4ODQzMjQsIm5iZiI6MTc3Mzc5Nzg2NCwic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6MTAwMTIxOCwicGkiOjk5NjkyNywibm4iOiJOaXVuaXVkYWRkeSIsInN5cyI6IlNTX0NOIiwiZWQiOiJOIiwicGhuIjoiMTczMjg4Nzg4ODkiLCJlbSI6IlhJQTE3MzI4ODc4ODg5QHNlbGxlcnNwcml0ZS5jb20iLCJtbCI6IlMiLCJlbmQiOjE3ODI4Njk5MjQ1NDJ9.kOqRdtY0BIKqdBtKILcKDPHWL04gLeqOJ-c56yCOwzLLvh38Q_YjLFFQORW3LxZ-f7jA-Lx84sgYMY9At3-urqKJepEOFbNCqVAYZ6P7oDec8qYn4mzNgov9NnKO_Yt2yrtdA2ARAA001ErlqjRmR3EODtUq_Uf-BtIT3GEgs9uNmSGZ4JkY1ybx7doqUAyo_8GY8udECVlga5l6nZXwHa46PJjddTp3UcHxQc3CfGeb60HyU45pziSgr_flW9nc6wTdH8PHr6VcXY9R2drKkQCH62pPR55zWB4gsRqdrDFau9i26CNLEgmyIPluJScPM166l-UeOiujZEG5dgXbUw; ao_lo_to_n="4255583771dFgIRimZ8koYY1UiJdoXmL5PnETTbo8PcLJW7m0QbZNY3veWRKVayZStddXEyqUZKsjJNQJUkAfmeT4h9UQdkc0Y1kuNg1p/UnufvsPC4kA="; Hm_lvt_e0dfc78949a2d7c553713cb5c573a486=1773740929,1773797927; Hm_lpvt_e0dfc78949a2d7c553713cb5c573a486=1773797927; HMACCOUNT=DB02FE95993721CC; _ga_CN0F80S6GL=GS2.1.s1773795884$o15$g1$t1773797936$j51$l0$h0; _clsk=l631p9%5E1773797959849%5E30%5E1%5El.clarity.ms%2Fcollect; _ga_38NCVF2XST=GS2.1.s1773795884$o15$g1$t1773797959$j20$l0$h821635753',
}

json_data = {
    'rankGrowthType': 'W1',
    'size': 100,
    'page': 1,
    'movementMarket': '',
    'market': 'COM',
    'q': '',
    'table': 'ara_202601',
    'reverseType': 'M',
    'departments': [
        'toys-and-games',
    ],
    'keywordBidMatchType': 'exact',
    'order': {
        'field': 'searchfrequencyrank',
        'desc': False,
    },
}

url = 'https://www.sellersprite.com/v3/api/aba-research'
save_dir = 'ara_202601'
success_flag = '"code":"OK","message":"成功"'

os.makedirs(save_dir, exist_ok=True)

def is_valid_file(file_path):
    if not os.path.exists(file_path):
        return False
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return success_flag in content

def fetch_and_save(page):
    json_data["page"] = page
    response = requests.post(
        url,
        cookies=cookies,
        headers=headers,
        json=json_data
    )
    file_path = os.path.join(save_dir, f'keywords_page_{page}.html')
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    return response.text

# 先确保第1页可用，并解析总页数
page1_file = os.path.join(save_dir, 'keywords_page_1.html')

if is_valid_file(page1_file):
    print("Page 1 已存在且成功，直接读取")
    with open(page1_file, 'r', encoding='utf-8') as f:
        text = f.read()
else:
    print("正在请求 Page 1")
    text = fetch_and_save(1)
    print(f"Saved {page1_file}")

try:
    result = json.loads(text)
    total_pages = result["data"]["pages"]
    print(f"总页数: {total_pages}")
except Exception as e:
    print("无法从第1页解析 total_pages")
    print("错误信息：", e)
    raise

for page in range(1, total_pages + 1):
    file_path = os.path.join(save_dir, f'keywords_page_{page}.html')

    if is_valid_file(file_path):
        print(f"Page {page} 已存在且成功，跳过")
        continue

    print(f"正在请求 Page {page}")
    fetch_and_save(page)
    print(f"Saved {file_path}")

    time.sleep(1)