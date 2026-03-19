import os
import json
import time
import requests
import sys
import random
from datetime import datetime

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
    'f406f3cf90e7c11a8569': '773812463fbe5203f1dc781974f13535',
    '_fp': '6bdb7315c2b18cd40408207b8baba3a9',
    '_clck': '1flo5tj%5E2%5Eg4g%5E0%5E2190',
    '7ba84d0b95f668d77aff': '8e047504da5b4d64dc49bd1815cc7daa',
    '6b7146199ef392cb4449': '2dc923726476977ee4c5fef1731e30ed',
    'Hm_lvt_e0dfc78949a2d7c553713cb5c573a486': '1773740929,1773797927',
    'HMACCOUNT': 'DB02FE95993721CC',
    '_gcl_au': '1.1.1318762765.1773740930.651287507.1773822643.1773822643',
    '_gaf_fp': '7073f7be342b8f0c3b32ba804a94f3a2',
    'rank-login-user': '3720883771/hRwrRCKBLGVx+hTs3aSuSZTx9/ASka6ax9QFnqlokTCq//VtZ8htBcujtKbNMsu',
    'rank-login-user-info': '"eyJuaWNrbmFtZSI6IuaMvemjjueni+i+niIsImlzQWRtaW4iOmZhbHNlLCJhY2NvdW50IjoiMTMyKioqKjY0NjUiLCJ0b2tlbiI6IjM3MjA4ODM3NzEvaFJ3clJDS0JMR1Z4K2hUczNhU3VTWlR4OS9BU2thNmF4OVFGbnFsb2tUQ3EvL1Z0WjhodEJjdWp0S2JOTXN1In0="',
    'Sprite-X-Token': 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiJ6TEpRdndSc25EaFBvYWt5SHlKN3pRIiwiaWF0IjoxNzczODIyNjczLCJleHAiOjE3NzM5MDkwNzMsIm5iZiI6MTc3MzgyMjYxMywic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6MTc2MjUwOSwicGkiOm51bGwsIm5uIjoi5oy96aOO56eL6L6eIiwic3lzIjoiU1NfQ04iLCJlZCI6Ik4iLCJwaG4iOiIxMzI1MjAwNjQ2NSIsImVtIjoiY24zNDM1NDI2NDI1QDE2My5jb20iLCJtbCI6IkcifQ.CIXXn6I0W20eDxJmDMybzLTHSyALwheDZzEfTNJrJRxaSBBwPQ4FLvC0MWpPZxEA2JjTFuh7-ikvsILyBwXYdM8P3n6uf34UaQCo5S6P0FBPopo4XQI-DO1ORHKEMpkB3WZGTfAVxXByMKRvZnOvzn_CSJabtXXAe943Ht50bmSI88arU0YM8ErrDc3-7gJ7EkBL6Ju5uE3-ImhlF365eIl9FOs3B_CpAlMHM0jl3Bs0StKjRoIqG4RH4BCGGRoLD3aOi5xhaOVm55OEwhx5vxpj4TCTXM6SQzZv45kF8yoF4paia-FLoZTdtUaCILKGlNELXrYiLWdK61emzP5H0Q',
    'JSESSIONID': 'BDB1FC97633F9ADA9DD73B460A2CBADE',
    'Hm_lpvt_e0dfc78949a2d7c553713cb5c573a486': '1773822700',
    '_ga_CN0F80S6GL': 'GS2.1.s1773820082$o17$g1$t1773822713$j46$l0$h0',
    '_clsk': 'apai2h%5E1773822814542%5E41%5E1%5Eo.clarity.ms%2Fcollect',
    '_ga_38NCVF2XST': 'GS2.1.s1773820082$o19$g1$t1773822814$j25$l0$h1187695156',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'cache-control': 'no-cache',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://www.sellersprite.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.sellersprite.com/v3/aba-research?rankGrowthType=W1&size=100&page=2&movementMarket=&market=COM&q=&table=ara_202601&reverseType=M&keywordBidMatchType=exact&order%5Bfield%5D=searchfrequencyrank&order%5Bdesc%5D=false',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
    # 'cookie': 'current_guest=EYiHg0NJK4gX_251106-095964; k_size=50; _ga=GA1.1.921509791.1762393243; MEIQIA_TRACK_ID=355HvhtFLSsb3ha6EtaoXI8REvq; MEIQIA_VISIT_ID=355Hvm1gs4lE4tOTnOmRwYDAyxH; ecookie=L5DbLzsfdQi5eXZM_CN; t_size=50; t_order_field=created_time; t_order_flag=2; p_c_size=50; f406f3cf90e7c11a8569=773812463fbe5203f1dc781974f13535; _fp=6bdb7315c2b18cd40408207b8baba3a9; _clck=1flo5tj%5E2%5Eg4g%5E0%5E2190; 7ba84d0b95f668d77aff=8e047504da5b4d64dc49bd1815cc7daa; 6b7146199ef392cb4449=2dc923726476977ee4c5fef1731e30ed; Hm_lvt_e0dfc78949a2d7c553713cb5c573a486=1773740929,1773797927; HMACCOUNT=DB02FE95993721CC; _gcl_au=1.1.1318762765.1773740930.651287507.1773822643.1773822643; _gaf_fp=7073f7be342b8f0c3b32ba804a94f3a2; rank-login-user=3720883771/hRwrRCKBLGVx+hTs3aSuSZTx9/ASka6ax9QFnqlokTCq//VtZ8htBcujtKbNMsu; rank-login-user-info="eyJuaWNrbmFtZSI6IuaMvemjjueni+i+niIsImlzQWRtaW4iOmZhbHNlLCJhY2NvdW50IjoiMTMyKioqKjY0NjUiLCJ0b2tlbiI6IjM3MjA4ODM3NzEvaFJ3clJDS0JMR1Z4K2hUczNhU3VTWlR4OS9BU2thNmF4OVFGbnFsb2tUQ3EvL1Z0WjhodEJjdWp0S2JOTXN1In0="; Sprite-X-Token=eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiJ6TEpRdndSc25EaFBvYWt5SHlKN3pRIiwiaWF0IjoxNzczODIyNjczLCJleHAiOjE3NzM5MDkwNzMsIm5iZiI6MTc3MzgyMjYxMywic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6MTc2MjUwOSwicGkiOm51bGwsIm5uIjoi5oy96aOO56eL6L6eIiwic3lzIjoiU1NfQ04iLCJlZCI6Ik4iLCJwaG4iOiIxMzI1MjAwNjQ2NSIsImVtIjoiY24zNDM1NDI2NDI1QDE2My5jb20iLCJtbCI6IkcifQ.CIXXn6I0W20eDxJmDMybzLTHSyALwheDZzEfTNJrJRxaSBBwPQ4FLvC0MWpPZxEA2JjTFuh7-ikvsILyBwXYdM8P3n6uf34UaQCo5S6P0FBPopo4XQI-DO1ORHKEMpkB3WZGTfAVxXByMKRvZnOvzn_CSJabtXXAe943Ht50bmSI88arU0YM8ErrDc3-7gJ7EkBL6Ju5uE3-ImhlF365eIl9FOs3B_CpAlMHM0jl3Bs0StKjRoIqG4RH4BCGGRoLD3aOi5xhaOVm55OEwhx5vxpj4TCTXM6SQzZv45kF8yoF4paia-FLoZTdtUaCILKGlNELXrYiLWdK61emzP5H0Q; JSESSIONID=BDB1FC97633F9ADA9DD73B460A2CBADE; Hm_lpvt_e0dfc78949a2d7c553713cb5c573a486=1773822700; _ga_CN0F80S6GL=GS2.1.s1773820082$o17$g1$t1773822713$j46$l0$h0; _clsk=apai2h%5E1773822814542%5E41%5E1%5Eo.clarity.ms%2Fcollect; _ga_38NCVF2XST=GS2.1.s1773820082$o19$g1$t1773822814$j25$l0$h1187695156',
}

json_data = {
    'rankGrowthType': 'W1',
    'size': 100,
    'page': 1,
    'movementMarket': '',
    'market': 'COM',
    'q': '',
    'table': 'ara_202511',
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
save_dir = 'ara_202511'

os.makedirs(save_dir, exist_ok=True)


def pause_program():
    try:
        os.system('pause')
    except Exception:
        try:
            input('程序已暂停，按回车退出...')
        except Exception:
            pass


def stop_program(msg=''):
    if msg:
        print(msg)
    pause_program()
    sys.exit(1)


def parse_json_safe(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None


def is_success_text(text: str) -> bool:
    obj = parse_json_safe(text)
    if not obj:
        return False
    return obj.get('code') == 'OK' and obj.get('message') == '成功'


def is_valid_file(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return is_success_text(content)
    except Exception:
        return False


def print_error_detail(text: str):
    obj = parse_json_safe(text)
    if obj:
        print('返回异常：')
        print(f"code: {obj.get('code')}")
        print(f"message: {obj.get('message')}")
        if 'data' in obj:
            print(f"data: {obj.get('data')}")
    else:
        print('返回内容不是合法 JSON，前1000字符如下：')
        print(text[:1000])


def fetch_and_save(page: int) -> str:
    json_data['page'] = page

    try:
        response = requests.post(
            url,
            cookies=cookies,
            headers=headers,
            json=json_data,
            timeout=30
        )
    except Exception as e:
        stop_program(f'请求 Page {page} 失败：{e}')

    text = response.text

    # 只要不是成功，就直接停，不写文件
    if not is_success_text(text):
        print(f'Page {page} 返回不是成功状态，程序停止。')
        print(f'HTTP状态码: {response.status_code}')
        print_error_detail(text)
        stop_program('检测到返回结果不是 {"code":"OK","message":"成功"}，已暂停。')

    file_path = os.path.join(save_dir, f'keywords_page_{page}.html')
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(text)

    return text


def get_total_pages(text: str) -> int:
    obj = parse_json_safe(text)
    if not obj:
        stop_program('第1页返回不是合法 JSON，无法解析总页数。')

    try:
        return obj['data']['pages']
    except Exception as e:
        stop_program(f'无法从第1页解析 total_pages：{e}')


def main():
    page1_file = os.path.join(save_dir, 'keywords_page_1.html')

    if is_valid_file(page1_file):
        print('Page 1 已存在且成功，直接读取')
        with open(page1_file, 'r', encoding='utf-8') as f:
            text = f.read()
    else:
        print('正在请求 Page 1')
        text = fetch_and_save(1)
        print(f'Saved {page1_file}')

    total_pages = get_total_pages(text)
    print(f'总页数: {total_pages}')

    start_time = datetime.now()
    for page in range(1, total_pages + 1):
        file_path = os.path.join(save_dir, f'keywords_page_{page}.html')

        if is_valid_file(file_path):
            print(f'Page {page} 已存在且成功，跳过')
            continue

        print(f'正在请求 Page {page}')
        fetch_and_save(page)
        print(f'Saved {file_path}')

        random_time = random.uniform(2.0, 5.0)
        print(f'休息{random_time:.2f}s')
        time.sleep(random_time)

    end_time = datetime.now()

    duration = end_time - start_time
    total_seconds = int(duration.total_seconds())

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    print(f'全部完成，共耗时{hours}时{minutes}分{seconds}秒')
    pause_program()


if __name__ == '__main__':
    main()