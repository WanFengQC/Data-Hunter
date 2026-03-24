import os
import json
import time
import requests
import sys
import random
import threading
import argparse
import re
from pathlib import Path
from datetime import datetime, timedelta

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
    '7ba84d0b95f668d77aff': '8e047504da5b4d64dc49bd1815cc7daa',
    '6b7146199ef392cb4449': '2dc923726476977ee4c5fef1731e30ed',
    'Hm_lvt_e0dfc78949a2d7c553713cb5c573a486': '1773740929,1773797927',
    'HMACCOUNT': 'DB02FE95993721CC',
    '_gcl_au': '1.1.1318762765.1773740930.651287507.1773822643.1773822643',
    '_clck': '1flo5tj%5E2%5Eg4m%5E0%5E2190',
    'Hm_lpvt_e0dfc78949a2d7c553713cb5c573a486': '1774315021',
    'JSESSIONID': '88D57B7929C180684B5CBB912DC8D4CC',
    '_gaf_fp': 'e7933d24a0e9f1bfa9f41859a1cd5ef5',
    'rank-login-user': '1142834771CE0ZkivBTt/+ELvpbAlm6UZyeeF8F96lhgA3XXetIVaMC4ytVvUUS9n5te8fBR7p',
    'rank-login-user-info': '"eyJuaWNrbmFtZSI6Ik5pdW5pdWRhZGR5MDAxIiwiaXNBZG1pbiI6ZmFsc2UsImFjY291bnQiOiJOaXVuaXVkYWRkeTAwMSIsInRva2VuIjoiMTE0MjgzNDc3MUNFMFpraXZCVHQvK0VMdnBiQWxtNlVaeWVlRjhGOTZsaGdBM1hYZXRJVmFNQzR5dFZ2VVVTOW41dGU4ZkJSN3AifQ=="',
    'Sprite-X-Token': 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiIyUTY4cnpmdUx1N1Voa25OMWxPUHhRIiwiaWF0IjoxNzc0MzI0ODExLCJleHAiOjE3NzQ0MTEyMTEsIm5iZiI6MTc3NDMyNDc1MSwic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6MTAwNTQ0MywicGkiOjk5NjkyNywibm4iOiJOaXVuaXVkYWRkeTAwMSIsInN5cyI6IlNTX0NOIiwiZWQiOiJOIiwiZW0iOiJOaXVuaXVkYWRkeTAwMUBzZWxsZXJzcHJpdGUuY29tIiwibWwiOiJTIiwiZW5kIjoxNzgyODc4NDExNjU0fQ.C6F0CsBo3hPEp4g33dBr2nqlZ1jynMGsnMP-Ve_aHnZu8APc5D4EAahqwZ9F-jAkzqDJ18u8yMTXRuq3DSg7Ec9wMRckJEiCt0VYr55Pd-YAT9H3ZptGEYBZgpz9MbVvygoVJTFQmUes2HyUDeBaDNw1kvxCF35zTm9sjHHlZ6ZAs8_ZRP_-f9-z79JYzjWLdl_ZNEz8ogW9K37xMOzYANsVFn4R7_4q2Y5TipeiKQjzyS1s8YOnhFiQUDzwM3eg14Znjj_Ymq61CVVttbV_h0hxXxym9GkFPNgN9pJdyuifNYWpu2goMXDd20v1ZIEXNJJrOVmZYjsLA629-VYCdg',
    'ao_lo_to_n': '1142834771CE0ZkivBTt/+ELvpbAlm6Vv5hFvN405byIJk0G1TD4u/5cHGcAlewhCt3BoALIVe5/mWML9jaEXn6/9kwvYHzC0jebYYBVOFPZltiD+vEdvO8AJ5fSnzTwHU57EnmfjH',
    '_ga_38NCVF2XST': 'GS2.1.s1774331004$o33$g1$t1774331006$j58$l0$h1210554953',
    '_ga_CN0F80S6GL': 'GS2.1.s1774331004$o27$g1$t1774331009$j55$l0$h0',
    '_clsk': 'fcgf2f%5E1774331013673%5E2%5E0%5Ez.clarity.ms%2Fcollect',
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

BASE_REQUEST_JSON = {
    'rankGrowthType': 'W1',
    'size': 100,
    'page': 1,
    'movementMarket': '',
    'market': 'COM',
    'q': '',
    'table': 'ara_202502',
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
DEFAULT_ARA_BASE_DIR = Path(os.getenv("ARA_BASE_DIR", "D:/ara"))
DEFAULT_TABLE = str(BASE_REQUEST_JSON.get("table") or "ara_202502")
REFERER_TEMPLATE = (
    "https://www.sellersprite.com/v3/aba-research"
    "?rankGrowthType=W1&size=100&page=2&movementMarket=&market=COM&q=&table={table}"
    "&reverseType=M&keywordBidMatchType=exact&order%5Bfield%5D=searchfrequencyrank&order%5Bdesc%5D=false"
)


PAUSE_RETRY_MIN_SECONDS = 20 * 60
PAUSE_RETRY_MAX_SECONDS = 40 * 60
MAX_PAUSE_TIMES = 3
pause_times = 0


class PauseAndRetry(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Batch crawl SellerSprite ABA pages. "
            "Use --tables for multiple tables, e.g. "
            "--tables ara_202502 ara_202501 or --tables ara_202502,ara_202501"
        )
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Table list to crawl. Supports comma or space separated values.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Single table, equivalent to --tables <table>.",
    )
    parser.add_argument(
        "--base-dir",
        default=str(DEFAULT_ARA_BASE_DIR),
        help="Output base directory (default: env ARA_BASE_DIR or D:/ara).",
    )
    return parser.parse_args()


def normalize_table_list(raw_items):
    items = raw_items or []
    output = []
    seen = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        for part in re.split(r"[,\s;，]+", text):
            name = part.strip()
            if not name or name in seen:
                continue
            seen.add(name)
            output.append(name)
    return output


def resolve_target_tables(args):
    if args.table:
        tables = normalize_table_list([args.table])
        if tables:
            return tables
    if args.tables:
        tables = normalize_table_list(args.tables)
        if tables:
            return tables
    env_tables = os.getenv("ARA_TABLES", "")
    if env_tables.strip():
        tables = normalize_table_list([env_tables])
        if tables:
            return tables
    return [DEFAULT_TABLE]


def wait_for_manual_exit():
    try:
        input('程序已完成，按回车退出...')
    except Exception:
        pass


def wait_retry_or_enter(wait_seconds: int) -> bool:
    """
    返回 True 表示用户按回车手动跳过等待；
    返回 False 表示等待结束进入自动重试。
    """
    print('等待期间输入回车可立即重试（本次不计入自动暂停次数）。')
    deadline = time.time() + wait_seconds

    # 统一使用 input，避免某些终端下 msvcrt 无法正确捕获回车
    skip_event = threading.Event()

    def _wait_input():
        try:
            typed = input('请输入回车立即重试（或等待自动重试）: ')
            if typed == '':
                skip_event.set()
        except Exception:
            pass

    threading.Thread(target=_wait_input, daemon=True).start()
    while time.time() < deadline:
        if skip_event.is_set():
            return True
        time.sleep(0.2)
    return False


def handle_pause_retry(reason=''):
    global pause_times

    wait_seconds = random.randint(PAUSE_RETRY_MIN_SECONDS, PAUSE_RETRY_MAX_SECONDS)
    wait_minutes = wait_seconds // 60
    now = datetime.now()
    retry_at = datetime.now() + timedelta(seconds=wait_seconds)

    if reason:
        print(reason)

    print(f'当前时间: {now:%Y-%m-%d %H:%M:%S}')
    next_auto_pause = pause_times + 1
    print(f'程序暂停。若不手动跳过，本次将记为自动暂停第 {next_auto_pause}/{MAX_PAUSE_TIMES} 次。')
    print(f'将在 {wait_minutes} 分钟后自动重试，预计重试时间: {retry_at:%Y-%m-%d %H:%M:%S}')
    skipped = wait_retry_or_enter(wait_seconds)
    if skipped:
        print('检测到回车，已跳过等待并继续执行（本次不计入自动暂停次数）。')
        return

    pause_times += 1
    if pause_times >= MAX_PAUSE_TIMES:
        print(f'自动暂停已达到 {MAX_PAUSE_TIMES} 次，程序自动退出。')
        sys.exit(1)
    print(f'开始自动重试...（已累计自动暂停 {pause_times}/{MAX_PAUSE_TIMES} 次）')


def stop_program(msg=''):
    raise PauseAndRetry(msg or '程序暂停')


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


def is_session_expired_text(text: str) -> bool:
    obj = parse_json_safe(text)
    if not obj:
        return False
    code = str(obj.get('code') or '').strip()
    message = str(obj.get('message') or '').strip()
    return code == 'ERR_GLOBAL_SESSION_EXPIRED' or message == '登录已失效。'


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


def build_request_payload(table_name: str, page: int) -> dict:
    payload = json.loads(json.dumps(BASE_REQUEST_JSON))
    payload['table'] = table_name
    payload['page'] = page
    return payload


def fetch_and_save(table_name: str, page: int, save_dir: str) -> str:
    payload = build_request_payload(table_name, page)
    request_headers = dict(headers)
    request_headers['referer'] = REFERER_TEMPLATE.format(table=table_name)

    try:
        response = requests.post(
            url,
            cookies=cookies,
            headers=request_headers,
            json=payload,
            timeout=30
        )
    except Exception as e:
        stop_program(f'[{table_name}] request page {page} failed: {e}')

    text = response.text

    # Stop immediately if response is not success.
    if not is_success_text(text):
        print(f'[{table_name}] Page {page} returned non-success status, stop now.')
        print(f'HTTP status code: {response.status_code}')
        print_error_detail(text)
        if is_session_expired_text(text):
            print(f'[{table_name}] session expired, exit immediately without retry.')
            sys.exit(1)
        stop_program(f'[{table_name}] response is not {{"code":"OK","message":"??"}}, paused.')

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


def run_once(table_name: str, base_dir: Path):
    save_dir = str(base_dir / table_name)
    os.makedirs(save_dir, exist_ok=True)
    page1_file = os.path.join(save_dir, 'keywords_page_1.html')

    if is_valid_file(page1_file):
        print(f'[{table_name}] Page 1 exists and is valid, reuse it.')
        with open(page1_file, 'r', encoding='utf-8') as f:
            text = f.read()
    else:
        print(f'[{table_name}] requesting Page 1')
        text = fetch_and_save(table_name, 1, save_dir)
        print(f'[{table_name}] Saved {page1_file}')

    total_pages = get_total_pages(text)
    print(f'[{table_name}] total pages: {total_pages}')

    start_time = datetime.now()
    for page in range(1, total_pages + 1):
        file_path = os.path.join(save_dir, f'keywords_page_{page}.html')

        if is_valid_file(file_path):
            print(f'[{table_name}] Page {page} exists and is valid, skip')
            continue

        print(f'[{table_name}] requesting Page {page}')
        fetch_and_save(table_name, page, save_dir)
        print(f'[{table_name}] Saved {file_path}')

        random_time = random.uniform(1.00, 3.00)
        print(f'[{table_name}] sleep {random_time:.2f}s')
        time.sleep(random_time)

    end_time = datetime.now()

    duration = end_time - start_time
    total_seconds = int(duration.total_seconds())

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    print(f'[{table_name}] done, duration {hours}h {minutes}m {seconds}s')


def main():
    global pause_times

    args = parse_args()
    tables = resolve_target_tables(args)
    base_dir = Path(args.base_dir).expanduser().resolve()
    os.makedirs(base_dir, exist_ok=True)

    print(f'Output base dir: {base_dir}')
    print(f'Tables to crawl (in order): {", ".join(tables)}')

    for idx, table_name in enumerate(tables, start=1):
        print(f'================ [{idx}/{len(tables)}] {table_name} ================')
        pause_times = 0
        while True:
            try:
                run_once(table_name, base_dir)
                break
            except PauseAndRetry as exc:
                handle_pause_retry(f'[{table_name}] {exc}')

    print('All table tasks finished.')
    wait_for_manual_exit()


if __name__ == '__main__':
    main()
