import os
import csv
import time
import requests
from datetime import datetime

# ================= config =================
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_TABLE_ID")

CSV_FILE = "prices.csv"

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            return result.get("tenant_access_token")
        else:
            print(f" 获取 Token 失败: {result.get('msg')}")
    except Exception as e:
        print(f" 请求 Token 异常: {e}")
    return None

def read_latest_batch(file_path):
    if not os.path.exists(file_path):
        print(f" 文件不存在: {file_path}")
        return []

    rows = []
    # 定义完整表头，处理 CSV 中可能缺失的 Price_Trend 表头
    fieldnames = ["Date", "Time", "Brand", "Product Name", "Country", "Platform", "Price", "Currency", "Page Title", "Status", "Price_Trend"]
    
    with open(file_path, mode='r', encoding='utf-8') as f:
        # 跳过第一行（原始表头），使用自定义完整表头
        f.readline() 
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            # 过滤掉完全为空的行
            if any(row.values()):
                rows.append(row)
    
    if not rows:
        return []

    # 获取最后一行有效的 Date 和 Time
    last_row = rows[-1]
    latest_date = last_row.get("Date")
    latest_time = last_row.get("Time")
    
    if not latest_date or not latest_time:
        print(" 错误: 最后一行数据的 Date 或 Time 为空，无法识别批次。")
        return []

    print(f" 最新数据批次标识: Date={latest_date}, Time={latest_time}")
    
    # 筛选出匹配最新批次的数据
    latest_rows = [r for r in rows if r.get("Date") == latest_date and r.get("Time") == latest_time]
    print(f" 筛选出增量数据: {len(latest_rows)} 条")
    return latest_rows

def format_feishu_fields(row):
    """
    将 CSV 行数据映射并清洗为飞书格式
    """
    fields = {}
    
    # 日期转换：YYYY-MM-DD -> 毫秒级时间戳 (int)
    date_str = row.get("Date")
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # 飞书多维表格日期字段接收毫秒级时间戳
            fields["日期"] = int(dt.timestamp() * 1000)
        except Exception:
            pass

    fields["时间"] = row.get("Time")
    fields["品牌"] = row.get("Brand")
    fields["型号"] = row.get("Product Name")
    fields["国家"] = row.get("Country")
    fields["平台"] = row.get("Platform")
    fields["币种"] = row.get("Currency")
    fields["状态"] = row.get("Status")
    fields["价格动态"] = row.get("Price_Trend")

    # 价格处理
    price_str = row.get("Price")
    if price_str:
        try:
            fields["价格"] = float(price_str)
        except ValueError:
            pass

    # 清洗：移除 None 或空字符串
    cleaned_fields = {k: v for k, v in fields.items() if v is not None and v != ""}
    return cleaned_fields

def batch_push_to_feishu(token, records):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    # 分块推送，飞书单次上限 100 条
    batch_size = 100
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        payload = {
            "records": [{"fields": r} for r in chunk]
        }
        
        try:
            print(f" 正在推送第 {i//batch_size + 1} 批次 ({len(chunk)} 条)...")
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            if result.get("code") == 0:
                print(f" 推送成功: 已新增 {len(chunk)} 条记录")
            else:
                print(f" 推送失败: {result.get('msg')}")
                print(f" 错误详情: {result}")
        except Exception as e:
            print(f" 网络推送异常: {e}")

def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print(" 错误: 请确保环境变量 FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_TOKEN, FEISHU_TABLE_ID 已设置")
        return

    # 1. 获取 Token
    token = get_tenant_access_token()
    if not token:
        return

    # 2. 读取增量数据
    data_to_sync = read_latest_batch(CSV_FILE)
    if not data_to_sync:
        print(" 没有需要同步的数据。")
        return

    # 3. 转换数据格式
    feishu_records = [format_feishu_fields(row) for row in data_to_sync]

    # 4. 批量执行推送
    batch_push_to_feishu(token, feishu_records)

if __name__ == "__main__":
    main()
