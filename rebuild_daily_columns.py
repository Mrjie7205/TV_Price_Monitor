"""
一次性脚本：把 7 列每日价格补填到飞书价格表已有的周均行。

使用场景：archive_feishu.py 已经把历史日级数据压缩成周均行（612 条），
但当时还没有 7 列每日价格字段。此脚本读 prices.csv（日级 source of truth），
match 到飞书里已有的周均行，用 batch_update 补字段。

运行后丢弃，cron 通过新版 archive_feishu.py 自动生成带 7 列的新周均行。
"""
import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "tblHdETxxA0aEdCJ")

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")

CSV_FILE = os.environ.get("CSV_FILE", "prices.csv")

BASE_URL = "https://open.feishu.cn/open-apis"
ARCHIVED_MARKERS = ("周均", "周均-无数据")

WEEKDAY_FIELDS = {
    0: "周一价格",
    1: "周二价格",
    2: "周三价格",
    3: "周四价格",
    4: "周五价格",
    5: "周六价格",
    6: "周日价格",
}


def get_tenant_access_token():
    url = f"{BASE_URL}/auth/v3/tenant_access_token/internal"
    try:
        r = requests.post(url, headers={"Content-Type": "application/json; charset=utf-8"},
                          json={"app_id": APP_ID, "app_secret": APP_SECRET})
        r.raise_for_status()
        result = r.json()
        if result.get("code") == 0:
            return result.get("tenant_access_token")
        print(f" 获取 Token 失败: {result.get('msg')}")
    except Exception as e:
        print(f" 请求 Token 异常: {e}")
    return None


def date_str_to_week_monday_and_weekday(date_str):
    """'2026-04-22' → (week_monday_ms, weekday_int)，与 sync_feishu.py:81 一致的朴素解析"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = dt.weekday()
    monday = dt - timedelta(days=weekday)
    week_monday_ms = int(monday.timestamp() * 1000)
    return week_monday_ms, weekday


def fetch_weekly_rows(token):
    """拉取飞书表所有 状态 ∈ {周均, 周均-无数据} 的行"""
    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    weekly = []
    page_token = None
    has_more = True
    total = 0
    while has_more:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        result = r.json()
        if result.get("code") != 0:
            print(f" 拉取失败: {result.get('msg')}")
            break
        data = result.get("data", {})
        for item in data.get("items", []):
            total += 1
            f = item.get("fields", {})
            if f.get("状态") not in ARCHIVED_MARKERS:
                continue
            try:
                week_ms = int(f.get("日期"))
            except (TypeError, ValueError):
                continue
            weekly.append({
                "record_id": item.get("record_id"),
                "key": (
                    str(f.get("品牌") or "").strip(),
                    str(f.get("型号") or "").strip(),
                    str(f.get("国家") or "").strip(),
                    str(f.get("平台") or "").strip(),
                    week_ms,
                ),
                "status": f.get("状态"),
            })
        has_more = data.get("has_more", False)
        page_token = data.get("page_token")
    print(f" 飞书扫描: 共 {total} 条记录, 周均行 {len(weekly)} 条")
    return weekly


def build_daily_index_from_csv():
    """
    读 prices.csv，构建索引: 5元组 → {weekday: price}
    5元组: (Brand, Product Name, Country, Platform, week_monday_ms)
    同日多条 Success 用 csv 后出现的覆盖前面的（按文件顺序）。
    """
    if not os.path.exists(CSV_FILE):
        print(f" 错误: 找不到 {CSV_FILE}")
        return None

    index = defaultdict(dict)
    success_count = 0
    skipped = 0
    with open(CSV_FILE, "r", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("Status") != "Success":
                continue
            price_str = row.get("Price")
            if not price_str:
                continue
            try:
                price = float(price_str)
            except ValueError:
                skipped += 1
                continue
            date_str = row.get("Date")
            if not date_str:
                skipped += 1
                continue
            try:
                week_ms, weekday = date_str_to_week_monday_and_weekday(date_str)
            except ValueError:
                skipped += 1
                continue
            key = (
                (row.get("Brand") or "").strip(),
                (row.get("Product Name") or "").strip(),
                (row.get("Country") or "").strip(),
                (row.get("Platform") or "").strip(),
                week_ms,
            )
            index[key][weekday] = price  # 覆盖式：同日多条用最后一条
            success_count += 1
    if skipped:
        print(f" csv 跳过 {skipped} 条无效行")
    print(f" csv 解析: {success_count} 条 Success 价格, 覆盖 {len(index)} 个 (产品×周) 组合")
    return index


def build_update_payload(weekly_rows, csv_index):
    """对每个飞书周均行匹配 csv 索引，生成 batch_update 用的字段补丁"""
    updates = []
    matched = 0
    no_match = 0
    no_data_skipped = 0
    for w in weekly_rows:
        if w["status"] == "周均-无数据":
            no_data_skipped += 1
            continue
        days = csv_index.get(w["key"])
        if not days:
            no_match += 1
            continue
        fields = {}
        for wd, price in days.items():
            fields[WEEKDAY_FIELDS[wd]] = price
        if fields:
            updates.append({"record_id": w["record_id"], "fields": fields})
            matched += 1
    print(f" 匹配: {matched} 条周均行将更新, {no_match} 条 csv 找不到对应数据, {no_data_skipped} 条「周均-无数据」行跳过")
    return updates


def batch_update_records(token, updates):
    if not updates:
        print(" 无可更新记录")
        return True
    if DRY_RUN:
        print(f" [DRY_RUN] 将更新 {len(updates)} 条飞书记录（未实际发送）")
        for sample in updates[:3]:
            print(f"   样本: record_id={sample['record_id']}, fields={sample['fields']}")
        if len(updates) > 3:
            print(f"   ... 另外 {len(updates) - 3} 条略")
        return True

    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_update"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    batch_size = 100
    all_ok = True
    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        try:
            r = requests.post(url, headers=headers, json={"records": chunk})
            r.raise_for_status()
            result = r.json()
            if result.get("code") == 0:
                print(f" 第 {i // batch_size + 1} 批更新成功: {len(chunk)} 条")
            else:
                print(f" 第 {i // batch_size + 1} 批更新失败: {result.get('msg')} | {result}")
                all_ok = False
        except Exception as e:
            print(f" 第 {i // batch_size + 1} 批更新异常: {e}")
            all_ok = False
    return all_ok


def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print(" 错误: 缺少环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_APP_TOKEN / FEISHU_TABLE_ID")
        return
    if DRY_RUN:
        print(" === DRY_RUN 模式: 只读 + 打印计划, 不实际更新 ===")
    print(f" 目标表: {TABLE_ID}")

    token = get_tenant_access_token()
    if not token:
        return

    weekly_rows = fetch_weekly_rows(token)
    if not weekly_rows:
        print(" 飞书表没有周均行, 退出")
        return

    csv_index = build_daily_index_from_csv()
    if csv_index is None:
        return

    updates = build_update_payload(weekly_rows, csv_index)
    batch_update_records(token, updates)


if __name__ == "__main__":
    main()
