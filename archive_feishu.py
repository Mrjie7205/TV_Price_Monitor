import os
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter

# ================= Config =================
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "tbl28CxOZgpiTUd4")

# DRY_RUN 模式：只读 + 打印计划，不实际写入/删除任何数据
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")

BASE_URL = "https://open.feishu.cn/open-apis"

ARCHIVED_MARKERS = ("周均", "周均-无数据")


def get_tenant_access_token():
    url = f"{BASE_URL}/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            return result.get("tenant_access_token")
        print(f" 获取 Token 失败: {result.get('msg')}")
    except Exception as e:
        print(f" 请求 Token 异常: {e}")
    return None


def compute_last_week_window():
    """
    返回上周时间窗口的毫秒时间戳：
      [上周一 00:00:00.000 UTC, 上周日 23:59:59.999 UTC]
    """
    now_utc = datetime.now(timezone.utc)
    this_monday = (now_utc - timedelta(days=now_utc.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    last_monday = this_monday - timedelta(days=7)
    last_sunday_end = this_monday - timedelta(milliseconds=1)
    start_ms = int(last_monday.timestamp() * 1000)
    end_ms = int(last_sunday_end.timestamp() * 1000)
    return start_ms, end_ms, last_monday


def fetch_records_in_window(token, start_ms, end_ms):
    """分页拉取飞书价格表，客户端过滤出日期落在窗口内的记录"""
    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    matched = []
    page_token = None
    has_more = True
    total_scanned = 0

    while has_more:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()
            if result.get("code") != 0:
                print(f" 拉取记录失败: {result.get('msg')}")
                break
            data = result.get("data", {})
            items = data.get("items", [])
            total_scanned += len(items)
            for item in items:
                fields = item.get("fields", {})
                date_val = fields.get("日期")
                if date_val is None:
                    continue
                try:
                    date_ms = int(date_val)
                except (TypeError, ValueError):
                    continue
                if start_ms <= date_ms <= end_ms:
                    matched.append({
                        "record_id": item.get("record_id"),
                        "fields": fields,
                    })
            has_more = data.get("has_more", False)
            page_token = data.get("page_token")
        except Exception as e:
            print(f" 网络请求异常: {e}")
            break

    print(f" 扫描总记录数: {total_scanned}, 上周窗口内: {len(matched)} 条")
    return matched


def check_already_archived(records):
    """上周窗口里若已存在周均行，说明已经归档过，应直接退出避免重复"""
    for r in records:
        if r["fields"].get("状态") in ARCHIVED_MARKERS:
            return True
    return False


def _extract_string(fields, key):
    val = fields.get(key)
    if val is None:
        return ""
    return str(val).strip()


def _mode_or_first(values):
    cleaned = [v for v in values if v]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def _first_non_empty(values):
    for v in values:
        if v:
            return v
    return ""


def group_and_build_weekly_rows(records, monday_ms):
    """
    按 (品牌, 型号, 国家, 平台) 分组日级记录，对每组生成 1 条周均行：
      - 有 Success 数据 → 状态=周均, 价格=有效价格平均
      - 全周无 Success 数据 → 状态=周均-无数据, 价格字段不写（兜底保留产品在场感）
    """
    groups = defaultdict(list)
    for r in records:
        f = r["fields"]
        # 防御性过滤：理论上 check_already_archived 已经挡住
        if f.get("状态") in ARCHIVED_MARKERS:
            continue
        key = (
            _extract_string(f, "品牌"),
            _extract_string(f, "型号"),
            _extract_string(f, "国家"),
            _extract_string(f, "平台"),
        )
        groups[key].append(f)

    weekly_rows = []
    has_data_count = 0
    no_data_count = 0

    for (brand, model, country, platform), items in groups.items():
        # "有效价格" 判定：状态==Success 且 价格是有效数字（沿用 monitor.py:206 的潜规则）
        valid_prices = []
        for f in items:
            if f.get("状态") != "Success":
                continue
            price_val = f.get("价格")
            if price_val in (None, ""):
                continue
            try:
                valid_prices.append(float(price_val))
            except (TypeError, ValueError):
                continue

        page_title = _first_non_empty([_extract_string(f, "页面标题") for f in items])
        currency = _mode_or_first([_extract_string(f, "币种") for f in items])

        base = {
            "日期": monday_ms,
            "时间": "weekly",
            "品牌": brand,
            "型号": model,
            "国家": country,
            "平台": platform,
            "页面标题": page_title,
            "币种": currency,
        }

        if valid_prices:
            avg = round(sum(valid_prices) / len(valid_prices), 2)
            row = {**base, "状态": "周均", "价格动态": "周均", "价格": avg}
            has_data_count += 1
        else:
            row = {**base, "状态": "周均-无数据", "价格动态": "无数据"}
            no_data_count += 1

        # 清洗空字段，沿用 sync_feishu.py:106
        row = {k: v for k, v in row.items() if v not in (None, "")}
        weekly_rows.append(row)

    print(f" 聚合结果: 有数据 {has_data_count} 条, 无数据兜底 {no_data_count} 条")
    return weekly_rows


def batch_create_records(token, rows):
    if not rows:
        return True
    if DRY_RUN:
        print(f" [DRY_RUN] 将写入 {len(rows)} 条周均行（未实际发送）")
        for i, row in enumerate(rows[:3]):
            print(f"   样本{i+1}: {row}")
        if len(rows) > 3:
            print(f"   ... 另外 {len(rows) - 3} 条略")
        return True

    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    batch_size = 100
    all_ok = True
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        payload = {"records": [{"fields": r} for r in chunk]}
        try:
            print(f" 写入第 {i // batch_size + 1} 批 ({len(chunk)} 条)...")
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            if result.get("code") == 0:
                print(f" 写入成功: 新增 {len(chunk)} 条周均行")
            else:
                print(f" 写入失败: {result.get('msg')} | 详情: {result}")
                all_ok = False
        except Exception as e:
            print(f" 写入异常: {e}")
            all_ok = False
    return all_ok


def batch_delete_records(token, record_ids):
    if not record_ids:
        return True
    if DRY_RUN:
        print(f" [DRY_RUN] 将删除 {len(record_ids)} 条日级记录（未实际发送）")
        return True

    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_delete"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    batch_size = 500
    all_ok = True
    for i in range(0, len(record_ids), batch_size):
        chunk = record_ids[i:i + batch_size]
        payload = {"records": chunk}
        try:
            print(f" 删除第 {i // batch_size + 1} 批 ({len(chunk)} 条)...")
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            if result.get("code") == 0:
                print(f" 删除成功: 移除 {len(chunk)} 条日级记录")
            else:
                print(f" 删除失败: {result.get('msg')} | 详情: {result}")
                all_ok = False
        except Exception as e:
            print(f" 删除异常: {e}")
            all_ok = False
    return all_ok


def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print(" 错误: 缺少环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_APP_TOKEN / FEISHU_TABLE_ID")
        return

    if DRY_RUN:
        print(" === DRY_RUN 模式: 只读 + 打印计划, 不会写入或删除任何数据 ===")

    start_ms, end_ms, last_monday = compute_last_week_window()
    print(f" 上周窗口起点 (UTC): {last_monday.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   start_ms={start_ms}, end_ms={end_ms}")

    token = get_tenant_access_token()
    if not token:
        return

    records = fetch_records_in_window(token, start_ms, end_ms)
    if not records:
        print(" 上周窗口内无任何记录, 退出")
        return

    if check_already_archived(records):
        print(" 上周已归档 (检测到 状态='周均' 或 '周均-无数据'), 跳过本次执行")
        return

    weekly_rows = group_and_build_weekly_rows(records, start_ms)
    if not weekly_rows:
        print(" 没有可聚合的日级数据, 退出")
        return

    record_ids_to_delete = [r["record_id"] for r in records]

    create_ok = batch_create_records(token, weekly_rows)
    if not create_ok:
        print(" 周均行写入存在失败, 跳过删除步骤以避免数据丢失")
        return

    delete_ok = batch_delete_records(token, record_ids_to_delete)
    if not delete_ok:
        print(" 警告: 部分日级记录删除失败; 下次运行时幂等检查会阻止重复归档, 必要时请手动检查飞书表")
    else:
        print(" 归档完成")


if __name__ == "__main__":
    main()
