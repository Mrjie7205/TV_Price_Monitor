import os
import csv
import requests

# ================= 配置读取 (从环境变量获取) =================
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_PRODUCT_TABLE_ID")

CSV_FILE = "products.csv"

# 飞书列名映射
# 注意：新增了 "是否监控" 字段
FIELD_MAPPING = {
    "品牌": "Brand",
    "型号": "Product Name",
    "国家": "Country",
    "平台": "Platform",
    "链接": "Link",
    "是否监控": "Is_Active"
}

def get_tenant_access_token():
    """获取飞书租户验证令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            return result.get("tenant_access_token")
        else:
            print(f"❌ 获取 Token 失败: {result.get('msg')}")
    except Exception as e:
        print(f"❌ 请求 Token 异常: {e}")
    return None

def fetch_active_feishu_products(token):
    """从飞书拉取所有标记为“监控中”的产品"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    active_records = []
    page_token = None
    has_more = True
    
    while has_more:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
            
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") == 0:
                data = result.get("data", {})
                items = data.get("items", [])
                for item in items:
                    fields = item.get("fields", {})
                    
                    # 检查“是否监控”状态
                    # 飞书复选框通常为 bool，单选/多选可能为字符串 "是"
                    is_active_val = fields.get("是否监控")
                    is_monitored = False
                    if isinstance(is_active_val, bool):
                        is_monitored = is_active_val
                    elif isinstance(is_active_val, str):
                        is_monitored = (is_active_val == "是")
                    
                    if not is_monitored:
                        continue

                    # 转换基础字段
                    record = {FIELD_MAPPING[k]: fields.get(k) for k in FIELD_MAPPING if k in fields}
                    
                    # 处理链接字段 (兼容超链接对象)
                    link_data = fields.get("链接")
                    if isinstance(link_data, dict):
                        record["Link"] = link_data.get("link", "")
                    else:
                        record["Link"] = str(link_data) if link_data else ""
                    
                    active_records.append(record)
                
                has_more = data.get("has_more", False)
                page_token = data.get("page_token")
            else:
                print(f"❌ 拉取记录失败: {result.get('msg')}")
                break
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            break
            
    return active_records

def get_product_key(record):
    """生成唯一组合键：品牌_型号_国家_平台"""
    brand = str(record.get("Brand") or "").strip().lower()
    model = str(record.get("Product Name") or "").strip().lower()
    country = str(record.get("Country") or "").strip().lower()
    platform = str(record.get("Platform") or "").strip().lower()
    return f"{brand}_{model}_{country}_{platform}"

def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print("❌ 错误: 环境参数缺失")
        return

    # 1. 备份并读取本地现有的 Link (用于保留爬虫已找好的链接)
    local_link_map = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean_row = {k.strip(): v for k, v in row.items()}
                key = get_product_key(clean_row)
                link = clean_row.get("Link", "").strip()
                if link:
                    local_link_map[key] = link

    # 2. 获取飞书最新的“监控中”名单
    token = get_tenant_access_token()
    if not token: return
    
    print("🚀 正在从飞书全量同步监控清单（仅同步标记为‘是’的产品）...")
    feishu_active_list = fetch_active_feishu_products(token)
    
    # 3. 整合数据：以飞书为准，但保留本地已有的 Link
    final_rows = []
    seen_keys = set()
    
    for item in feishu_active_list:
        key = get_product_key(item)
        if not key or key in seen_keys: # 简单去重
            continue
            
        # 如果飞书里没链接，但本地存过，则回填本地链接
        if not item.get("Link"):
            if key in local_link_map:
                item["Link"] = local_link_map[key]
        
        final_rows.append(item)
        seen_keys.add(key)

    # 4. 全量覆盖写入 CSV
    fieldnames = ["Brand", "Product Name", "Country", "Platform", "Link"]
    with open(CSV_FILE, mode='w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in final_rows:
            # 只写入需要的 5 个字段
            writer.writerow({fn: row.get(fn, "") for fn in fieldnames})
    
    print(f"✨ 同步完成！当前共有 {len(final_rows)} 个监控项。")
    print(f"🧹 已自动移除飞书上关闭监控或不再存在的产品。")

if __name__ == "__main__":
    main()
