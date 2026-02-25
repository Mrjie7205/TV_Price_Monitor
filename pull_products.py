import os
import csv
import requests

# ================= 配置读取 (从环境变量获取) =================
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_PRODUCT_TABLE_ID")

CSV_FILE = "products.csv"

# 飞书列名与 CSV 列名映射
# 飞书: 品牌, 型号, 国家, 平台, 链接
# CSV: Brand, Product Name, Country, Platform, Link
FIELD_MAPPING = {
    "品牌": "Brand",
    "型号": "Product Name",
    "国家": "Country",
    "平台": "Platform",
    "链接": "Link"
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

def fetch_feishu_products(token):
    """从飞书多维表格分页拉取所有产品记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    all_records = []
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
                    # 按照映射转换
                    record = {FIELD_MAPPING[k]: fields.get(k) for k in FIELD_MAPPING if k in fields}
                    # 对于链接字段，飞书可能是文本也可能是超链接对象，做简单处理
                    link_data = fields.get("链接")
                    if isinstance(link_data, dict): # 可能是超链接格式
                        record["Link"] = link_data.get("link", "")
                    else:
                        record["Link"] = str(link_data) if link_data else ""
                    
                    all_records.append(record)
                
                has_more = data.get("has_more", False)
                page_token = data.get("page_token")
            else:
                print(f"❌ 拉取记录失败: {result.get('msg')}")
                break
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            break
            
    return all_records

def get_product_key(record):
    """生成唯一组合键，用于比对重复：品牌_型号_国家_平台"""
    brand = str(record.get("Brand") or "").strip()
    model = str(record.get("Product Name") or "").strip()
    country = str(record.get("Country") or "").strip()
    platform = str(record.get("Platform") or "").strip()
    return f"{brand}_{model}_{country}_{platform}".lower()

def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print("❌ 错误: 环境参数缺失，请检查 FEISHU_APP_ID/SECRET/TOKEN 及 FEISHU_PRODUCT_TABLE_ID")
        return

    # 1. 获取本地已有产品的 key
    existing_keys = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # 兼容处理表头可能有空格的情况
            field_names = [fn.strip() for fn in reader.fieldnames] if reader.fieldnames else []
            for row in reader:
                # 重新构建去空格的 row
                clean_row = {k.strip(): v for k, v in row.items()}
                existing_keys.add(get_product_key(clean_row))

    # 2. 获取 token 并拉取飞书数据
    token = get_tenant_access_token()
    if not token: return
    
    print("🚀 正在从飞书拉取待监控产品列表...")
    feishu_products = fetch_feishu_products(token)
    
    # 3. 筛选新增产品
    new_records = []
    for p in feishu_products:
        key = get_product_key(p)
        if key and key not in existing_keys:
            new_records.append(p)
            existing_keys.add(key) # 防止飞书内部重复

    # 4. 追加写入 CSV
    if new_records:
        file_exists = os.path.exists(CSV_FILE)
        # 字段顺序固定
        fieldnames = ["Brand", "Product Name", "Country", "Platform", "Link"]
        
        with open(CSV_FILE, mode='a', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            # 如果文件完全不存在（理论上不会，但做个冗余），写表头
            if not file_exists or os.path.getsize(CSV_FILE) == 0:
                writer.writeheader()
            
            for item in new_records:
                # 整理数据，确保只有我们需要的 5 个字段
                row_to_write = {fn: item.get(fn, "") for fn in fieldnames}
                writer.writerow(row_to_write)
        
        print(f"✨ 成功！发现了 {len(new_records)} 条新监控产品并已追加到 {CSV_FILE}")
    else:
        print("ℹ️ 没有发现新的监控产品。")

if __name__ == "__main__":
    main()
