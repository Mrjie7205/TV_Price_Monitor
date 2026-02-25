import os
import csv
import requests

# ================= 配置读取 (从环境变量获取) =================
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
TABLE_ID = os.environ.get("FEISHU_PRODUCT_TABLE_ID")

CSV_FILE = "products.csv"

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

def get_product_key(brand, model, country, platform):
    """生成唯一组合键，用于比对重复：品牌_型号_国家_平台"""
    b = str(brand or "").strip().lower()
    m = str(model or "").strip().lower()
    c = str(country or "").strip().lower()
    p = str(platform or "").strip().lower()
    return f"{b}_{m}_{c}_{p}"

def main():
    if not all([APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID]):
        print("❌ 错误: 环境参数缺失，请检查 FEISHU_APP_ID/SECRET/TOKEN 及 FEISHU_PRODUCT_TABLE_ID")
        return

    # 1. 构建本地 Link 字典
    local_links = {}
    if not os.path.exists(CSV_FILE):
        print(f"⚠️ 本地文件 {CSV_FILE} 不存在，跳过回填。")
        return

    with open(CSV_FILE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 兼容处理列名空格
            clean_row = {k.strip(): v for k, v in row.items()}
            link = clean_row.get("Link", "").strip()
            if link:
                key = get_product_key(
                    clean_row.get("Brand"),
                    clean_row.get("Product Name"),
                    clean_row.get("Country"),
                    clean_row.get("Platform")
                )
                local_links[key] = link

    if not local_links:
        print("ℹ️ 本地 CSV 中没有发现有效的链接，无需回填。")
        return

    # 2. 获取 token 并从飞书拉取记录
    token = get_tenant_access_token()
    if not token: return

    print("🚀 正在检查飞书表格，寻找待回填的记录...")
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    records_to_update = []
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
                    record_id = item.get("record_id")
                    fields = item.get("fields", {})
                    
                    # 生成飞书端的 key
                    brand = fields.get("品牌")
                    model = fields.get("型号")
                    country = fields.get("国家")
                    platform = fields.get("平台")
                    
                    # 获取飞书端现有链接
                    feishu_link_data = fields.get("链接")
                    feishu_link = ""
                    if isinstance(feishu_link_data, dict):
                        feishu_link = feishu_link_data.get("link", "")
                    elif feishu_link_data:
                        feishu_link = str(feishu_link_data)

                    fs_key = get_product_key(brand, model, country, platform)
                    
                    # 核心逻辑：飞书链接为空，但本地有链接
                    if not feishu_link.strip() and fs_key in local_links:
                        target_link = local_links[fs_key]
                        records_to_update.append({
                            "record_id": record_id,
                            "fields": {"链接": target_link},
                            "debug_info": f"{brand}|{model}"
                        })
                
                has_more = data.get("has_more", False)
                page_token = data.get("page_token")
            else:
                print(f"❌ 飞书记录拉取失败: {result.get('msg')}")
                break
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            break

    # 3. 执行回填（批量更新）
    if not records_to_update:
        print("ℹ️ 未发现需要回填的飞书记录（链接可能已存在或本地未搜到）。")
        return

    print(f"🔍 发现了 {len(records_to_update)} 条记录需要回填链接。")
    
    batch_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_update"
    
    # 飞书单次批量更新上限为 100
    batch_size = 100
    for i in range(0, len(records_to_update), batch_size):
        chunk = records_to_update[i : i + batch_size]
        # 只保留 API 需要的 record_id 和 fields
        payload = {
            "records": [{"record_id": r["record_id"], "fields": r["fields"]} for r in chunk]
        }
        
        try:
            print(f"⌛ 正在回填第 {i//batch_size + 1} 批次 ({len(chunk)} 条)...")
            res = requests.post(batch_url, headers=headers, json=payload)
            res.raise_for_status()
            res_json = res.json()
            
            if res_json.get("code") == 0:
                for r in chunk:
                    print(f"✅ 成功回填: {r['debug_info']} -> {r['fields']['链接']}")
            else:
                print(f"❌ 批量回填失败: {res_json.get('msg')}")
        except Exception as e:
            print(f"❌ 更新异常: {e}")

    print("✨ 回填任务完成！")

if __name__ == "__main__":
    main()
