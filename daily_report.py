import os
import csv
from datetime import datetime, timezone, timedelta
import requests
from tavily import TavilyClient
from openai import OpenAI

from sync_feishu import get_tenant_access_token

# ====== 设定东八区时间，以防 GitHub Actions 默认按 UTC 产生日历差 ======
BJ_TZ = timezone(timedelta(hours=8))

def get_internal_data(csv_file="prices.csv"):
    """
    梳理同目录的 prices.csv 数据
    提取今天的降价/涨价数据，以及状态突变（如 Out of Stock）的数据
    """
    notable_price_changes = []
    status_mutations = []

    print(f">>> [内部数据] 正在检查 CSV 文件: {csv_file}")
    if not os.path.exists(csv_file):
         print(">>> [内部数据] 警告：CSV 文件不存在。")
         return "CSV 文件不存在", []

    # 使用东八区时间
    today_str = datetime.now(BJ_TZ).strftime("%Y-%m-%d")
    print(f">>> [内部数据] 获取到当前东八区日期为: {today_str}")
    
    product_history = {}
    fieldnames = ["Date", "Time", "Brand", "Product Name", "Country", "Platform", "Price", "Currency", "Page Title", "Status", "Price_Trend"]
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            f.readline() # 跳过文件头
            reader = csv.DictReader(f, fieldnames=fieldnames)
            for row in reader:
                 if not any(row.values()): 
                     continue
                 key = (row.get("Brand"), row.get("Product Name"), row.get("Platform"), row.get("Country"))
                 if key not in product_history:
                     product_history[key] = []
                 product_history[key].append(row)
                 
        print(f">>> [内部数据] 成功读取 CSV 文件，共 {sum(len(v) for v in product_history.values())} 条历史记录。")
    except Exception as e:
        print(f">>> [内部数据] 读取 CSV 出错: {e}")
        return notable_price_changes, status_mutations

    for key, history in product_history.items():
         today_records = [r for r in history if r.get("Date") == today_str]
         if not today_records:
             continue
         
         # 当天的最新记录
         latest_today = today_records[-1]
         trend = latest_today.get("Price_Trend", "")
         
         # 1. 甄别涨降价
         if trend and ("降价" in trend or "涨价" in trend):
             notable_price_changes.append(latest_today)

         # 2. 甄别状态突变
         if len(history) > 1:
             today_first_idx = history.index(today_records[0])
             if today_first_idx > 0:
                 prev_record = history[today_first_idx - 1]
                 old_status = prev_record.get("Status")
                 new_status = latest_today.get("Status")
                 # 状态发生改变，视为突变
                 if old_status != new_status:
                     status_mutations.append({
                         "key": key,
                         "old_status": old_status,
                         "new_status": new_status,
                         "details": latest_today
                     })

    print(f">>> [内部数据] 分析完毕：发现近期显著调价单品 {len(notable_price_changes)} 个，异常状态突变单品 {len(status_mutations)} 个。")
    return notable_price_changes, status_mutations


def get_external_news():
    """
    抓取过去 24 小时的欧洲电视与家电行业新闻
    """
    print(">>> [外部资讯] 准备抓取外部行业新闻...")
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        print(">>> [外部资讯] ❌ 未配置 TAVILY_API_KEY，跳过新闻抓取。")
        return "未配置 TAVILY_API_KEY，无法获取外部新闻。"
        
    try:
        print(">>> [外部资讯] 正在建立 TavilyClient 连接并发送查询...")
        client = TavilyClient(api_key=api_key)
        query = "过去24小时欧洲电视零售市场动态、电视产品上新与退市、显示面板供应链变动，以及欧洲家电相关的法律法规变化"
        
        # 使用 Tavily 抓取内容
        response = client.search(query=query, search_depth="basic", max_results=5)
        results = response.get("results", [])
        
        if not results:
            print(">>> [外部资讯] ⚠️ 搜索执行成功，但未返回最新相关结果。")
            return "经过搜索，未抓取到任何与家电或面板相关的新闻信息。"
        
        print(f">>> [外部资讯] ✅ 成功拉取到 {len(results)} 条相关新闻信息。")
        snippets = [f"- 标题：{r.get('title')}\n  摘要：{r.get('content')}" for r in results]
        return "\n".join(snippets)
    except Exception as e:
        print(f">>> [外部资讯] ❌ 新闻抓取发生异常：{e}")
        return f"新闻抓取过程发生异常：{e}"


def generate_report(price_data, status_data, news_info):
    """
    调用大模型（DeepSeek）进行核心商业视角的综合分析与生成，输出 JSON 结构
    """
    import json
    print(">>> [AI分析] 准备调用大模型引擎生成分析报告...")
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    # 为了保证健壮性，若无 OpenAI Key，我们直接返回组装的基础字典
    if not api_key:
        print(">>> [AI分析] ⚠️ 未配置 DEEPSEEK_API_KEY，采取 Fallback 返回基础格式数据字典。")
        return {
            "price_report": f"{price_data}\n{status_data}",
            "industry_news": news_info
        }

    try:
        print(">>> [AI分析] 正在建立大模型客户端请求...")
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1" 
        )
        
        system_prompt = (
            "你被设定为一位电视行业资深的市场总监。请依据提供的【内部价格与库存变动数据】与【外部行业新闻】，"
            "生成一份深度但精简的市场监控早报。\n\n"
            "【模块安排】\n"
            "模块一：【价格日报】。请总结提供的竞品价格或库存发生了哪些显著变化，并且你必须结合行业常识推测产生这些变化的原因"
            "（例如：为黑五或特定节日预热、生命周期末端清库策略、汇率剧烈波动、上游面板涨跌影响等）。若我提供给你的内部数据没有显著变化，请直接用精简的一句话概括为：“今日大盘稳定，无显著价格/库存异动”。\n\n"
            "模块二：【行业简讯】。请梳理提供的外部Tavily内容，重点提炼欧洲区域的产品上新、面板行业趋势或相关的法律法规变更。\n\n"
            "【⚠️ 致命限制要求】\n"
            "你必须严格以合法的 JSON 格式输出结果。绝对禁止使用任何 Markdown 代码块包裹（不要输出 ```json 或 ```）。\n"
            "输出的 JSON 结构必须且仅包含以下两个字段：\n"
            "{\n"
            '  "price_report": "生成的价格日报内容...",\n'
            '  "industry_news": "生成的行业简讯内容..."\n'
            "}"
        )
        
        user_prompt = (
             f"【内部数据-价格波动 (今日)】\n{price_data}\n\n"
             f"【内部数据-库存/异常状态突变】\n{status_data}\n\n"
             f"【外部新闻】\n{news_info}"
        )
        
        print(">>> [AI分析] 提示词已组装完毕，等待大模型流返回 (可能需要数秒至十几秒)...")
        # 如果模型有 json_object 模式则开启，兼容性最好
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.7,
            max_tokens=1500
        )
        
        print(">>> [AI分析] ✅ 成功接收大模型返回内容！")
        content = response.choices[0].message.content.strip()
        
        # 针对如果不小心带有代码块时的剔除逻辑
        if content.startswith("```json"):
            content = content[7:]
        elif content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
            
        return json.loads(content)
        
    except json.JSONDecodeError as je:
        print(f">>> [AI分析] ❌ JSON 解析异常：{je}\n原始大模型返回：{content}")
        # 返回部分可用信息
        return {
            "price_report": "⚠️ 大模型解析异常，返回非合法 JSON",
            "industry_news": f"【原始数据兜底】\n{news_info}\n【大模型原始返回】\n{content}"
        }
    except Exception as e:
        print(f">>> [AI分析] ❌ 请求大模型失败：{e}")
        return {
            "price_report": f"请求大模型失败，系统警告：{e}\n\n[原始价格摘要]\n{price_data}",
            "industry_news": f"[原始新闻摘要]\n{news_info}"
        }


def append_to_feishu_bitable(report_dict):
    """
    将生成的报告字典写入飞书多维表格 (Bitable) 中
    """
    import json
    import os
    
    print(">>> [同步飞书] 准备向飞书 Bitable 写入最终报告...")
    app_token = os.environ.get("FEISHU_APP_TOKEN")
    table_id = os.environ.get("FEISHU_REPORT_TABLE_ID")
    
    if not app_token or not table_id:
        print(">>> [同步飞书] ⚠️ 警告：未发现 FEISHU_APP_TOKEN 或 FEISHU_REPORT_TABLE_ID 环境变量，已跳过数据写入环节。")
        return
        
    app_token = app_token.strip()
    table_id = table_id.strip()

    print(">>> [同步飞书] 正在尝试获取飞书应用凭证 Tenant Access Token...")
    token = get_tenant_access_token()
    if not token:
        print(">>> [同步飞书] ❌ 获取凭证失败 (请检查 FEISHU_APP_ID/SECRET)，写入已中止。")
        return

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # 读取字段值
    price_report = report_dict.get("price_report", "分析异常，未提取到报告数据")
    industry_news = report_dict.get("industry_news", "分析异常，未提取到新闻数据")
    
    # 过滤可能存在导致转义失败的控制符
    price_report = price_report.replace('\u0000', '')
    industry_news = industry_news.replace('\u0000', '')

    # 飞书多维表格 Date 字段目前要求毫秒级时间戳，不能输入格式化的字符串
    today_timestamp = int(datetime.now(BJ_TZ).timestamp() * 1000)

    payload = {
        "fields": {
            "日期": today_timestamp,
            "价格日报": price_report,
            "行业简讯": industry_news
        }
    }

    try:
        resp = requests.post(url, headers=headers, json=payload)
        
        # 针对 400/500 等异常状态码全面拦截，不粗暴抛出报错，全量打印排查信息
        if resp.status_code != 200:
            print(f">>> [同步飞书] ❌ HTTP 请求失败，Status Code: {resp.status_code}")
            print(f"--- 飞书 API 报错 Response (resp.text) --- \n{resp.text}\n------------------------------------------")
            print(f"--- 发送的 Payload 数据 --- \n{json.dumps(payload, ensure_ascii=False, indent=2)}\n-------------------------")
            return
            
        result = resp.json()
        if result.get("code") == 0:
            print(">>> [同步飞书] ✅ 成功：大模型商业日报已被推送至飞书 Bitable (多维表格) 中！")
        else:
            print(f">>> [同步飞书] ❌ 失败：写入遇到逻辑错误 ({result.get('code')}): {result.get('msg')}")
            print(f"--- 飞书 API 报错 Response 结构 --- \n{resp.text}\n-----------------------------------")
            print(f"--- 发送的 Payload 数据 --- \n{json.dumps(payload, ensure_ascii=False, indent=2)}\n-------------------------")
    except Exception as e:
        print(f">>> [同步飞书] ❌ 网络接口请求异常：{e}")


def main():
    print("==============================================")
    print(f"--- 开启定时任务节点：生成每日市场概览报告 ---")
    print(f"--- 触发日期：{datetime.now(BJ_TZ).strftime('%Y-%m-%d %H:%M:%S (UTC+8)')} ---")
    print("==============================================\n")
    
    price_changes, status_mutations = get_internal_data("prices.csv")
    
    price_info = "今日内部监控的 SKU 无显著降价或涨价数据记录。"
    if price_changes:
         price_info = "今日发生价格变动的 SKU 列表：\n" + "\n".join(
             [f" - {r['Brand']} {r['Product Name']} [{r['Platform']}-{r['Country']}]: "
              f"最新价格 {r['Price']} {r['Currency']}, 趋势：{r['Price_Trend']}" 
              for r in price_changes]
         )
         
    status_info = "无显著 SKU 在售状态发生突变数据记录。"
    if status_mutations:
         status_info = "今日发生异常状态跨越的 SKU 列表：\n" + "\n".join(
             [f" - {m['key'][0]} {m['key'][1]} [{m['key'][2]}-{m['key'][3]}]: "
              f"从上一状态 '{m['old_status']}' 变为 '{m['new_status']}'" 
              for m in status_mutations]
         )

    news_info = get_external_news()
    report_dict = generate_report(price_info, status_info, news_info)
    
    print("\n----------------- 报告内容预览 -----------------")
    import json
    preview = json.dumps(report_dict, ensure_ascii=False, indent=2)
    if len(preview) > 500:
        print(preview[:500] + "\n... (已折叠剩余部分)")
    else:
        print(preview)
    print("------------------------------------------------\n")

    append_to_feishu_bitable(report_dict)
    
    print("\n--- 今日报告闭环流转完毕，任务退出！ ---")

if __name__ == "__main__":
    main()
