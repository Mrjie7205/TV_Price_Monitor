import os
import csv
from datetime import datetime
import requests
from tavily import TavilyClient
from openai import OpenAI

from sync_feishu import get_tenant_access_token

def get_internal_data(csv_file="prices.csv"):
    """
    梳理同目录的 prices.csv 数据
    提取今天的降价/涨价数据，以及状态突变（如 Out of Stock）的数据
    """
    notable_price_changes = []
    status_mutations = []

    if not os.path.exists(csv_file):
         return "CSV 文件不存在", []

    today_str = datetime.now().strftime("%Y-%m-%d")
    product_history = {}
    
    # 按照已有格式定义列名
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
    except Exception as e:
        print(f"读取 CSV 出错: {e}")
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

    return notable_price_changes, status_mutations


def get_external_news():
    """
    抓取过去 24 小时的欧洲电视与家电行业新闻
    """
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        return "未配置 TAVILY_API_KEY，无法获取外部新闻。"
        
    try:
        client = TavilyClient(api_key=api_key)
        query = "过去24小时欧洲电视零售市场动态、电视产品上新与退市、显示面板供应链变动，以及欧洲家电相关的法律法规变化"
        
        # 使用 Tavily 抓取内容
        response = client.search(query=query, search_depth="basic", max_results=5)
        results = response.get("results", [])
        
        if not results:
            return "经过搜索，未抓取到任何与家电或面板相关的新闻信息。"
        
        snippets = [f"- 标题：{r.get('title')}\n  摘要：{r.get('content')}" for r in results]
        return "\n".join(snippets)
    except Exception as e:
        return f"新闻抓取过程发生异常：{e}"


def generate_report(price_data, status_data, news_info):
    """
    调用大模型（DeepSeek）进行核心商业视角的综合分析与生成，输出纯文本
    """
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    # 为了保证健壮性，若无 OpenAI Key，我们直接返回组装的基础文本
    if not api_key:
        fallback_msg = "未配置 DEEPSEEK_API_KEY，返回基础版简报。\n\n模块一：【价格日报】\n"
        fallback_msg += price_data + "\n" + status_data + "\n\n模块二：【行业简讯】\n" + news_info
        return fallback_msg

    try:
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1" # 默认指向 DeepSeek 接口，可随时变更为 OpenAI 兼容接口
        )
        
        system_prompt = (
            "你被设定为一位电视行业资深的市场总监。请依据提供的【内部价格与库存变动数据】与【外部行业新闻】，"
            "生成一份深度但精简的市场监控早报。\n\n"
            "【模块安排】\n"
            "模块一：【价格日报】。请总结提供的竞品价格或库存发生了哪些显著变化，并且你必须结合行业常识推测产生这些变化的原因"
            "（例如：为黑五或特定节日预热、生命周期末端清库策略、汇率剧烈波动、上游面板涨跌影响等）。若我提供给你的内部数据没有显著变化，请直接用精简的一句话概括为：“今日大盘稳定，无显著价格/库存异动”。\n\n"
            "模块二：【行业简讯】。请梳理提供的外部Tavily内容，重点提炼欧洲区域的产品上新、面板行业趋势或相关的法律法规变更。\n\n"
            "【⚠️ 致命限制要求】\n"
            "请直接输出纯文本。绝对禁止使用任何 Markdown 语法标记（不可使用 * 、 # 、 ** 、 > 等任何修饰符）。"
            "你的输出必须是干净平铺的文本。所有的段落区隔、强调、列表、标题等，仅能通过纯文本的中文标号和系统自带的回车断行实现。"
        )
        
        user_prompt = (
             f"【内部数据-价格波动 (今日)】\n{price_data}\n\n"
             f"【内部数据-库存/异常状态突变】\n{status_data}\n\n"
             f"【外部新闻】\n{news_info}"
        )
        
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=1500
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"大模型生成日报失败，系统级警告：{e}\n\n[原始数据参考]\n{price_data}\n{news_info}"


def append_to_feishu_docx(content):
    """
    利用 Docx API 将报告追加到指定飞书文档的最末尾
    注意这并不是表格 bitable 追加，而是向文本文档中增加 Block
    """
    doc_id = os.environ.get("FEISHU_DOC_ID")
    if not doc_id:
        print("警告：未配置 FEISHU_DOC_ID 环境变量，将跳过飞书文档回写。")
        return

    # 从现有的 sync_feishu 获取合法的内部租户级 token
    token = get_tenant_access_token()
    if not token:
        print("错误：无法获取飞书 Tenant Access Token，回写中止。")
        return

    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{doc_id}/blocks/{doc_id}/children"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # 当天的日期标题
    today_title = f"{datetime.now().strftime('%Y-%m-%d')} 市场监控日报"

    # 构建三个 Block 的 JSON 数据模型
    payload = {
        "index": -1, # 在文档的末尾追加
        "children": [
            {
                "block_type": 4, # Heading 2
                "heading2": {
                    "elements": [{"text_run": {"content": today_title}}]
                }
            },
            {
                "block_type": 2, # 普通文本 Text Block
                "text": {
                    "elements": [{"text_run": {"content": content}}]
                }
            },
            {
                "block_type": 22, # 分割线 Divider
                "divider": {}
            }
        ]
    }

    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            print(">>> ✅ 成功：大模型商业日报已被追加至指定飞书文档！")
        else:
            print(f">>> ❌ 失败：写入飞书文档遇到错误: {result.get('msg')} ({result.get('code')})")
    except Exception as e:
        print(f">>> ❌ 网络接口请求异常：{e}")


def main():
    print("--- 开始生成每日市场概览报告 ---")
    
    # 步骤一：提取清洗内部运营数据
    print(">>> 正在准备内部数据 (处理 prices.csv) ...")
    price_changes, status_mutations = get_internal_data("prices.csv")
    
    # 数据友好格式化
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

    # 步骤二：Tavily 外网宏观新闻捕捉
    print(">>> 正在使用 Tavily 获取外部行业资讯 ...")
    news_info = get_external_news()

    # 步骤三：DeepSeek 资深视角分析与综合整理
    print(">>> 正在请求大语言模型，生成资深市场视角行业报告 ...")
    report_text = generate_report(price_info, status_info, news_info)
    
    print("\n----------------- 报告预览 -----------------")
    print(report_text[:300] + " ... (隐藏长文)\n" if len(report_text) > 300 else report_text)
    print("--------------------------------------------\n")

    # 步骤四：回推飞书进行沉淀
    print(">>> 准备推送结果至飞书云文档...")
    append_to_feishu_docx(report_text)
    
    print("--- 今日报告闭环流转完毕 ---")

if __name__ == "__main__":
    main()
