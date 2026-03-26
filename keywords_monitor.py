"""
keywords_monitor.py — 全平台新品上市监控
读取 keywords.csv（Brand + Product Name，无尺寸/国家/平台）
→ 遍历所有已攻克平台搜索
→ 严格标题匹配（品牌 + 型号均出现在页面标题中）
→ 提取价格
→ 写入 keywords_prices.csv（与 prices.csv 结构完全一致）

查看结果：筛选 Status=Success 的行，即为该产品已在该平台上市。
"""

import asyncio
import csv
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ── 复用 filler.py 的搜索函数 ────────────────────────────────────────────────
from filler import (
    get_first_result_boulanger, get_first_result_darty, get_first_result_fnac,
    get_first_result_currys, get_first_result_amazon,
    get_first_result_mediamarkt, get_first_result_coolblue,
    handle_currys_cloudflare,
)

# ── 复用 monitor.py 的价格提取函数 ───────────────────────────────────────────
from monitor import (
    get_boulanger_price, get_darty_price, get_fnac_price,
    get_currys_price, get_amazon_price, get_mediamarkt_price, get_coolblue_price,
    amazon_warmup, USER_AGENTS, STEALTH_JS,
)

# ── 配置 ──────────────────────────────────────────────────────────────────────
BASE_DIR            = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_CSV        = os.path.join(BASE_DIR, "keywords.csv")
KEYWORDS_PRICES_CSV = os.path.join(BASE_DIR, "keywords_prices.csv")

PLATFORMS = [
    {"name": "Boulanger",  "country": "FR", "locale": "fr-FR", "tz": "Europe/Paris",
     "search": get_first_result_boulanger,  "price": get_boulanger_price},
    {"name": "Darty",      "country": "FR", "locale": "fr-FR", "tz": "Europe/Paris",
     "search": get_first_result_darty,      "price": get_darty_price},
    {"name": "Fnac",       "country": "FR", "locale": "fr-FR", "tz": "Europe/Paris",
     "search": get_first_result_fnac,       "price": get_fnac_price},
    {"name": "Currys",     "country": "UK", "locale": "en-GB", "tz": "Europe/London",
     "search": get_first_result_currys,     "price": get_currys_price},
    {"name": "Amazon",     "country": "UK", "locale": "en-GB", "tz": "Europe/London",
     "search": get_first_result_amazon,     "price": get_amazon_price},
    {"name": "MediaMarkt", "country": "DE", "locale": "de-DE", "tz": "Europe/Berlin",
     "search": get_first_result_mediamarkt, "price": get_mediamarkt_price},
    {"name": "Coolblue",   "country": "DE", "locale": "de-DE", "tz": "Europe/Berlin",
     "search": get_first_result_coolblue,   "price": get_coolblue_price},
]

# ── 工具函数 ───────────────────────────────────────────────────────────────────

def is_strict_match(title, brand, model):
    """
    严格标题匹配：品牌和型号关键字必须同时出现在页面标题中。
    例：brand="TCL", model="C6K" → "c6k" in "tcl 65c6k qled tv" = True
    子串匹配可自动覆盖所有尺寸规格（65C6K / 55C6K 等均可命中）。
    """
    t = title.lower()
    return brand.lower() in t and model.lower() in t


def load_keywords():
    if not os.path.exists(KEYWORDS_CSV):
        print(f"[错误] 未找到 {KEYWORDS_CSV}，请先创建并填写监控关键词。")
        return []
    keywords = []
    with open(KEYWORDS_CSV, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            brand = (row.get("Brand") or "").strip()
            model = (row.get("Product Name") or "").strip()
            if brand and model:
                keywords.append({"brand": brand, "model": model})
    print(f"已加载 {len(keywords)} 个监控关键词")
    return keywords


def load_historical_prices():
    """从 keywords_prices.csv 读取上次成功价格，用于涨跌趋势对比。"""
    prices = {}
    if not os.path.exists(KEYWORDS_PRICES_CSV):
        return prices
    try:
        with open(KEYWORDS_PRICES_CSV, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row.get("Status") == "Success" and row.get("Price"):
                    try:
                        key = f"{row['Product Name']}_{row['Platform']}"
                        prices[key] = float(row["Price"])
                    except ValueError:
                        pass
    except Exception as e:
        print(f"[提示] 读取历史价格失败: {e}")
    return prices


def log_result(date_str, time_str, brand, model, country, platform,
               price, currency, page_title, status, trend):
    file_exists = os.path.isfile(KEYWORDS_PRICES_CSV)
    header = ["Date", "Time", "Brand", "Product Name", "Country", "Platform",
              "Price", "Currency", "Page Title", "Status", "Price_Trend"]
    try:
        with open(KEYWORDS_PRICES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "Date": date_str, "Time": time_str,
                "Brand": brand, "Product Name": model,
                "Country": country, "Platform": platform,
                "Price": price if price is not None else "",
                "Currency": currency or "",
                "Page Title": page_title,
                "Status": status,
                "Price_Trend": trend,
            })
    except Exception as e:
        print(f"  [错误] 写入 CSV 失败: {e}")


# ── 核心任务单元 ───────────────────────────────────────────────────────────────

def _result(brand, model, country, platform,
            price=None, currency=None, title="",
            status="Not Found", trend="-"):
    return {
        "brand": brand, "model": model,
        "country": country, "platform": platform,
        "price": price, "currency": currency,
        "title": title, "status": status, "trend": trend,
    }


async def search_one(sem, browser, keyword, platform, historical_prices):
    """在单个平台上搜索单个关键词，返回结果字典。"""
    brand     = keyword["brand"]
    model     = keyword["model"]
    plat_name = platform["name"]
    country   = platform["country"]

    async with sem:
        context = None
        try:
            ua = random.choice(USER_AGENTS)
            context = await browser.new_context(
                user_agent=ua,
                viewport={'width': random.choice([1920, 1366, 1440]), 'height': 1080},
                locale=platform["locale"],
                timezone_id=platform["tz"],
            )
            await context.add_init_script(STEALTH_JS)
            page = await context.new_page()

            # Amazon 需要首页预热
            if plat_name == "Amazon":
                await amazon_warmup(page)

            search_kw = f"{brand} {model}"
            print(f"\n  [{plat_name}] 搜索: {search_kw} ...")
            url = await platform["search"](page, search_kw)

            if not url:
                print(f"  [{plat_name}] 未搜到: {search_kw}")
                return _result(brand, model, country, plat_name, status="Not Found")

            # 导航到产品页
            await page.goto(url, wait_until='domcontentloaded', timeout=40000)
            await asyncio.sleep(random.uniform(1.5, 3.0))

            # 反爬检测（Currys / MediaMarkt / Coolblue）
            if any(x in url.lower() for x in ["currys", "mediamarkt", "coolblue"]):
                await handle_currys_cloudflare(page, model)

            page_title = await page.title()

            # 严格标题匹配
            if not is_strict_match(page_title, brand, model):
                print(f"  [{plat_name}] 标题不符: '{page_title[:70]}'")
                return _result(brand, model, country, plat_name,
                               title=page_title, status="Not Found: Title Mismatch")

            # 提取价格
            price_data = await platform["price"](page)
            if not price_data:
                print(f"  [{plat_name}] 找到页面但未提取到价格")
                return _result(brand, model, country, plat_name,
                               title=page_title, status="Failed: Price Not Found")

            price_val, currency = price_data
            hist_key  = f"{model}_{plat_name}"
            old_price = historical_prices.get(hist_key)
            if old_price is None:
                trend = "新上线"
            elif price_val < old_price:
                trend = "降价"
            elif price_val > old_price:
                trend = "涨价"
            else:
                trend = "持平"

            print(f"  [{plat_name}] SUCCESS: {brand} {model} = {currency} {price_val} ({trend})")
            return _result(brand, model, country, plat_name,
                           price=price_val, currency=currency,
                           title=page_title, status="Success", trend=trend)

        except Exception as e:
            msg = str(e)[:80]
            print(f"  [{plat_name}] 异常 ({brand} {model}): {msg}")
            return _result(brand, model, country, plat_name,
                           status=f"Failed: {str(e)[:60]}")
        finally:
            if context:
                await context.close()


# ── 主入口 ─────────────────────────────────────────────────────────────────────

async def run_async():
    keywords = load_keywords()
    if not keywords:
        return

    historical_prices = load_historical_prices()
    total = len(keywords) * len(PLATFORMS)
    print(f"启动关键词监控: {len(keywords)} 个关键词 × {len(PLATFORMS)} 个平台 = {total} 个任务\n")

    async with async_playwright() as p:
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox', '--disable-setuid-sandbox',
            '--disable-infobars', '--disable-dev-shm-usage',
        ]
        try:
            browser = await p.chromium.launch(headless=True, channel="chrome", args=browser_args)
        except Exception:
            browser = await p.chromium.launch(headless=True, args=browser_args)

        sem     = asyncio.Semaphore(3)
        tasks   = [
            search_one(sem, browser, kw, plat, historical_prices)
            for kw in keywords
            for plat in PLATFORMS
        ]
        results = await asyncio.gather(*tasks)
        await browser.close()

    now      = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")

    print("\n写入结果到 keywords_prices.csv ...")
    for r in results:
        log_result(date_str, time_str,
                   r["brand"], r["model"], r["country"], r["platform"],
                   r["price"], r["currency"], r["title"], r["status"], r["trend"])

    success = [r for r in results if r["status"] == "Success"]
    print(f"\n完成！共 {total} 个任务，找到 {len(success)} 条上市记录：")
    for r in success:
        print(f"  ✓ {r['brand']} {r['model']} @ {r['platform']} ({r['country']}): "
              f"{r['currency']} {r['price']} [{r['trend']}]")
    if not success:
        print("  （本次未发现新上市产品）")


def run():
    asyncio.run(run_async())


if __name__ == "__main__":
    run()
