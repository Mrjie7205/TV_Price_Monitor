import asyncio
import csv
import os
import random
import re
import time
from datetime import datetime
from urllib.parse import quote
from playwright.async_api import async_playwright

# ================= 配置区域 =================
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "prices.csv")
PRODUCTS_CSV = os.path.join(BASE_DIR, "products.csv")

# ================= 工具函数 =================

def clean_price(text):
    """清洗价格文本 (纯逻辑，不需要async)"""
    if not text: return None
    text = text.strip()
    currency = "EUR"
    if "£" in text or "GBP" in text: currency = "GBP"
    elif "$" in text or "USD" in text: currency = "USD"
    
    clean_text = text.replace("€", "").replace("£", "").replace("$", "").replace("EUR", "").replace("GBP", "").replace("USD", "")
    clean_text = clean_text.replace("\xa0", "").strip()
    
    try:
        if currency == "EUR":
            # 欧式: 2.199,00 或 2 199,00 -> 2199.00
            clean_text = clean_text.replace(" ", "").replace(",", ".")
        else:
            # 英美式: 2,199.00 -> 2199.00
            clean_text = clean_text.replace(",", "").replace(" ", "")

        # 提取第一个数字
        match = re.search(r"(\d+(\.\d+)?)", clean_text)
        if match:
            return float(match.group(1)), currency
    except: pass
    return None

def load_products_from_csv():
    """读取商品列表"""
    products = []
    if not os.path.exists(PRODUCTS_CSV):
        print(f"[提示] 未找到 {PRODUCTS_CSV}，创建模板。")
        with open(PRODUCTS_CSV, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Brand", "Product Name", "Country", "Platform", "Link"])
        return []

    try:
        with open(PRODUCTS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                link = row.get("Link") or row.get("链接") or row.get("url")
                name = row.get("Product Name") or row.get("型号")
                platform = row.get("Platform") or row.get("平台")
                brand = row.get("Brand") or row.get("品牌")
                country = row.get("Country", "FR")
                if not country: country = "FR"

                if name:
                    products.append({
                        "product_name": name.strip(),
                        "url": link.strip() if link else "",
                        "platform": platform.strip() if platform else "",
                        "brand": brand.strip() if brand else "",
                        "country": country.strip().upper()
                    })
    except Exception as e:
        print(f"[错误] 读取 CSV 失败: {e}")
    
    print(f"已加载 {len(products)} 个商品任务")
    return products

def log_price_update(date_str, time_str, brand, name, country, platform, price, currency, page_title, status="Success"):
    """写入 CSV (同步写入，AsyncIO单线程安全)"""
    file_exists = os.path.isfile(CSV_FILE)
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Date", "Time", "Brand", "Product Name", "Country", "Platform", "Price", "Currency", "Page Title", "Status"])
            writer.writerow([date_str, time_str, brand, name, country, platform, price, currency, page_title, status])
            print(f"  [记录] {currency} {price} | Status: {status}")
    except Exception as e:
        print(f"  [错误] 写入 CSV 失败: {e}")

# ================= 爬虫策略函数 (Async) =================

async def get_fnac_price(page):
    selectors = [".f-price", ".userPrice", ".product-price", ".price", "span[class*='price']"]
    for sel in selectors:
        try:
            if await page.is_visible(sel, timeout=2000):
                text = await page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

async def get_darty_price(page):
    selectors = [".product_price", ".price", ".darty_price", "span[class*='price']"]
    for sel in selectors:
        try:
            if await page.is_visible(sel, timeout=2000):
                text = await page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

async def get_boulanger_price(page):
    try:
        price_main = page.locator(".price__amount").first
        if await price_main.is_visible(timeout=3000):
            text = await price_main.inner_text() 
            text = text.replace("\n", ",") 
            result = clean_price(text)
            if result: return result
    except: pass
    
    selectors = [".price", "span[class*='price']"]
    for sel in selectors:
        try:
            if await page.is_visible(sel, timeout=2000):
                text = await page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

async def get_amazon_price(page):
    """
    抓取亚马逊价格 (修复: 优先抓取 Deal Price，避免抓到原价，支持第三方卖家)
    """
    # 策略 1: 明确的 "Price To Pay" 区域 (最准)
    selectors = [
        ".priceToPay .a-offscreen", 
        ".apexPriceToPay .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .priceToPay .a-offscreen",
        "span[data-a-color='price'] .a-offscreen"
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                text = await el.text_content()
                if text:
                    result = clean_price(text)
                    if result: return result
        except: pass

    # 策略 2: 兜底通用 .a-price, 但尝试排除 "原价" (basisPrice)
    try:
        el = page.locator(".a-price:not(.a-text-price) .a-offscreen").first
        if await el.count() > 0:
             text = await el.text_content()
             result = clean_price(text)
             if result: return result
    except: pass
    
    # 策略 3: 检测 "See All Buying Options" (仅第三方卖家)
    # 通常显示为 "from £199.00"
    try:
        # 查找明显的 "查看所有购买选项" 按钮或链接
        if await page.is_visible("a[title*='See All Buying Options']", timeout=500) or \
           await page.is_visible("text=See All Buying Options", timeout=500):
            # 尝试抓取 "from ..." 价格, 通常是 span.a-color-price
            # Amazon 经常在按钮旁边显示最低价
            el = page.locator("span.a-color-price").first
            if await el.count() > 0:
                text = await el.text_content()
                result = clean_price(text)
                if result: 
                    print(f"  [提示] 抓取到第三方卖家起售价: {result}")
                    return result
    except: pass
    
    # 策略 4: 旧版 ID 选择器
    old_selectors = ["#priceblock_ourprice", "#priceblock_dealprice", ".a-price-whole"]
    for sel in old_selectors:
        try:
            if await page.is_visible(sel, timeout=500):
                text = await page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass

    return None

# ================= 导入 Filler =================
try:
    from filler import get_first_result_darty, get_first_result_boulanger, get_first_result_fnac, get_first_result_amazon, update_product_link_in_csv
    FILLER_AVAILABLE = True
except ImportError:
    print("[警告] 未能导入 filler.py")
    FILLER_AVAILABLE = False

# ================= 主逻辑 (Async) =================

async def process_product(sem, browser, item):
    """单个商品处理逻辑 (并发单元, 返回结果而不直接写入)"""
    async with sem: # 限制并发数
        # 初始化
        url = item.get('url', '').strip()
        name = item['product_name']
        brand = item['brand']
        platform = item.get('platform', '').strip()
        country = item.get('country', 'FR')
        platform_lower = platform.lower()
        
        # 默认结果结构
        result = {
            "brand": brand, "name": name, "country": country, 
            "platform": platform, "url": url,
            "price": None, "currency": None, "title": "",
            "status": "Pending"
        }
        
        print(f"\n正在处理 [{country}] {name} ({platform}) ...")
        
        context = None
        try:
            # 创建独立上下文
            context = await browser.new_context(
                user_agent=USER_AGENT_STR,
                viewport={'width': 1920, 'height': 1080},
                locale='en-GB', 
                timezone_id='Europe/London'
            )
            # 屏蔽 WebDriver 特征
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = await context.new_page()
            
            # === 大循环: 允许 "链接失效 -> 清空 -> 重新搜索" 的流程 ===
            # 最多循环2次：
            # 第1次: 正常尝试
            # 第2次: 如果第1次发现死链并清空了URL，第2次就会触发搜索
            MAX_LOOPS = 2
            for loop_index in range(MAX_LOOPS):
                
                # --- 1. 自动填充逻辑 ---
                just_filled = False
                if not url and FILLER_AVAILABLE:
                    print(f"  [{name}] 链接为空/失效，执行自动搜索...")
                    new_link = None
                    target_keyword = f"{brand} {name}"
                    try:
                        if "fnac" in platform_lower:
                            new_link = await get_first_result_fnac(page, target_keyword)
                        elif "darty" in platform_lower:
                            new_link = await get_first_result_darty(page, target_keyword)
                        elif "boulanger" in platform_lower:
                            new_link = await get_first_result_boulanger(page, target_keyword)
                        elif "amazon" in platform_lower:
                            new_link = await get_first_result_amazon(page, target_keyword)
                        
                        if new_link:
                            print(f"  [成功] 自动填充: {new_link}")
                            url = new_link
                            result['url'] = new_link
                            update_product_link_in_csv(name, new_link)
                            just_filled = True
                        else:
                            print(f"  [{name}] 未搜到链接")
                            result['status'] = "Failed: No Link Found"
                            break # 搜不到就没戏了，退出
                    except Exception as e:
                        print(f"  [{name}] 自动填充出错: {e}")
                        result['status'] = f"Failed: Filler Error"
                        break

                if not url:
                    if result['status'] == "Pending": result['status'] = "Failed: Empty URL"
                    break

                # --- 2. 导航与抓取 (含重试) ---
                price_found = False
                
                # 内层重试循环 (针对网络波动)
                MAX_RETRIES = 2
                for attempt in range(MAX_RETRIES):
                    try:
                        should_navigate = True
                        # 优化: Boulanger 自动跳转检测 (仅当刚刚填充时)
                        if just_filled and "/ref/" in url and url in page.url:
                             should_navigate = False
                        
                        if should_navigate:
                            try:
                                # 随机等待，模拟人类
                                delay = random.uniform(1.0, 3.0)
                                await asyncio.sleep(delay)
                                
                                timeout_val = 40000 if attempt == 0 else 60000
                                await page.goto(url, wait_until='domcontentloaded', timeout=timeout_val)
                            except Exception as e:
                                print(f"  [{name}] 导航超时/错误 ({attempt+1}): {e}")
                                if attempt < MAX_RETRIES - 1: continue
                                
                                # 关键修复: 导航彻底失败时，视为死链，触发 Filler
                                print(f"  [{name}] 导航彻底失败，尝试标记为无效链接以触发搜索...")
                                url = None
                                result['url'] = None
                                break # 跳出 retry loop，进入外层 loop 的 continue 逻辑

                        # === 死链与反爬检测 ===
                        page_title = await page.title()
                        page_content = await page.content() if "amazon" in platform_lower else ""
                        
                        is_broken = False
                        is_bot_check = False
                        
                        # 404 检测
                        if "404" in page_title or "Page Not Found" in page_title: is_broken = True
                        if "boulanger" in platform_lower and ("Oups" in page_title or "produit est épuisé" in page_title): is_broken = True
                        if "amazon" in platform_lower and ("SORRY" in page_content or "we cannot find that page" in page_content): is_broken = True
                        
                        # 反爬检测
                        if "amazon" in platform_lower and ("Robot Check" in page_title or page_title == "Amazon.co.uk"):
                             # 此时如果不包含具体商品名，很可能是验证码
                             # 简单判断: 标题太短且是Amazon
                             if len(page_title) < 15: is_bot_check = True
                        
                        if is_broken:
                            print(f"  [{name}] 检测到死链/404页面: {url}")
                            # 关键: 清空URL，不重试抓取，直接 break 进入外层循环 (触发重搜)
                            url = None
                            result['url'] = None 
                            break 
                        
                        if is_bot_check:
                             print(f"  [{name}] 检测到 Amazon 验证码 (Anti-Bot)")
                             result['status'] = "Failed: Anti-Bot Block"
                             # 这种情况下重试可能有用，也可能没用。
                             # 但如果已经重试多次，只能认栽
                             if attempt == MAX_RETRIES - 1: 
                                 break
                        
                        # Anti-bot logic (Clicking buttons)
                        try:
                            if await page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                                await page.click("#onetrust-accept-btn-handler")
                            if "amazon" in platform_lower:
                                if await page.is_visible("input#continue-shopping", timeout=2000):
                                    await page.click("input#continue-shopping")
                                if await page.is_visible("#sp-cc-accept", timeout=2000):
                                    await page.click("#sp-cc-accept")
                        except: pass

                        # 等待价格
                        if "amazon" in platform_lower:
                            try: await page.wait_for_selector(".a-price, #outOfStock, #availability, .a-color-price", timeout=5000)
                            except: pass
                        elif "boulanger" in platform_lower:
                            try: await page.wait_for_selector(".price__amount, .price", timeout=5000)
                            except: pass

                        # 抓取价格
                        price_data = None
                        if "fnac" in platform_lower: price_data = await get_fnac_price(page)
                        elif "darty" in platform_lower: price_data = await get_darty_price(page)
                        elif "boulanger" in platform_lower: price_data = await get_boulanger_price(page)
                        elif "amazon" in platform_lower: 
                            is_oos = False
                            for oos_sel in ["#outOfStock", "#availability .a-color-price", "span:has-text('Currently unavailable')"]:
                                if await page.is_visible(oos_sel, timeout=500):
                                    if "unavailable" in (await page.inner_text(oos_sel)).lower():
                                        is_oos = True; break
                            if is_oos:
                                result['status'] = "Out of Stock"
                                price_found = True # 算作"成功"处理了(状态明确)
                                break
                            price_data = await get_amazon_price(page)
                        
                        if price_data:
                            result['price'], result['currency'] = price_data
                            result['status'] = "Success"
                            
                            # === 成功后读取标题 (修复: 避免 Amazon.co.uk) ===
                            final_title = await page.title()
                            if len(final_title) < 15 or "Amazon.co.uk" == final_title.strip():
                                try:
                                    h1 = await page.inner_text("h1")
                                    if h1: final_title = h1.strip()
                                except: pass
                            result['title'] = final_title
                            
                            print(f"  [成功] {name}: {result['currency']} {result['price']}")
                            price_found = True
                            break # 成功，退出重试
                        else:
                            # 没抓到价格
                            print(f"  [{name}] 未找到价格 (尝试 {attempt+1})")
                            if attempt == MAX_RETRIES - 1:
                                if not result['status'].startswith("Failed"):
                                     result['status'] = "Failed: Price Not Found"
                            else:
                                await asyncio.sleep(2)
                                
                    except Exception as e:
                         print(f"  [{name}] 异常: {str(e)[:50]}")
                
                # 内层循环结束
                if price_found:
                    break # 成功抓到数据(或缺货)，跳出外层大循环
                
                if url is None:
                    # 说明在内层循环里被标记为死链了
                    # continue 到外层循环，此时 url 为空，会进入上方 "自动填充" 逻辑
                    continue
                
                # 如果 URL 还在，但就是没抓到价格 (重试也耗尽了)
                break # 不重搜，直接结束

        except Exception as e:
            print(f"  [{name}] 严重异常: {e}")
            result['status'] = f"Failed: Critical Error {str(e)[:50]}"
        finally:
            if context: await context.close()
            
        return result

async def run_scraper_async(headless=True):
    products = load_products_from_csv()
    if not products: return

    print(f"启动并发爬虫 (Headless={headless}, Concurrency=3)...")

    async with async_playwright() as p:
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--ignore-certificate-errors'
        ]
        # Try real Chrome
        try:
            browser = await p.chromium.launch(headless=headless, channel="chrome", args=browser_args)
        except:
            browser = await p.chromium.launch(headless=headless, args=browser_args)
        
        sem = asyncio.Semaphore(3)
        
        # 1. 并发执行所有任务
        tasks = [process_product(sem, browser, item) for item in products]
        results = await asyncio.gather(*tasks) # gather 保证结果顺序与 products 顺序一致
        
        await browser.close()
    
    # 2. 按顺序写入 CSV
    print("\n正在按顺序写入结果...")
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    
    for res in results:
        log_price_update(
            date_str, time_str, 
            res['brand'], res['name'], res['country'], res['platform'], 
            res['price'], res['currency'], res['title'], res['status']
        )
            
    print("所有任务完成。")

def run_scraper(headless=True):
    asyncio.run(run_scraper_async(headless))

if __name__ == "__main__":
    run_scraper(headless=True)
