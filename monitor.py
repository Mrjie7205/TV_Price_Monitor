import time
import csv
import os
import random
import re
from datetime import datetime
from urllib.parse import quote
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ================= 配置区域 =================
# 模拟 Windows Chrome 浏览器 (使用较新的版本号)
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# 结果保存文件 (确保保存在脚本同级目录)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "prices.csv")
PRODUCTS_CSV = os.path.join(BASE_DIR, "products.csv")

# ================= 工具函数 =================

def clean_price(text):
    """
    清洗价格文本，智能识别币种并返回 (price_value, currency_code)。
    支持格式:
    - 1 299,00 € (EUR)
    - £1,299.00 (GBP)
    - $1,299.00 (USD)
    
    Returns:
        tuple: (float_price, str_currency) or None if failed
    """
    if not text:
        return None

    text = text.strip()
    
    # 1. 识别货币
    currency = "EUR" # 默认欧元
    if "£" in text or "GBP" in text:
        currency = "GBP"
    elif "$" in text or "USD" in text:
        currency = "USD"
    
    # 2. 清洗数字格式
    # 保留数字、小数点、逗号
    clean_text = text.replace("€", "").replace("£", "").replace("$", "").replace("EUR", "").replace("GBP", "").replace("USD", "")
    clean_text = clean_text.replace("\xa0", "").strip() # 去除不间断空格
    
    try:
        if currency == "EUR":
            # 欧元区通常格式: 1 234,56 (空格作千分位, 逗号作小数点)
            clean_text = clean_text.replace(" ", "") # 去除千分位空格
            clean_text = clean_text.replace(",", ".") # 逗号变点
        else:
            # 英美通常格式: 1,234.56 (逗号作千分位, 点作小数点)
            clean_text = clean_text.replace(",", "") # 去除千分位逗号
            clean_text = clean_text.replace(" ", "") # 去除可能存在的空格

        # 使用正则提取浮点数
        match = re.search(r"(\d+(\.\d+)?)", clean_text)
        if match:
            price_float = float(match.group(1))
            return price_float, currency
            
    except Exception as e:
        pass
        
    return None

def load_products_from_csv():
    """从 products.csv 加载商品列表"""
    products = []
    
    if not os.path.exists(PRODUCTS_CSV): # 如果没有，创建一个新结构的模板
        print(f"[提示] 未找到 {PRODUCTS_CSV}，正在创建模板...")
        with open(PRODUCTS_CSV, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            # 新结构: Brand, Product Name, Country, Platform, Link
            writer.writerow(["Brand", "Product Name", "Country", "Platform", "Link"])
            writer.writerow(["TCL", "示例商品", "FR", "Darty", "https://www.darty.com/..."])
        return []

    try:
        with open(PRODUCTS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 兼容多种列名写法
                link = row.get("Link") or row.get("链接") or row.get("url")
                name = row.get("Product Name") or row.get("型号") or row.get("name")
                platform = row.get("Platform") or row.get("平台") or row.get("渠道")
                brand = row.get("Brand") or row.get("品牌")
                
                # 新增 Country 字段，默认为 FR
                country = row.get("Country", "FR") 
                if not country: country = "FR"
                country = country.strip().upper()

                if name:
                    products.append({
                        "product_name": name.strip(),
                        "url": link.strip() if link else "",
                        "platform": platform.strip() if platform else "",
                        "brand": brand.strip() if brand else "",
                        "country": country
                    })
    except Exception as e:
        print(f"[错误] 读取 products.csv 失败: {e}")
    
    print(f"已加载 {len(products)} 个商品任务")
    return products

def log_price_update(date_str, time_str, brand, name, country, platform, price, currency, page_title, status="Success"):
    """
    追加写入价格数据到 prices.csv (新结构)
    Format: Date, Time, Brand, Product Name, Country, Platform, Price, Currency, Page Title, Status
    """
    file_exists = os.path.isfile(CSV_FILE)
    
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 如果文件不存在，写入新表头
            if not file_exists:
                writer.writerow(["Date", "Time", "Brand", "Product Name", "Country", "Platform", "Price", "Currency", "Page Title", "Status"])
            
            # 写入数据
            writer.writerow([
                date_str, 
                time_str, 
                brand, 
                name, 
                country, 
                platform, 
                price, 
                currency, 
                page_title, 
                status
            ])
            
            print(f"  [记录] {currency} {price} | Status: {status}")
            
    except Exception as e:
        print(f"  [错误] 写入 CSV 失败: {e}")

# ================= 爬虫策略函数 =================

def get_fnac_price(page):
    """Fnac 价格提取"""
    selectors = [".f-price", ".userPrice", ".product-price", ".price", "span[class*='price']"]
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=2000):
                text = page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

def get_darty_price(page):
    """Darty 价格提取"""
    selectors = [".product_price", ".price", ".darty_price", "span[class*='price']"]
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=2000):
                text = page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

def get_boulanger_price(page):
    """Boulanger 价格提取"""
    # 策略 1: 主价格区域
    try:
        price_main = page.locator(".price__amount").first
        if price_main.is_visible(timeout=3000):
            text = price_main.inner_text() 
            text = text.replace("\n", ",") 
            result = clean_price(text)
            if result: return result
    except: pass
    
    # 策略 2: 通用
    selectors = [".price", "span[class*='price']"]
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=2000):
                text = page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
    return None

def get_amazon_price(page):
    """Amazon 价格提取"""
    # 策略 1: 隐藏的完整价格文本 (最准确)
    try:
        price_el = page.locator(".a-price .a-offscreen").first
        if price_el.count() > 0:
            text = price_el.text_content() # use text_content for hidden elements
            result = clean_price(text)
            if result: return result
    except: pass
    
    # 策略 2: 详情页特定 ID
    selectors = ["#priceblock_ourprice", "#priceblock_dealprice", "#corePriceDisplay_desktop_feature_div .a-price-whole", ".apexPriceToPay"]
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=1000):
                text = page.inner_text(sel)
                result = clean_price(text)
                if result: return result
        except: pass
        
    return None

# ================= 主逻辑 =================

# 尝试导入自动填充逻辑
try:
    from filler import get_first_result_darty, get_first_result_boulanger, get_first_result_fnac, get_first_result_amazon, update_product_link_in_csv
    FILLER_AVAILABLE = True
except ImportError:
    print("[警告] 未能导入 filler.py，自动修复链接功能将不可用。")
    FILLER_AVAILABLE = False

def run_scraper(headless=True):
    products = load_products_from_csv()
    if not products:
        print("没有找到商品任务，请检查 products.csv")
        return

    print(f"启动爬虫 (Headless={headless})...")

    with sync_playwright() as p:
        # 启动浏览器 - 升级反爬配置
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--window-position=0,0',
            '--ignore-certificate-errors',
            '--ignore-certificate-errors-spki-list'
        ]
        
        # 尝试使用本机安装的 Chrome (如果可用)，因其指纹更真实
        try:
            browser = p.chromium.launch(headless=headless, channel="chrome", args=browser_args)
        except:
            print("  [提示] 使用默认 Chromium")
            browser = p.chromium.launch(headless=headless, args=browser_args)

        context = browser.new_context(
            user_agent=USER_AGENT_STR,
            viewport={'width': 1920, 'height': 1080},
            locale='en-GB', 
            timezone_id='Europe/London'
        )
        
        # 强力屏蔽 Webdriver 特征
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.navigator.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """)
        
        page = context.new_page()

        for item in products:
            url = item.get('url', '').strip()
            name = item['product_name']
            brand = item['brand']
            platform = item.get('platform', '').strip()
            country = item.get('country', 'FR')
            
            platform_lower = platform.lower()
            
            print(f"\n正在处理 [{country}] {name} ({platform}) ...")
            
            # --- 自动填充逻辑 ---
            just_filled = False
            if not url and FILLER_AVAILABLE:
                print("  [提示] 链接为空，尝试自动搜索...")
                new_link = None
                try:
                    target_keyword = f"{brand} {name}"
                    
                    if "fnac" in platform_lower:
                        new_link = get_first_result_fnac(page, target_keyword)
                    elif "darty" in platform_lower:
                        new_link = get_first_result_darty(page, target_keyword)
                    elif "boulanger" in platform_lower:
                        from filler import get_first_result_boulanger 
                        new_link = get_first_result_boulanger(page, target_keyword)
                    elif "amazon" in platform_lower:
                        from filler import get_first_result_amazon
                        new_link = get_first_result_amazon(page, target_keyword)
                    
                    if new_link:
                        print(f"  [成功] 自动填充链接: {new_link}")
                        url = new_link
                        item['url'] = new_link
                        update_product_link_in_csv(name, new_link)
                        just_filled = True
                    else:
                        print("  [失败] 无法自动找到链接，跳过。")
                        continue
                except Exception as e:
                    print(f"  [错误] 自动填充出错: {e}")
                    continue

            if not url:
                continue

            # --- 抓取价格 ---
            try:
                # 优化: 如果刚刚自动填充且已经在目标页面 (如 Boulanger 重定向)，跳过跳转
                should_navigate = True
                if just_filled and url in page.url and "/ref/" in url:
                    print("  [优化] 页面已位于目标链接，跳过导航")
                    should_navigate = False
                
                if should_navigate:
                    page.goto(url, wait_until='domcontentloaded', timeout=40000)
                
                # === 反爬虫对抗 ===
                
                # 1. Amazon "Continue shopping" 验证
                if "amazon" in platform_lower:
                    try:
                        # 查找类似 "Continue shopping" 的按钮
                        btn = page.locator("button:has-text('Continue shopping'), input#continue-shopping").first
                        if btn.is_visible(timeout=2000):
                            print("  [对抗] 检测到 Amazon 验证页，尝试点击继续...")
                            btn.click()
                            page.wait_for_load_state("domcontentloaded")
                            time.sleep(2)
                    except: pass
                
                # 2. 通用 Cookie 弹窗
                try:
                    if page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                        page.click("#onetrust-accept-btn-handler")
                    if "amazon" in platform_lower and page.is_visible("#sp-cc-accept", timeout=2000):
                        page.click("#sp-cc-accept")
                except: pass
                
                # 3. Boulanger "Invalid URL" 检测
                page_title = page.title()
                if "boulanger" in platform_lower and "Invalid URL" in page_title:
                     print("  [对抗] Boulanger 返回 Invalid URL，尝试强制刷新...")
                     page.reload(wait_until="domcontentloaded")
                     time.sleep(3)
                     page_title = page.title() # Update title

                price_data = None 
                
                # 根据平台调用策略
                if "fnac" in platform_lower:
                    price_data = get_fnac_price(page)
                elif "darty" in platform_lower:
                    price_data = get_darty_price(page)
                elif "boulanger" in platform_lower:
                    price_data = get_boulanger_price(page)
                elif "amazon" in platform_lower:
                    price_data = get_amazon_price(page)
                else:
                    # 通用兜底
                    try:
                        price_el = page.locator("body").get_by_text(re.compile(r"[\d.,]+\s?[€$£]")).first
                        if price_el.count() > 0:
                            price_data = clean_price(price_el.inner_text())
                    except: pass

                # --- 记录结果 ---
                now = datetime.now()
                date_str = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H:%M:%S")

                if price_data:
                    price, currency = price_data
                    status = "Success"
                    log_price_update(date_str, time_str, brand, name, country, platform, price, currency, page_title, status)
                else:
                    print(f"  [失败] 未能抓取到价格 (Title: {page_title.strip()})")
                    # 截图留证
                    try:
                        screenshot = f"debug_{platform}_{int(time.time())}.png"
                        page.screenshot(path=screenshot)
                    except: pass
                    
            except Exception as e:
                print(f"  [错误] 处理 {name} 时发生异常: {e}")
                
        context.close()
        browser.close()

if __name__ == "__main__":
    run_scraper(headless=True)
