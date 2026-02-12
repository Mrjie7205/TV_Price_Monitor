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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "prices.csv")
PRODUCTS_CSV = os.path.join(BASE_DIR, "products.csv")

# ================= 随机 User-Agent 池 =================
USER_AGENTS = [
    # Chrome - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Edge - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Edge - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Safari - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Chrome - Linux (模拟 CI 环境)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ================= Stealth 注入脚本 =================
STEALTH_JS = """
// 1. 屏蔽 webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 2. 伪造 plugins (正常浏览器至少有几个)
Object.defineProperty(navigator, 'plugins', {
    get: () => [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
        { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
    ]
});

// 3. 伪造 languages
Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en'] });
Object.defineProperty(navigator, 'language', { get: () => 'en-GB' });

// 4. 屏蔽 chrome.runtime (Headless Chrome 特征)
if (window.chrome) {
    window.chrome.runtime = undefined;
}

// 5. 伪造 permissions (正常浏览器有 query 方法)
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
);

// 6. 隐藏 Headless 特征 (window.outerWidth/outerHeight)
Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth });
Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight + 85 });

// 7. 给 HTMLIFrameElement 打补丁 (防止通过 iframe 检测)
const iframeProto = HTMLIFrameElement.prototype;
const origContentWindow = Object.getOwnPropertyDescriptor(iframeProto, 'contentWindow');
if (origContentWindow) {
    Object.defineProperty(iframeProto, 'contentWindow', {
        get: function() {
            const iframe = origContentWindow.get.call(this);
            if (iframe) {
                try { Object.defineProperty(iframe.navigator, 'webdriver', { get: () => undefined }); } catch(e) {}
            }
            return iframe;
        }
    });
}
"""

# ================= 工具函数 =================

def clean_price(text):
    """清洗价格文本"""
    if not text: return None
    text = text.strip()
    currency = "EUR"
    if "£" in text or "GBP" in text: currency = "GBP"
    elif "$" in text or "USD" in text: currency = "USD"
    
    clean_text = text.replace("€", "").replace("£", "").replace("$", "").replace("EUR", "").replace("GBP", "").replace("USD", "")
    clean_text = clean_text.replace("\xa0", "").strip()
    
    try:
        if currency == "EUR":
            clean_text = clean_text.replace(" ", "").replace(",", ".")
        else:
            clean_text = clean_text.replace(",", "").replace(" ", "")
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
    """写入 CSV"""
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

def clean_duplicate_links_in_csv():
    """运行前清洗: 检测 products.csv 中重复的链接，将后出现的重复项清空以触发 Filler 重搜"""
    if not os.path.exists(PRODUCTS_CSV):
        return
    try:
        with open(PRODUCTS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
    except Exception as e:
        print(f"[清洗] 读取 CSV 失败: {e}")
        return
    
    link_col = None
    for i, col in enumerate(header):
        if col.strip().lower() in ('link', '链接', 'url'):
            link_col = i
            break
    if link_col is None:
        return
    
    name_col = None
    for i, col in enumerate(header):
        if col.strip().lower() in ('product name', '型号'):
            name_col = i
            break
    
    seen_urls = set()
    duplicates_found = 0
    for row in rows:
        if link_col >= len(row): continue
        link = row[link_col].strip()
        if not link: continue
        if link in seen_urls:
            product_name = row[name_col].strip() if name_col is not None and name_col < len(row) else "Unknown"
            print(f"  [清洗] 发现重复链接，已清空等待重新搜索: {product_name}")
            row[link_col] = ""
            duplicates_found += 1
        else:
            seen_urls.add(link)
    
    if duplicates_found > 0:
        print(f"[清洗] 共发现 {duplicates_found} 个重复链接，正在写回 products.csv...")
        try:
            with open(PRODUCTS_CSV, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
            print(f"[清洗] products.csv 已更新。")
        except Exception as e:
            print(f"[清洗] 写回 CSV 失败: {e}")
    else:
        print("[清洗] 未发现重复链接，跳过。")

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
    """抓取亚马逊价格 (优先 Deal Price，支持第三方卖家)"""
    # 策略 1: PriceToPay (最准)
    for sel in [".priceToPay .a-offscreen", ".apexPriceToPay .a-offscreen",
                "#corePriceDisplay_desktop_feature_div .priceToPay .a-offscreen",
                "span[data-a-color='price'] .a-offscreen"]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                text = await el.text_content()
                if text:
                    result = clean_price(text)
                    if result: return result
        except: pass

    # 策略 2: 排除原价
    try:
        el = page.locator(".a-price:not(.a-text-price) .a-offscreen").first
        if await el.count() > 0:
             text = await el.text_content()
             result = clean_price(text)
             if result: return result
    except: pass
    
    # 策略 3: See All Buying Options (第三方卖家)
    try:
        if await page.is_visible("a[title*='See All Buying Options']", timeout=500) or \
           await page.is_visible("text=See All Buying Options", timeout=500):
            el = page.locator("span.a-color-price").first
            if await el.count() > 0:
                text = await el.text_content()
                result = clean_price(text)
                if result:
                    print(f"  [提示] 抓取到第三方卖家起售价: {result}")
                    return result
    except: pass
    
    # 策略 4: 旧版选择器
    for sel in ["#priceblock_ourprice", "#priceblock_dealprice", ".a-price-whole"]:
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

# ================= Amazon 专属: 首页预热 =================

async def amazon_warmup(page):
    """模拟真人: 先访问首页，接受 Cookie，滑动一下，再跳转商品页"""
    try:
        print("  [预热] 访问 Amazon UK 首页...")
        await page.goto("https://www.amazon.co.uk", wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(2.0, 4.0))
        
        # 接受 Cookie
        try:
            if await page.is_visible("#sp-cc-accept", timeout=3000):
                await page.click("#sp-cc-accept")
                await asyncio.sleep(1)
        except: pass
        
        # 模拟滚动 (注入 JS)
        try:
            await page.evaluate("""
                () => {
                    window.scrollBy(0, 300);
                    setTimeout(() => window.scrollBy(0, 200), 500);
                    setTimeout(() => window.scrollBy(0, -100), 1200);
                }
            """)
            await asyncio.sleep(random.uniform(1.5, 3.0))
        except: pass
        
        print("  [预热] 首页预热完成。")
    except Exception as e:
        print(f"  [预热] 首页预热失败 (非致命): {e}")

# ================= 主逻辑 (Async) =================

async def process_product(sem, browser, item):
    """单个商品处理逻辑 (并发单元, 返回结果而不直接写入)"""
    async with sem:
        # 初始化
        url = item.get('url', '').strip()
        name = item['product_name']
        brand = item['brand']
        platform = item.get('platform', '').strip()
        country = item.get('country', 'FR')
        platform_lower = platform.lower()
        is_amazon = "amazon" in platform_lower
        
        result = {
            "brand": brand, "name": name, "country": country,
            "platform": platform, "url": url,
            "price": None, "currency": None, "title": "",
            "status": "Pending"
        }
        
        print(f"\n正在处理 [{country}] {name} ({platform}) ...")
        
        context = None
        try:
            # === 创建独立上下文 (随机指纹) ===
            ua = random.choice(USER_AGENTS)
            context = await browser.new_context(
                user_agent=ua,
                viewport={'width': random.choice([1920, 1366, 1440, 1536]), 'height': random.choice([1080, 768, 900])},
                locale='en-GB',
                timezone_id='Europe/London'
            )
            # 注入完整 Stealth 脚本
            await context.add_init_script(STEALTH_JS)
            
            page = await context.new_page()
            
            # === Amazon 专属: 首页预热 ===
            if is_amazon:
                await amazon_warmup(page)
            
            # === 大循环: 允许 "链接失效 -> 清空 -> 重新搜索" ===
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
                        elif is_amazon:
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
                            break
                    except Exception as e:
                        print(f"  [{name}] 自动填充出错: {e}")
                        result['status'] = "Failed: Filler Error"
                        break

                if not url:
                    if result['status'] == "Pending": result['status'] = "Failed: Empty URL"
                    break

                # --- 2. 导航与抓取 (含重试 + Robot Check 逃逸) ---
                price_found = False
                MAX_RETRIES = 2
                robot_check_retried = False  # Robot Check 只重试一次
                
                for attempt in range(MAX_RETRIES):
                    try:
                        should_navigate = True
                        if just_filled and "/ref/" in url and url in page.url:
                             should_navigate = False
                        
                        if should_navigate:
                            try:
                                # === Amazon 专属降速: 8-15秒等待 ===
                                if is_amazon:
                                    delay = random.uniform(8.0, 15.0)
                                    print(f"  [{name}] Amazon 降速等待 {delay:.1f}s ...")
                                    await asyncio.sleep(delay)
                                else:
                                    await asyncio.sleep(random.uniform(1.0, 3.0))
                                
                                timeout_val = 40000 if attempt == 0 else 60000
                                await page.goto(url, wait_until='domcontentloaded', timeout=timeout_val)
                            except Exception as e:
                                print(f"  [{name}] 导航超时/错误 ({attempt+1}): {e}")
                                if attempt < MAX_RETRIES - 1: continue
                                print(f"  [{name}] 导航彻底失败，标记为无效链接...")
                                url = None
                                result['url'] = None
                                break

                        # === 死链与反爬检测 ===
                        page_title = await page.title()
                        page_content = ""
                        if is_amazon:
                            page_content = await page.content()
                        
                        # 404 检测
                        is_broken = False
                        if "404" in page_title or "Page Not Found" in page_title: is_broken = True
                        if "boulanger" in platform_lower and ("Oups" in page_title or "épuisé" in page_title): is_broken = True
                        if is_amazon and ("SORRY" in page_content or "we cannot find that page" in page_content): is_broken = True
                        
                        if is_broken:
                            print(f"  [{name}] 检测到死链/404页面")
                            url = None
                            result['url'] = None
                            break
                        
                        # === Robot Check 逃逸逻辑 ===
                        if is_amazon and ("Robot Check" in page_title or (len(page_title) < 15 and "Amazon" in page_title)):
                            if not robot_check_retried:
                                print(f"  [{name}] ⚠ 遭遇验证码，尝试绕过...")
                                robot_check_retried = True
                                
                                # 1. 清空 Cookies
                                await context.clear_cookies()
                                print(f"  [{name}] Cookies 已清空")
                                
                                # 2. 长等待
                                wait_time = random.uniform(18.0, 25.0)
                                print(f"  [{name}] 等待 {wait_time:.0f}s 后重试...")
                                await asyncio.sleep(wait_time)
                                
                                # 3. 重新预热首页
                                await amazon_warmup(page)
                                
                                # 4. continue 重试抓取
                                continue
                            else:
                                print(f"  [{name}] 验证码逃逸失败，放弃")
                                result['status'] = "Failed: Anti-Bot Block"
                                break
                        
                        # Anti-bot (Cookie 弹窗)
                        try:
                            if await page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                                await page.click("#onetrust-accept-btn-handler")
                            if is_amazon:
                                if await page.is_visible("input#continue-shopping", timeout=2000):
                                    await page.click("input#continue-shopping")
                                    await asyncio.sleep(2)
                                if await page.is_visible("#sp-cc-accept", timeout=2000):
                                    await page.click("#sp-cc-accept")
                        except: pass

                        # 等待价格元素
                        if is_amazon:
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
                        elif is_amazon:
                            # 缺货检测
                            is_oos = False
                            for oos_sel in ["#outOfStock", "#availability .a-color-price", "span:has-text('Currently unavailable')"]:
                                try:
                                    if await page.is_visible(oos_sel, timeout=500):
                                        if "unavailable" in (await page.inner_text(oos_sel)).lower():
                                            is_oos = True; break
                                except: pass
                            if is_oos:
                                result['status'] = "Out of Stock"
                                price_found = True
                                break
                            price_data = await get_amazon_price(page)
                        
                        if price_data:
                            result['price'], result['currency'] = price_data
                            result['status'] = "Success"
                            
                            # 成功后读取标题
                            final_title = await page.title()
                            if len(final_title) < 15 or "Amazon.co.uk" == final_title.strip():
                                try:
                                    h1 = await page.inner_text("h1")
                                    if h1: final_title = h1.strip()
                                except: pass
                            result['title'] = final_title
                            
                            print(f"  [成功] {name}: {result['currency']} {result['price']}")
                            price_found = True
                            break
                        else:
                            print(f"  [{name}] 未找到价格 (尝试 {attempt+1})")
                            if attempt == MAX_RETRIES - 1:
                                if not result['status'].startswith("Failed"):
                                    result['status'] = "Failed: Price Not Found"
                            else:
                                await asyncio.sleep(2)
                                
                    except Exception as e:
                         print(f"  [{name}] 异常: {str(e)[:80]}")
                
                # 内层循环结束
                if price_found:
                    break
                if url is None:
                    continue
                if not result['status'].startswith("Failed"):
                    result['status'] = "Failed: Price Not Found"
                break

        except Exception as e:
            print(f"  [{name}] 严重异常: {e}")
            result['status'] = f"Failed: Critical Error {str(e)[:50]}"
        finally:
            if context: await context.close()
            
        return result

async def run_scraper_async(headless=True):
    # 运行前: 清洗重复链接
    clean_duplicate_links_in_csv()
    
    products = load_products_from_csv()
    if not products: return

    print(f"启动并发爬虫 (Headless={headless}, Concurrency=3)...")

    async with async_playwright() as p:
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--ignore-certificate-errors',
            '--disable-dev-shm-usage'
        ]
        try:
            browser = await p.chromium.launch(headless=headless, channel="chrome", args=browser_args)
        except:
            browser = await p.chromium.launch(headless=headless, args=browser_args)
        
        sem = asyncio.Semaphore(3)
        
        tasks = [process_product(sem, browser, item) for item in products]
        results = await asyncio.gather(*tasks)
        
        await browser.close()
    
    # 按顺序写入 CSV
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
