import time
import csv
import os
import random
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ================= 配置区域 =================
# 模拟 Windows Chrome 浏览器
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

# 结果保存文件
# 结果保存文件 (确保保存在脚本同级目录)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "prices.csv")

def load_products_from_csv():
    """从 products.csv 加载商品列表"""
    csv_path = os.path.join(BASE_DIR, "products.csv")
    products = []
    
    if not os.path.exists(csv_path): # 如果没有，创建一个空的
        print(f"[提示] 未找到 {csv_path}，正在创建模板...")
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Brand", "Product Name", "Platform", "Link"])
            writer.writerow(["TCL", "示例商品", "Darty", "https://www.darty.com/..."])
        return []

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # 简单的列名匹配
            for row in reader:
                # 尝试获取 Link 列 (兼容中文 '链接')
                link = row.get("Link") or row.get("链接") or row.get("url")
                # 尝试获取 Name 列 (兼容中文 '型号')
                name = row.get("Product Name") or row.get("型号") or row.get("name")
                # 尝试获取 Platform 列 (兼容中文 '平台')
                platform = row.get("Platform") or row.get("平台") or row.get("渠道")
                
                # 尝试获取 Brand 列 (兼容中文 '品牌')
                brand = row.get("Brand") or row.get("品牌") or row.get("brand")

                if name:
                    # 即使没有链接也加入，以便后续自动填充
                    products.append({
                        "product_name": name.strip(),
                        "url": link.strip() if link else "",
                        "platform": platform.strip() if platform else "",
                        "brand": brand.strip() if brand else ""
                    })
    except Exception as e:
        print(f"[错误] 读取 products.csv 失败: {e}")
    
    print(f"已加载 {len(products)} 个商品任务")
    return products

# 动态加载商品
PRODUCTS = load_products_from_csv()

# 尝试导入自动填充逻辑
try:
    from filler import get_first_result_darty, get_first_result_boulanger, get_first_result_fnac
    FILLER_AVAILABLE = True
except ImportError:
    print("[警告] 未能导入 filler.py，自动修复链接功能将不可用。")
    FILLER_AVAILABLE = False

def update_product_link_in_csv(product_name, new_url):
    """更新 CSV 中的链接"""
    temp_rows = []
    updated = False
    products_csv = os.path.join(BASE_DIR, "products.csv")
    
    if not os.path.exists(products_csv): return False
        
    try:
        with open(products_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                p_name = row.get("Product Name") or row.get("型号")
                if p_name and p_name.strip() == product_name.strip():
                    if "Link" in row: row["Link"] = new_url
                    elif "链接" in row: row["链接"] = new_url
                    elif "url" in row: row["url"] = new_url
                    updated = True
                temp_rows.append(row)
        
        if updated:
            with open(products_csv, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(temp_rows)
            print(f"  [系统] 已更新 {product_name} 的新链接到 CSV")
            return True
    except Exception as e:
        print(f"  [系统错误] 更新 CSV 失败: {e}")
    return False

# ================= 核心逻辑 =================

def clean_price(price_text):
    """
    清洗价格文本，转化为 float。
    处理情况：
    - "1 234,99 €" -> 1234.99
    - "999€99" -> 999.99
    - "1.234,99" -> 1234.99
    """
    if not price_text:
        return None
    
    # 1. 去除两端空格
    txt = price_text.strip()
    
    # 2. 移除欧元符号和不可见字符
    txt = txt.replace("€", "").replace("\u20ac", "").replace("\xa0", "").strip()
    
    # 3. 处理 Boulanger 可能出现的 "999€99" 这种中间带符号的情况 (虽然上面已经replace了€，但防止其他变体)
    #    通常 Boulanger 的结构会在HTML里分开，或者直接是 "1234,00"
    
    # 4. 处理千分位和小数点
    # 欧洲格式通常是: 点号(.)或空格是千分位，逗号(,)是小数点
    # 例如: 1 299,00 或 1.299,00
    
    # 先把空格去完
    txt = txt.replace(" ", "")
    
    # 如果有多个点或逗号，需要判断谁是千分位
    # 简单策略：直接把 所有非数字和逗号点号 的字符都去掉
    # 然后把 ',' 替换为 '.' 
    # (注意：如果同时存在 . 和 , 且 . 在前，则 . 是千分位，删掉)
    
    # 更稳健的逻辑：
    # 替换掉所有非 [0-9,.] 的字符
    import re
    txt = re.sub(r'[^0-9,.]', '', txt)
    
    if ',' in txt and '.' in txt:
        # 假设 . 是千分位 (1.234,56) -> 去掉 . -> 1234,56 -> 1234.56
        txt = txt.replace('.', '').replace(',', '.')
    elif ',' in txt:
        # 只有逗号 (1234,56) -> 1234.56
        txt = txt.replace(',', '.')
    # else: 只有点号，可能是千分位也可能是小数点，但在法国电商通常点号是千分位?? 
    # 不，Python float认点号。如果 txt是 "1.234" (一千二百) 转 float 是 1.234
    # 这里我们假设如果是 Darty/Boulanger，如果只有点号且在最后两位之前，可能是小数点？
    # 为保险起见，如果只有点号，不动它，直接转 float。
    
    try:
        return float(txt)
    except ValueError:
        print(f"[警告] 无法将 '{txt}' 转换为数字")
        return None

def get_darty_price(page):
    """
    Darty 价格提取策略
    """
    # 策略 1: 最常见的价格容器
    # 包含整数和小数部分
    selectors = [
        ".darty_prix_barre_remise_prix", # 有时打折时的现价
        ".product-price__price",         # 常见新版
        ".product_price",                # 旧版
        "span[class*='price']",          # 模糊匹配
        "[data-automation-id='product_price']"
    ]
    
    for sel in selectors:
        try:
            # 尝试等待元素出现 (短时间)
            if page.is_visible(sel, timeout=2000):
                # 针对 Darty 有时价格分两部分 (999 € 99), inner_text 会拿到 "999 € 99"
                text = page.inner_text(sel)
                if text and any(char.isdigit() for char in text):
                    print(f"  -> Darty 命中选择器: {sel}, 文本: {text}")
                    return clean_price(text)
        except:
            continue
            
    # 策略 2: 暴力正则匹配 (如果上面的选择器都失败了)
    # 尝试在整个页面内容中寻找类似 "123,45 €" 的文本
    try:
        print("  -> (Darty) 尝试通用正则搜索...")
        # 获取主要内容区域 (避免抓到推荐商品的价格)
        # 优先找 .product_main_container 或 body
        container = page.locator(".product_main_container").first
        if not container.count():
            container = page.locator("body")
            
        content = container.inner_text()
        import re
        # 匹配模式: 数字 + (空格/点/逗号) + 数字 + €
        # 注意：Darty 有时写成 999€99
        matches = re.findall(r'(\d[\d\s\.,]*€[\d\s]*)', content)
        
        # 过滤并尝试解析，取第一个看起来像主价格的 (通常主价格字号大，会在前面，或者我们需要更智能的判断)
        # 这里简单取第一个能解析成功的
        for m in matches[:5]: # 只看前5个匹配
            val = clean_price(m)
            if val and val > 0: # 假设价格大于0
                print(f"  -> (Darty) 正则命中: {m} -> {val}")
                return val
    except Exception as e:
        print(f"  -> 正则搜索出错: {e}")

    return None

def get_boulanger_price(page):
    """
    Boulanger 价格提取策略
    """
    selectors = [
        ".price__amount",       # 常见
        ".price",               # 通用
        "p[data-testid='price-value']",
        ".fix-price",
        ".product-price"
    ]
    
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=2000):
                text = page.inner_text(sel)
                # Boulanger 有时会把指数部分放在 sup 标签里，inner_text 通常能拿到所有文本
                if text and any(char.isdigit() for char in text):
                    print(f"  -> Boulanger 命中选择器: {sel}, 文本: {text}")
                    return clean_price(text)
        except:
            continue
    return None

def get_fnac_price(page):
    """
    Fnac 价格提取策略
    """
    # 策略 1: 常见价格选择器
    selectors = [
        ".f-price",             # 最常见
        ".userPrice",           # 会员价或一般价
        ".product-price",
        ".price",
        "span[class*='price']"
    ]
    
    for sel in selectors:
        try:
            if page.is_visible(sel, timeout=2000):
                text = page.inner_text(sel)
                if text and any(char.isdigit() for char in text):
                    print(f"  -> Fnac 命中选择器: {sel}, 文本: {text}")
                    return clean_price(text)
        except:
            continue
            
    # 策略 2: Meta 标签 (结构化数据)
    try:
        content = page.locator("meta[itemprop='price']").first.get_attribute("content")
        if content:
            print(f"  -> Fnac Meta 标签命中: {content}")
            return float(content)
    except: pass
    
    return None

def run_scraper(headless=False):
    print(f"启动爬虫 (Headless={headless})...")
    


    with sync_playwright() as p:
        # 启动浏览器 (尝试更隐蔽的参数 + 使用本机 Chrome)
        # 注意: 如果你没有安装 Chrome，可以尝试改为 channel="msedge"
        try:
            browser = p.chromium.launch(
                headless=headless,
                channel="chrome",  # 尝试调用本机 Chrome
                slow_mo=50, # 关键：让操作变慢，像人类一样
                args=[
                    '--disable-blink-features=AutomationControlled', 
                    '--start-maximized',
                    '--no-sandbox'
                ]
            )
        except Exception as e:
            print(f"启动 Chrome 失败，尝试默认 Chromium: {e}")
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=50,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized']
            )
        
        # 创建上下文
        context = browser.new_context(
            user_agent=USER_AGENT_STR,
            viewport={'width': 1920, 'height': 1080},
            locale="fr-FR",
            # 添加 Referer 伪装从 Google 来的
            extra_http_headers={
                "Referer": "https://www.google.fr/"
            }
        )
        
        # 注入脚本：屏蔽 webdriver 特征 (关键反爬手段)
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        page = context.new_page()



        for item in PRODUCTS:
            url = item.get('url', '').strip()
            name = item['product_name']
            # 读取平台并统一转小写，方便判断
            platform_raw = item.get('platform', '')
            platform = platform_raw.lower() if platform_raw else ""
            brand = item.get('brand', '')

            print(f"正在处理: {name}")
            
            price = None
            page_title = "Unknown"
            channel = "Unknown"
            status = "Success"
            
            # 1. 确定渠道 (根据 URL 或 平台字段)
            if "darty" in platform or "darty.com" in url:
                channel = "Darty"
            elif "boulanger" in platform or "boulanger.com" in url:
                channel = "Boulanger"
            elif "fnac" in platform or "fnac.com" in url:
                channel = "Fnac"
            
            try:
                # 2. 如果 URL 缺失，尝试自动填充
                if not url and channel != "Unknown" and FILLER_AVAILABLE:
                    print(f"  [Auto-Fill] 发现链接缺失，正在 {channel} 上搜索...")
                    new_link = None
                    try:
                        # 使用现有的 page 对象去搜，利用已有 session
                        if channel == "Darty":
                            new_link = get_first_result_darty(page, name)
                        elif channel == "Boulanger":
                            new_link = get_first_result_boulanger(page, name)
                        elif channel == "Fnac":
                            new_link = get_first_result_fnac(page, name)
                            
                        if new_link:
                            print(f"  [成功] 找到链接: {new_link}")
                            # 更新 CSV
                            if update_product_link_in_csv(name, new_link):
                                url = new_link # 更新内存变量，继续后续抓取
                                status = "Fixed"
                        else:
                            print("  [失败] 未能找到链接，跳过此商品。")
                            print("-" * 30)
                            continue
                    except Exception as e:
                        print(f"  [Error] 自动填充出错: {e}")
                        print("-" * 30)
                        continue
                
                if not url:
                    print(f"  [跳过] 无有效链接且未指定平台 (Platform: {platform_raw})")
                    print("-" * 30)
                    continue

                
                # 访问页面
                # Wait until 'domcontentloaded' is faster than 'networkidle'
                page.goto(url, wait_until='domcontentloaded', timeout=45000)
                time.sleep(random.uniform(2, 4))
                
                # 获取标题
                try: page_title = page.title().strip()
                except: page_title = "No Title"
                print(f"  页面标题: {page_title}")
                
                # 处理弹窗
                try:
                    if channel == "Darty":
                        if page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                            page.click("#onetrust-accept-btn-handler")
                    elif channel == "Boulanger":
                        if page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                            page.click("#onetrust-accept-btn-handler")
                        elif page.is_visible("button:has-text('Accepter')", timeout=1000):
                            page.click("button:has-text('Accepter')")
                    elif channel == "Fnac":
                        if page.is_visible("#onetrust-accept-btn-handler", timeout=2000):
                            page.click("#onetrust-accept-btn-handler")
                except:
                    pass

                # 获取价格
                if channel == "Darty":
                    price = get_darty_price(page)
                elif channel == "Boulanger":
                    price = get_boulanger_price(page)
                elif channel == "Fnac":
                    price = get_fnac_price(page)
                
                # === 自动修复判定 ===
                failed_keywords = ["404", "Not Found", "Page introuvable", "Accueil"]
                is_homepage = (channel == "Darty" and "Darty" in page_title and len(page_title) < 20) or \
                              (channel == "Boulanger" and "Boulanger" in page_title and len(page_title) < 25)
                
                if (price is None) and (is_homepage or any(k in page_title for k in failed_keywords)) and FILLER_AVAILABLE:
                    print(f"  [智能修复] 链接可能失效 (标题: {page_title})，尝试搜索新链接...")
                    new_link = None
                    if channel == "Darty":
                        new_link = get_first_result_darty(page, name)
                    elif channel == "Boulanger":
                        new_link = get_first_result_boulanger(page, name)
                        
                    if new_link and new_link != url:
                        print(f"  [成功] 找到新链接: {new_link}")
                        if update_product_link_in_csv(name, new_link):
                            print("  -> 使用新链接重试...")
                            status = "Fixed"
                            page.goto(new_link, timeout=45000)
                            time.sleep(3)
                            if channel == "Darty": price = get_darty_price(page)
                            elif channel == "Fnac": price = get_fnac_price(page)
                            else: price = get_boulanger_price(page)
                            page_title = page.title().strip()
                    else:
                        print("  [失败] 未能自动修复链接。")

            except Exception as e:
                print(f"  [Exception] 访问出错: {e}")
                if "Timeout" in str(e):
                    print(f"  [Timeout] URL: {url}")
            
            # 记录结果
            if price is not None:
                print(f"  成功获取价格: {price}")
            else:
                print(f"  未获取价格 [{page_title}]")
                # 失败截图
                if "Access Denied" not in page_title:
                    try:
                        screenshot = f"debug_{channel}_{int(time.time())}.png"
                        page.screenshot(path=screenshot)
                        print(f"  [调试] 已保存截图: {screenshot}")
                    except: pass
            
            # 写入 CSV
            # 写入 CSV (新版宽表)
            # 写入 CSV (新版长表: Brand 列支持)
            log_price_update(brand, name, channel, page_title, price, status)
                
            print("-" * 30)

        browser.close()
    print("所有任务完成。")

def log_price_update(brand, product_name, channel, page_title, price, status="Success"):
    """
    追加记录到 prices.csv (长表模式).
    格式: [Date, Time, Brand, Product Name, Platform, Price, Page Title, Status]
    """
    if not product_name or not channel: return

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    
    header = ["Date", "Time", "Brand", "Product Name", "Platform", "Price", "Page Title", "Status"]
    
    # 检查文件是否存在以决定是否写表头
    file_exists = os.path.exists(CSV_FILE)
    
    try:
        with open(CSV_FILE, 'a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(header)
                
            writer.writerow([
                date_str,
                time_str,
                brand if brand else "",
                product_name,
                channel,
                price if price is not None else "",
                page_title,
                status
            ])
            # print(f"  [记录] 数据已追加: {date_str} {time_str}")
    except Exception as e:
        print(f"  [Error] 写入 CSV 失败: {e}")

if __name__ == "__main__":
    # HEADLESS_MODE = True: 后台静默运行 (无界面)
    # HEADLESS_MODE = False: 显示浏览器界面 (调试用，能看到网页操作)
    
    # 建议：如果被反爬拦截严重，尝试设为 False 会更像真人
    HEADLESS_MODE = True 
    
    run_scraper(headless=HEADLESS_MODE)
