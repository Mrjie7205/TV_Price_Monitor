import os
import csv
import time
import requests
import asyncio
import urllib.parse
import random
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from playwright.async_api import async_playwright
from curl_cffi import requests as cffi_requests

# 引入项目中已有的获取 token 模块
from sync_feishu import get_tenant_access_token

# ================= 配置区 =================
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN")
KEYWORDS_TABLE_ID = os.environ.get("FEISHU_KEYWORDS_TABLE_ID")
NEW_ITEMS_TABLE_ID = os.environ.get("FEISHU_NEW_ITEMS_TABLE_ID")
KNOWN_PRODUCTS_CSV = "known_products.csv"
# 东八区时区
BJ_TZ = timezone(timedelta(hours=8))

# ================= 反爬伪装池与 Stealth 脚本 =================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

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
Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en', 'fr-FR', 'fr'] });
// 4. 屏蔽 chrome.runtime
if (window.chrome) { window.chrome.runtime = undefined; }
// 5. 伪造 permissions
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) : originalQuery(parameters)
);
// 6. 隐藏 Headless 特征
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

# ================= 飞书操作模块 =================
def get_feishu_keywords(token):
    """
    拉取监控配置（读取飞书表 1）
    """
    print(">>> [配置加载] 正在请求飞书检索待监控的关键词列表...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{KEYWORDS_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    
    keywords_to_monitor = []
    page_token = None
    
    try:
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
                
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                print(f" 获取关键词表失败: {result.get('msg')}")
                break
                
            data = result.get("data", {})
            items = data.get("items", [])
            
            for item in items:
                fields = item.get("fields", {})
                platform = fields.get("平台")
                keyword = fields.get("搜索关键词")
                is_active = fields.get("是否开启监控")
                
                # 兼容不同类型的数据格式 (True, "是", ["是"])
                active_flag = False
                if isinstance(is_active, bool) and is_active:
                    active_flag = True
                elif isinstance(is_active, str) and is_active.strip() in ["是", "True", "true"]:
                    active_flag = True
                elif isinstance(is_active, list) and len(is_active) > 0 and str(is_active[0]).strip() in ["是", "True", "true"]:
                    active_flag = True
                    
                if active_flag and platform and keyword:
                    keywords_to_monitor.append({
                        "platform": platform, 
                        "keyword": keyword
                    })
                    
            if data.get("has_more"):
                page_token = data.get("page_token")
            else:
                break
                
        print(f">>> [配置加载] 成功读取 {len(keywords_to_monitor)} 个待监控关键词。")
        return keywords_to_monitor
    except requests.exceptions.RequestException as e:
        print(f" 请求飞书表格异常 (网络层): {e}")
    except Exception as e:
        print(f" 读取飞书配置失败 (内部解析): {e}")
    
    return keywords_to_monitor

def push_new_items_to_feishu(token, records_to_push):
    """
    汇总与报警输出（写入飞书表 2）
    """
    if not records_to_push:
        print(">>> [结果推送] 本轮没有需要上报的新数据。")
        return
        
    print(f">>> [结果推送] 准备向飞书表写回 {len(records_to_push)} 条关键词维度的监控报告...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{NEW_ITEMS_TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    # 按照飞书要求构建 payload，保证 key 正确对应字段名
    feishu_payload = {"records": [{"fields": r} for r in records_to_push]}
    
    try:
        response = requests.post(url, headers=headers, json=feishu_payload)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            print(f">>> [结果推送] 推送成功！已写入 {len(records_to_push)} 条汇总记录。")
        else:
            print(f" 推送上新记录失败，飞书反馈信息: {result.get('msg')}")
    except Exception as e:
        print(f" 批量推送飞书表 2 异常: {e}")

# ================= 辅助反爬验证函数 =================
async def handle_bot_protection(page, keyword=""):
    """检测并处理各网站通用的防爬/Cloudflare/Datadome拦截页 (支持 Currys, Darty 等)"""
    try:
        for _ in range(8):
            content = await page.content()
            title = await page.title()
            if ("Bear with us" in title or 
                "checking your connection" in content.lower() or 
                "Verify you are human" in content or
                "Vérification de l" in content or
                "Vérification" in title):
                print(f"  [{keyword}] ⚠ 检测到平台验证层，稍作等待 5s...")
                await asyncio.sleep(5)
            else:
                return True
        return False
    except:
        return True

async def validate_title_match(title: str, keyword: str) -> bool:
    """验证商品标题，确保精准匹配品牌并过滤掉错误品类(如手机/周边)"""
    if not title or not keyword:
        return False
        
    title_lower = title.lower()
    keyword_lower = keyword.lower()
    
    # 强制校验1：标题必须包含搜索关键词的“第一核心词”（通常是品牌名，如 Samsung, Hisense）
    # 这样就能彻底挡住 LG 的电视混进 Samsung 的结果里
    brand = keyword_lower.split()[0]
    if brand not in title_lower:
        return False
        
    # 强制校验2：既然主推监控电视(TV)，我们要对同品牌下的“错乱品类”实施黑名单屏蔽
    if "tv" in keyword_lower or "téléviseur" in keyword_lower:
        # 把最容易混进来的：手机、手表、耳机、微波炉、保护壳等拉黑
        blacklist = ['galaxy', 'smartphone', 'mobile', 'watch', 'buds', 'coque', 'chargeur', 'lave-linge', 'réfrigérateur', 'frigo', 'four', 'micro-onde']
        for bad in blacklist:
            if bad in title_lower:
                return False
                
    return True

# ================= 纯 HTTP 爬取方案 (curl_cffi 绕过 TLS 指纹检测) =================
def _http_search_currys(keyword):
    """使用 curl_cffi 伪装真实浏览器进行 HTTP 请求，绕过 Cloudflare 检测"""
    products = []
    search_url = f"https://www.currys.co.uk/search/{urllib.parse.quote(keyword)}"
    
    # 随机选择一个指纹，让 curl_cffi 自己去补全 User-Agent 和所有 Request Headers
    impersonate_list = ["chrome100", "chrome104", "chrome110", "chrome116", "safari15_3", "safari15_5", "edge101"]
    impersonate_choice = random.choice(impersonate_list)
    print(f"  [Currys] 请求指纹: {impersonate_choice}")
    
    try:
        session = cffi_requests.Session(impersonate=impersonate_choice)
        # 预热主站
        session.get("https://www.currys.co.uk/", timeout=20)
        time.sleep(random.uniform(1, 4))
        
        # 搜索请求
        resp = session.get(search_url, timeout=20)
        print(f"  [Currys HTTP] 状态码: {resp.status_code}, 响应长度: {len(resp.text)}")
        
        if resp.status_code == 200 and len(resp.text) > 5000:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.select("a[href*='/products/']"):
                href = a_tag.get("href", "")
                title = a_tag.get("title", "") or a_tag.get_text(strip=True)
                if href and title and len(title) > 5:
                    url = "https://www.currys.co.uk" + href if not href.startswith("http") else href
                    products.append({"title": title, "url": url})
        else:
            print(f"  [Currys HTTP] 请求被拦截或异常 (状态码: {resp.status_code})")
            try:
                with open("debug_currys_http_response.html", "w", encoding="utf-8") as f:
                    f.write(resp.text[:5000])
            except: pass
    except Exception as e:
        print(f"  [Currys HTTP] 请求异常: {e}")
    return products

def _http_search_darty(keyword):
    """使用 curl_cffi 伪装真实浏览器进行 HTTP 请求，绕过 Datadome 检测"""
    products = []
    search_url = f"https://www.darty.com/nav/recherche?text={urllib.parse.quote(keyword)}"
    
    # 随机选择一个指纹，并禁用自动 User-Agent 强行写入错误信息，让内部 TLS 逻辑和 Header 自洽
    impersonate_list = ["chrome100", "chrome104", "chrome110", "chrome116", "safari15_3", "safari15_5", "edge101"]
    impersonate_choice = random.choice(impersonate_list)
    print(f"  [Darty] 请求指纹: {impersonate_choice}")
    
    try:
        session = cffi_requests.Session(impersonate=impersonate_choice)
        # 预热主站获得 session 各种 Cookies
        session.get("https://www.darty.com/", timeout=20)
        time.sleep(random.uniform(2, 5))
        
        # 搜索请求
        resp = session.get(search_url, timeout=20)
        print(f"  [Darty HTTP] 状态码: {resp.status_code}, 响应长度: {len(resp.text)}")
        
        if resp.status_code == 200 and len(resp.text) > 5000:
            soup = BeautifulSoup(resp.text, "html.parser")
            for sel in ["a.product_detail_link", "a[data-automation-id='product_details_link']", ".product-card__link", "div.product_list a"]:
                for a_tag in soup.select(sel):
                    href = a_tag.get("href", "")
                    title = a_tag.get("title", "") or a_tag.get_text(strip=True)
                    if href and title and len(title) > 5:
                        url = "https://www.darty.com" + href if not href.startswith("http") else href
                        products.append({"title": title, "url": url})
            
            if not products:
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if "/nav/codic/" in href or "/f-" in href:
                        title = a_tag.get("title", "") or a_tag.get_text(strip=True)
                        if title and len(title) > 5:
                            url = "https://www.darty.com" + href if not href.startswith("http") else href
                            products.append({"title": title, "url": url})
        else:
            print(f"  [Darty HTTP] 请求被拦截或异常 (状态码: {resp.status_code})")
            try:
                with open("debug_darty_http_response.html", "w", encoding="utf-8") as f:
                    f.write(resp.text[:5000])
            except: pass
    except Exception as e:
        print(f"  [Darty HTTP] 请求异常: {e}")
    return products

async def http_search_currys(keyword):
    """异步包装器：在线程池中运行同步的 curl_cffi 请求"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _http_search_currys, keyword)

async def http_search_darty(keyword):
    """异步包装器：在线程池中运行同步的 curl_cffi 请求"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _http_search_darty, keyword)

async def homepage_warmup(page, platform_url):
    """全局首页预热机制：先访问首页，接受 Cookie，滑动一下，再跳转搜索页"""
    try:
        print(f"  [全局预热] 正在访问官网首页: {platform_url} ...")
        await page.goto(platform_url, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(2.0, 4.0))
        
        # 常见 Cookie 同意逻辑 (增加 Didomi 授权捕捉，延长一点识别时间)
        for cookie_btn in [
            "#onetrust-accept-btn-handler",
            "#didomi-notice-agree-button",
            "button#didomi-notice-agree-button",
            "text=Accept all cookies",
            "button:has-text('Accept All')",
            "text=Allow all",
            "button:has-text('Allow all')",
            "text=Accepter et Fermer",
            "text=Accepter & Fermer", 
            "button:has-text('Accepter')",
            "text=Continuer sans accepter"
        ]:
            try:
                if await page.is_visible(cookie_btn, timeout=1500):
                    await page.click(cookie_btn)
                    await asyncio.sleep(1.5)
                    break 
            except: pass
            
        try: await page.keyboard.press("Escape")
        except: pass
        
        # 终极奥义：强制用 JS 剔出 DOM 树中顽固的 Cookie 黑膜遮罩，防止阻挡元素
        try:
            await page.evaluate("""
                () => {
                    document.querySelectorAll('.didomi-popup-backdrop, .onetrust-pc-dark-filter, #didomi-host, #onetrust-consent-sdk, .cookie-banner').forEach(el => el.remove());
                }
            """)
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
        
        print("  [全局预热] 官网首页热身完成。")
    except Exception as e:
        print(f"  [全局预热] 官网首页加载抛错 (容错忽略): {e}")

# ================= 核心爬虫模块 (Async) =================
async def search_scraper_async(page, platform, keyword):
    """
    通用搜索爬虫框架 (基于 Playwright 挂载的反爬应对)
    返回结构: ([{"title": "商品标题", "url": "商品绝对链接"}, ...], 大盘总商品数)
    """
    print(f">>> [爬虫执行] 正在开始检索。平台: {platform}，关键词: '{keyword}' ...")
    products = []
    total_found_count = None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    platform_lower = str(platform).lower()
    
    try:
        # ---- Amazon UK 分支 ----
        if "amazon" in platform_lower:
            try:
                if "amazon.co.uk" not in page.url and "amazon.com" not in page.url:
                    try: await page.goto("https://www.amazon.co.uk", wait_until='domcontentloaded', timeout=30000)
                    except Exception as e: print(f"  [继续提取] 预载导航超时: {e}")
                    await asyncio.sleep(random.uniform(1, 3))
                
                try:
                    if await page.is_visible("#sp-cc-accept", timeout=3000):
                        await page.click("#sp-cc-accept")
                        await asyncio.sleep(1)
                except: pass

                search_input = page.locator("#twotabsearchtextbox").first
                if await search_input.is_visible():
                    await search_input.click()
                    await search_input.fill("")
                    await asyncio.sleep(0.5)
                    await page.keyboard.type(keyword, delay=100)
                    await asyncio.sleep(0.5)
                    await page.keyboard.press("Enter")
                else:
                    url = f"https://www.amazon.co.uk/s?k={urllib.parse.quote(keyword)}"
                    try: await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    except Exception as e: print(f"  [继续提取] 预载导航超时: {e}")

                await asyncio.sleep(4)

                links = await page.locator("div.s-main-slot a[href*='/dp/']").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "slredirect" not in href and "#" not in href and "/dp/" in href:
                        url = "https://www.amazon.co.uk" + href if not href.startswith("http") else href
                        
                        parent_h2 = await page.evaluate_handle("el => el.closest('div.s-result-item').querySelector('h2')", link)
                        title_text = ""
                        if parent_h2: 
                            title_text = await (await parent_h2.get_property("innerText")).json_value()
                        if title_text and url and await validate_title_match(title_text, keyword):
                            products.append({"title": title_text.strip(), "url": url})
                
                if not products:
                    try: await page.screenshot(path=f"error_screenshot_empty_amazon_{keyword}.png", full_page=True)
                    except: pass
            except Exception as e:
                print(f"  [Amazon搜索失败] {e}")
                try: await page.screenshot(path=f"error_screenshot_amazon_{keyword}.png", full_page=True)
                except: pass

        # ---- Currys 分支 (HTTP优先 + Playwright兜底) ----
        elif "currys" in platform_lower:
            # === 第一层：使用 curl_cffi 纯 HTTP 请求绕过 Cloudflare TLS 指纹 ===
            try:
                print("  [Currys] 策略1: 启用 curl_cffi 纯HTTP模式(伪造Chrome TLS指纹)...")
                http_results = await http_search_currys(keyword)
                for item in http_results:
                    if await validate_title_match(item["title"], keyword):
                        products.append(item)
                if products:
                    print(f"  [Currys] HTTP模式成功! 获取到 {len(products)} 条匹配商品。")
            except Exception as e:
                print(f"  [Currys] HTTP模式异常: {e}")
            
            # === 第二层兜底：如果 HTTP 失败，尝试 Playwright 浏览器 ===
            if not products:
                print("  [Currys] HTTP模式未获取到结果，启用 Playwright 浏览器兜底...")
                try:
                    await homepage_warmup(page, "https://www.currys.co.uk")
                    await handle_bot_protection(page, keyword)
                    await asyncio.sleep(random.uniform(1, 2))
                    
                    search_input = None
                    for selector in ["#search", "input[name='q']", "input[data-test='search-input']", "input[name='search']", "input[type='search']"]:
                        try:
                            loc = page.locator(selector).first
                            if await loc.count() > 0 and await loc.is_visible():
                                search_input = loc
                                break
                        except: pass
                    
                    if search_input:
                        # 强制清除遮罩可能残留的风险，直接调用 focus() 并启用 force
                        try: await search_input.evaluate("el => el.focus()")
                        except: pass
                        await search_input.click(force=True)
                        await asyncio.sleep(0.5)
                        await search_input.fill("", force=True)
                        await search_input.press_sequentially(keyword, delay=150)
                        await asyncio.sleep(0.5)
                        await search_input.press("Enter")
                        try: await page.wait_for_load_state("domcontentloaded", timeout=15000)
                        except: pass
                        await asyncio.sleep(4)
                    else:
                        search_url = f"https://www.currys.co.uk/search/{urllib.parse.quote(keyword)}"
                        try: await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
                        except: pass
                        await handle_bot_protection(page, keyword)
                        await asyncio.sleep(3)
                    
                    links = await page.locator("a[href*='/products/']").all()
                    for link_locator in links:
                        href = await link_locator.get_attribute("href")
                        title = await link_locator.inner_text()
                        if href and await validate_title_match(title, keyword):
                            url = "https://www.currys.co.uk" + href if not href.startswith("http") else href
                            products.append({"title": title.strip(), "url": url})
                            
                    if not products:
                        try: await page.screenshot(path=f"error_screenshot_empty_currys_{keyword}.png", full_page=True)
                        except: pass
                except Exception as e:
                    print(f"  [Currys Playwright兜底失败] {e}")
                    try: await page.screenshot(path=f"error_screenshot_currys_{keyword}.png", full_page=True)
                    except: pass

        # ---- Boulanger 分支 ----
        elif "boulanger" in platform_lower:
            try:
                # Boulanger 恢复使用人类模拟策略：进入首页，预热，在搜索框中键入
                await homepage_warmup(page, "https://www.boulanger.com/")
                await asyncio.sleep(2)
                
                print(f"  [Boulanger] 尝试使用搜索框查词: {keyword}")
                search_input = None
                for selector in ["input[name='tr']", "#search-input", "input.search-input", "input[type='search']", "input[placeholder*='Rechercher']"]:
                    try:
                        loc = page.locator(selector).first
                        if await loc.count() > 0 and await loc.is_visible():
                            search_input = loc
                            break
                    except: pass
                
                if search_input:
                        try: await search_input.evaluate("el => el.focus()")
                        except: pass
                        await search_input.click(force=True)
                        await asyncio.sleep(0.5)
                        await search_input.fill("", force=True)
                        await search_input.press_sequentially(keyword, delay=150)
                        await asyncio.sleep(0.5)
                        await search_input.press("Enter")
                        try: await page.wait_for_load_state("domcontentloaded", timeout=15000)
                        except: pass
                        
                        # 给页面留出通过路由重定向的时间 (Boulanger的智能搜索可能会重定向到 /c/televiseur/...)
                        await asyncio.sleep(4)
                else:
                    # 兜底直接跳转
                    search_url = f"https://www.boulanger.com/resultats?tr={urllib.parse.quote(keyword)}"
                    print(f"  [Boulanger] 未找到输入框，使用 URL 跳转: {search_url}")
                    try: await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
                    except: pass
                    await asyncio.sleep(4)

                # 尝试提前挖掘页面的“大盘总数量”
                try:
                    # 寻找包含 "(123 articles)" 的标题，如："Téléviseur Samsung (123 articles)"
                    h1_text = await page.locator("h1").inner_text(timeout=2000)
                    match = re.search(r'\(?(\d+)\s*article', h1_text, re.IGNORECASE)
                    if match:
                        total_found_count = int(match.group(1))
                except: pass

                # 分页循环机制，最多允许发现翻页异常前进行更多页次 (如放宽到 10 页)
                previous_count = 0
                for page_num in range(1, 10):
                    # === 平滑滚动懒加载机制 ===
                    try:
                        print(f"  [Boulanger] 第 {page_num} 页: 触发深层平滑滚动，挖掘隐藏商品...")
                        previous_height = 0
                        scroll_attempts = 0
                        # 增加深层滚动次数，保证 40 个卡片能被完整划过到底部触发加载
                        while scroll_attempts < 12:
                            await page.evaluate("window.scrollBy(0, 1200)")
                            await asyncio.sleep(random.uniform(1.0, 2.0))
                            new_height = await page.evaluate("document.body.scrollHeight")
                            if new_height == previous_height:
                                break
                            previous_height = new_height
                            scroll_attempts += 1
                    except Exception as sc_e:
                        print(f"  [Boulanger] 滚动报错: {sc_e}")

                    # 提取列表元素 (拒绝 innerText 暴力兜底)
                    links = await page.locator("a[href*='/ref/']:visible").all()
                    
                    raw_urls_this_page = set()
                    
                    for link_locator in links:
                        try:
                            if not await link_locator.is_visible(): continue
                            href = await link_locator.get_attribute("href")
                            if href and "avis" not in href.lower() and "?" not in href:
                                url = "https://www.boulanger.com" + href if not href.startswith("http") else href
                                raw_urls_this_page.add(url)
                                
                                desc = await link_locator.get_attribute("title")
                                if not desc:
                                    desc = await link_locator.evaluate("""el => {
                                        let titleEl = el.querySelector('h2, h3, .product-designation, .product-label');
                                        return titleEl ? titleEl.innerText : null;
                                    }""")
                                
                                if not desc or len(desc.strip()) < 3:
                                    continue
                                    
                                desc_clean = " ".join(desc.split()).strip()
                                if desc_clean and url and await validate_title_match(desc_clean, keyword):
                                    products.append({"title": desc_clean, "url": url})
                                    # 利用字典推导去重，防止深度滚动重复抓取同一卡片
                                    products = list({p['url']: p for p in products}.values())
                        except Exception as loop_e:
                            print(f"    [忽略] Boulanger 单个卡片解析异常: {loop_e}")
                            continue

                    current_total = len(products)
                    delta = current_total - previous_count
                    raw_count = len(raw_urls_this_page)
                    
                    print(f"  [Boulanger] 第 {page_num} 页发现 {raw_count} 个原始商品卡片，过滤后提取到 {delta} 个符合条件的商品。目前累计: {current_total}")
                    
                    # 修复 Bug: 使用原始页面上真实存在的商品卡片数量来推算是否到底
                    # 而并不是基于 validate_title_match 过滤后的商品数 (delta) 进行推算
                    # 如果 delta < 40 就 break，因为有些相关配件会被过滤掉，会引发最后一页提前停止
                    if raw_count < 25:
                        print(f"  [Boulanger] 本页原始商品数量 ({raw_count}) 小于满页安全阈值，基本确认已到底，停止翻页。")
                        break
                        
                    previous_count = current_total
                    
                    # === 改用基于 URL 注入特征参数的极简翻页法 ===
                    next_page = page_num + 1
                    current_url = page.url
                    # 解析当前 URL，提取 Query 然后加入 / 修改 numPage
                    parsed_url = urllib.parse.urlparse(current_url)
                    query_dict = urllib.parse.parse_qs(parsed_url.query)
                    query_dict['numPage'] = [str(next_page)]
                    
                    new_query = urllib.parse.urlencode(query_dict, doseq=True)
                    next_url = urllib.parse.urlunparse((
                        parsed_url.scheme, 
                        parsed_url.netloc, 
                        parsed_url.path, 
                        parsed_url.params, 
                        new_query, 
                        parsed_url.fragment
                    ))
                    
                    print(f"  [Boulanger] 自动拼接好下一页 URL，准备直达第 {next_page} 页: {next_url}")
                    try:
                        await page.goto(next_url, wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(random.uniform(2.5, 4.0))
                    except Exception as goto_e:
                        print(f"  [Boulanger] 翻页跳转异常: {goto_e}")
                        break
                            
                if not products:
                    try: await page.screenshot(path=f"error_screenshot_empty_boulanger_{keyword}.png", full_page=True)
                    except: pass
            except Exception as e:
                print(f"  [Boulanger搜索失败] {e}")
                try: await page.screenshot(path=f"error_screenshot_boulanger_{keyword}.png", full_page=True)
                except: pass

        # ---- Darty 分支 (HTTP优先 + Playwright兜底) ----
        elif "darty" in platform_lower:
            # === 第一层：使用 curl_cffi 纯 HTTP 请求绕过 Datadome TLS 指纹 ===
            try:
                print("  [Darty] 策略1: 启用 curl_cffi 纯HTTP模式(伪造Chrome TLS指纹)...")
                http_results = await http_search_darty(keyword)
                for item in http_results:
                    if await validate_title_match(item["title"], keyword):
                        products.append(item)
                if products:
                    print(f"  [Darty] HTTP模式成功! 获取到 {len(products)} 条匹配商品。")
            except Exception as e:
                print(f"  [Darty] HTTP模式异常: {e}")
            
            # === 第二层兜底：如果 HTTP 失败，尝试 Playwright 浏览器 ===
            if not products:
                print("  [Darty] HTTP模式未获取到结果，启用 Playwright 浏览器兜底...")
                try:
                    await homepage_warmup(page, "https://www.darty.com")
                    await handle_bot_protection(page, keyword)
                    
                    search_input = None
                    for selector in ["#darty_search_main_input", ".search-bar__input", "input[type='search']", "input[name='text']"]:
                        try:
                            loc = page.locator(selector).first
                            if await loc.count() > 0 and await loc.is_visible():
                                search_input = loc
                                break
                        except: pass
                    
                    if search_input:
                        try: await search_input.evaluate("el => el.focus()")
                        except: pass
                        await search_input.click(force=True)
                        await asyncio.sleep(0.5)
                        await search_input.fill("", force=True)
                        await search_input.press_sequentially(keyword, delay=150)
                        await asyncio.sleep(0.5)
                        await search_input.press("Enter")
                        try: await page.wait_for_load_state("domcontentloaded", timeout=15000)
                        except: pass
                        await asyncio.sleep(3)
                    else:
                        search_url = f"https://www.darty.com/nav/recherche?text={urllib.parse.quote(keyword)}"
                        try: await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
                        except: pass
                        await handle_bot_protection(page, keyword)
                        await asyncio.sleep(3)
                    
                    selectors = [
                        ".product_detail_link", 
                        "a[data-automation-id='product_details_link']",
                        ".product-card__link",
                        "div.product_list a" 
                    ]
                    for sel in selectors:
                        locators = await page.locator(sel).all()
                        for loc in locators:
                            if await loc.is_visible():
                                href = await loc.get_attribute("href")
                                if href:
                                    url = "https://www.darty.com" + href if not href.startswith("http") else href
                                    title = await loc.inner_text()
                                    if await validate_title_match(title, keyword):
                                        products.append({"title": title.strip(), "url": url})
                                    
                    if not products:
                        try: await page.screenshot(path=f"error_screenshot_empty_darty_{keyword}.png", full_page=True)
                        except: pass
                except Exception as e:
                    print(f"  [Darty Playwright兜底失败] {e}")
                    try: await page.screenshot(path=f"error_screenshot_darty_{keyword}.png", full_page=True)
                    except: pass

        # ---- Fnac 分支 ----
        elif "fnac" in platform_lower:
            search_url = f"https://www.fnac.com/SearchResult/ResultList.aspx?Search={urllib.parse.quote(keyword)}"
            try:
                try: await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
                except Exception as e: print(f"  [继续提取] 搜索导航超时: {e}")
                await asyncio.sleep(2)
                try:
                    if await page.is_visible("#onetrust-accept-btn-handler", timeout=3000):
                        await page.click("#onetrust-accept-btn-handler")
                except: pass
                    
                potential_links = await page.locator("article a").all()
                for link in potential_links:
                    href = await link.get_attribute("href")
                    title = await link.inner_text()
                    if href:
                        url = "https://www.fnac.com" + href if not href.startswith("http") else href
                        if ("/a" in url or "/mp" in url) and not "avis" in url:
                            if await validate_title_match(title, keyword):
                                products.append({"title": title.strip(), "url": url})
                                
                if not products:
                    try: await page.screenshot(path=f"error_screenshot_empty_fnac_{keyword}.png", full_page=True)
                    except: pass
            except Exception as e:
                print(f"  [Fnac搜索失败] {e}")
                try: await page.screenshot(path=f"error_screenshot_fnac_{keyword}.png", full_page=True)
                except: pass

        # ---- 兜底逻辑：无特定规则的平台 ----
        else:
            print(f" 提示: {platform} 未配置专属爬虫规则。此版本暂不支持通过 Playwright 通杀未知平台。")

    except Exception as e:
        print(f"!!! 提取解析总控异常 ({platform} - {keyword}): {e}")
        try: await page.screenshot(path=f"error_screenshot_fatal_{platform}.png", full_page=True)
        except: pass
        
    # 去重处理（避免抓到同页面的重复挂载链接），这里基于 URL 去重
    unique_products = {p['url']: p for p in products}.values()
    final_list = list(unique_products)
    
    print(f">>> [爬虫执行] 搜索完毕，共抓取到 {len(final_list)} 个去重候选记录。大盘提取量: {total_found_count if total_found_count else '未知'}")
    return final_list, total_found_count

# ================= 本地状态校验模块 =================
def load_known_products():
    """读取已知的商品记忆库，返回一个集合"""
    known_urls = set()
    if not os.path.exists(KNOWN_PRODUCTS_CSV):
        return known_urls
        
    try:
        with open(KNOWN_PRODUCTS_CSV, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("Product URL")
                if url:
                    known_urls.add(url.strip())
    except Exception as e:
        print(f" 加载本地 CSV 记忆库异常: {e}")
        
    print(f">>> [本地校验] 成功加载 {len(known_urls)} 条历史商品记录作为记忆库。")
    return known_urls

def append_new_products(new_items):
    """将新发现的商品追加写入本地 CSV 记忆库"""
    if not new_items:
        return
        
    file_exists = os.path.exists(KNOWN_PRODUCTS_CSV)
    try:
        with open(KNOWN_PRODUCTS_CSV, mode="a", encoding="utf-8-sig", newline="") as f:
            fieldnames = ["Platform", "Keyword", "Product Title", "Product URL"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
                
            for item in new_items:
                writer.writerow(item)
        print(f">>> [记忆存储] 本次新发现的 {len(new_items)} 件商品已成功写入备忘库。")
    except Exception as e:
        print(f" 追加保存本地 CSV 记忆库时异常: {e}")

# ================= 主控制流程 =================
async def run_monitor_async():
    # 环境自检
    if not KEYWORDS_TABLE_ID or not NEW_ITEMS_TABLE_ID or not APP_TOKEN:
        print(" 错误: 请确保环境变量 FEISHU_KEYWORDS_TABLE_ID， FEISHU_NEW_ITEMS_TABLE_ID，以及 FEISHU_APP_TOKEN 已正确配置。")
        return
        
    # 优先抓取 token (复用了旧组件)
    token = get_tenant_access_token()
    if not token:
        print(" 错误: 无法获取飞书全局凭配(Token)，程序终止。")
        return
        
    # 1. 下载待监控项配置表
    keywords_list = get_feishu_keywords(token)
    if not keywords_list:
        print(" 没有获取到任何需要处于开启状态的监控关键词，程序即刻退出。")
        return
        
    # 2. 预热本地的知识过滤库
    known_urls = load_known_products()
    
    feishu_report_records = []
    all_new_csv_items = []
    
    # 初始化 Playwright 无头浏览器环境
    USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    
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
            # 优先调用系统自带的原生 Chrome，这样可以直接共享用户的代理配置，启用有头模式方便排查
            browser = await p.chromium.launch(headless=True, channel="chrome", args=browser_args)
        except Exception as e:
            # 退避策略
            print(f"  [引擎提示] 尝试调用系统原生 Chrome 失败，退回 Playwright 默认内核下载版。{e}")
            browser = await p.chromium.launch(headless=True, args=browser_args)
            
        # 不多开context了，复用同一个context模拟人类行为，但记得清理缓存
        # 3. 逐个进行爬虫处理和比对 (独立 Context 避免交叉污染)
        for job in keywords_list:
            platform = job["platform"]
            keyword = job["keyword"]
            
            # --- 为了防止前一个关键词被目标网站拦截后把 "连坐惩罚" 带入下一个关键词的搜索 ---
            # 每次新词建立一个全新的无痕迹 Context
            ua = random.choice(USER_AGENTS)
            context = await browser.new_context(
                user_agent=ua,
                viewport={'width': random.choice([1920, 1366, 1440, 1536]), 'height': random.choice([1080, 768, 900])},
                locale="fr-FR",  # Boulanger/Darty 等法国平台倾向于看到法语 local
                timezone_id="Europe/Paris"
            )
            await context.add_init_script(STEALTH_JS)
            page = await context.new_page()
            
            # 使用基于 playwright 异步机制的方法抓取
            scraped_products, total_found = await search_scraper_async(page, platform, keyword)
            # 优先使用网页上官方标示的大盘总数据，如果没提取到则用爬到的本页明细代替
            total_scraped = total_found if total_found is not None else len(scraped_products)
            
            await context.close()  # 打完收工，销毁伪造身份
        
            # 挑选新商品 (修正缩进，使其并入每个关键词的循环)
            new_items = []
            for p in scraped_products:
                # 过滤乱码和换行符保证 CSV 干净整洁
                clean_title = p["title"].replace('\n', ' ').replace('\r', ' ').strip()
                p["title"] = clean_title
                # 判断逻辑：只要链接不在记忆库就算“上新”
                if p["url"] not in known_urls:
                    new_items.append(p)
                    known_urls.add(p["url"])  # 同批次加缓存排重
                    
                    # 为 CSV 持久化收集素材
                    all_new_csv_items.append({
                        "Platform": platform,
                        "Keyword": keyword,
                        "Product Title": p["title"],
                        "Product URL": p["url"]
                    })
                    
            # 拼装给飞表的上报行
            new_count = len(new_items)
            detail_lines = []
            if new_count == 0:
                detail_text = "今日无上新"
            else:
                for item in new_items:
                    detail_lines.append(f"- 标题：{item['title']} \n  链接：{item['url']}")
                detail_text = "\n".join(detail_lines)
                
            # 根据需求映射飞书字段
            record_fields = {
                "日期": int(datetime.now(BJ_TZ).timestamp() * 1000),
                "平台": platform,
                "关键词": keyword,
                "大盘商品数": total_scraped,
                "上新数量": new_count,
                "上新清单详情": detail_text
            }
            feishu_report_records.append(record_fields)
            
            # 为了防护策略，每次任务结束后做一段随机休眠
            await asyncio.sleep(random.uniform(2, 4))
            
    # 彻底关闭游览器
    await browser.close()
        
    # 4. 把更新记忆回写硬盘
    append_new_products(all_new_csv_items)
    
    # 5. 上传最终报表
    push_new_items_to_feishu(token, feishu_report_records)
    print(">>> [整体流程] 执行完毕，全部监控项已处理。")

if __name__ == "__main__":
    asyncio.run(run_monitor_async())
