import csv
import os
import time
import random
import urllib.parse
import asyncio
import re
from playwright.async_api import async_playwright

# 基础配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "products.csv")
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

STEALTH_JS = """
// 1. 屏蔽 webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 2. 伪造 plugins
Object.defineProperty(navigator, 'plugins', {
    get: () => [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
        { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
    ]
});

// 3. 伪造 languages
Object.defineProperty(navigator, 'languages', { get: () => ['de-DE', 'de', 'en-US', 'en'] });
Object.defineProperty(navigator, 'language', { get: () => 'de-DE' });

// 4. 屏蔽 chrome.runtime
if (window.chrome) {
    window.chrome.runtime = undefined;
}

// 5. 伪造 permissions
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
);

// 6. 隐藏 Headless 特征
Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth });
Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight + 85 });
"""
# ================= 辅助验证函数 =================

def validate_link(link, keyword, page_title=""):
    """
    验证搜索到的链接是否与关键词匹配。
    逻辑：检查关键词中的重要部分（如品牌和型号）是否出现在链接或页面标题中。
    """
    if not link: return False
    
    parts = [p.strip().lower() for p in keyword.split() if len(p.strip()) >= 2]
    if not parts: return True

    link_lower = link.lower()
    title_lower = page_title.lower() if page_title else ""
    
    brand = parts[0]
    import re
    def has_word(w, text):
        t = text.replace("-", " ").replace("/", " ")
        # Insert space between numbers and letters (e.g. 65u8q -> 65 u8 q) to allow word boundaries to match correctly
        # when User introduces artificial spaces in product names but the URLs are consolidated.
        t = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', t)
        t = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', t)
        w_clean = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', w)
        w_clean = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', w_clean)
        return bool(re.search(r'\b' + re.escape(w) + r'\b', t)) or bool(re.search(r'\b' + re.escape(w_clean) + r'\b', t))
        
    if not has_word(brand, link_lower) and not has_word(brand, title_lower) and brand not in link_lower.replace("-", ""):
        print(f"    [链接被拒] 搜索: {keyword} | 找到标题: {page_title} | 原因: 缺失核心品牌 [{brand}]")
        return False
    
    matches = 0
    model_matches = 0
    for i, p in enumerate(parts):
        if len(p) <= 3 or p in ["pro", "max", "ultra", "plus"]:
            if has_word(p, link_lower) or has_word(p, title_lower):
                matches += 1
                if i > 0: model_matches += 1
        else:
            if p in link_lower.replace("-", "") or p in title_lower:
                matches += 1
                if i > 0: model_matches += 1
            
    if matches == 0:
        print(f"    [链接被拒] 搜索: {keyword} | 找到标题: {page_title} | 原因: 关键词全军覆没")
        return False
    
    if len(parts) >= 2 and model_matches == 0:
        print(f"    [链接被拒] 搜索: {keyword} | 找到标题: {page_title} | 原因: 仅匹配到品牌，未匹配到核心型号")
        return False
    
    anti_keywords = ["dji", "drone", "mavic", "fly-more", "lave-linge", "washing machine", "frigo", "réfrigérateur", "refrigerator", "four", "oven", "aspirateur", "vacuum", "micro-ondes", "smartphone", "galaxy", "hue", "bulb", "light", "ampoule", "zubehor", "zubehör"]
    if any(k in keyword.lower() for k in ["samsung", "tcl", "hisense", "tv", "monitor", "écran"]):
        if any(ak in link_lower or ak in title_lower for ak in anti_keywords):
            print(f"    [链接被拒] 搜索: {keyword} | 找到标题: {page_title} | 原因: 触碰家电黑名单")
            return False

    return True

async def handle_antibot_page(page, keyword=""):
    """检测并处理各电商网站的反爬拦截页"""
    try:
        for _ in range(4):
            content = await page.content()
            title = await page.title()
            title_lower = title.lower()
            content_lower = content.lower()
            
            is_bot_page = (
                "bear with us" in title_lower or 
                "checking your connection" in content_lower or 
                "verify you are human" in content_lower or
                "just a moment" in title_lower or
                "ein moment" in title_lower or
                "access denied" in title_lower or
                "attention required" in title_lower or
                "cloudflare" in title_lower
            )
            
            if is_bot_page:
                print(f"  [{keyword}] ⚠ 检测到 Anti-Bot 拦截页 ({title})，等待 5s...")
                await asyncio.sleep(5)
            else:
                return True
        return False
    except:
        return True

# ================= 搜索函数 (Async) =================

async def get_first_result_darty(page, keyword):
    """在 Darty 搜索并提取第一个结果"""
    print(f"  正在 Darty 搜索: {keyword} ...")
    search_url = f"https://www.darty.com/nav/recherche?text={urllib.parse.quote(keyword)}"
    
    try:
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
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
                    link = await loc.get_attribute("href")
                    if link:
                        if not link.startswith("http"):
                            link = "https://www.darty.com" + link
                        
                        # 验证链接
                        if validate_link(link, keyword):
                            print(f"  -> 找到链接: {link}")
                            return link
    except Exception as e:
        print(f"  [Darty搜索失败] {e}")
    return None

async def get_first_result_boulanger(page, keyword):
    """在 Boulanger 搜索并提取第一个结果 (模拟人工)"""
    print(f"  正在 Boulanger 搜索: {keyword} ...")
    try:
        if "boulanger.com" not in page.url or "resultats" in page.url or "Oups" in await page.title():
            await page.goto("https://www.boulanger.com", wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(random.uniform(2, 4))

        try:
            if await page.is_visible("#onetrust-accept-btn-handler", timeout=3000):
                await page.click("#onetrust-accept-btn-handler")
                await asyncio.sleep(1)
        except: pass

        search_input = None
        for selector in ["input[name='tr']", "#searching", "input[type='search']"]:
            if await page.locator(selector).count() > 0:
                search_input = page.locator(selector).first
                break
        
        if search_input and await search_input.is_visible():
            await search_input.click()
            await search_input.fill("")
            await asyncio.sleep(0.5)
            await page.keyboard.type(keyword, delay=100) 
            await asyncio.sleep(0.5)
            
            # 记录当前 URL 以确认跳转
            old_url = page.url
            await page.keyboard.press("Enter")
            
            # 强化等待：确保进入真实搜索页或直接进入产品详情
            try:
                # 等待 URL 变化或主容器出现，最多等待 10s
                await page.wait_for_function(
                    f"url => url !== '{old_url}' && (url.includes('resultats') || url.includes('/ref/'))",
                    old_url, timeout=10000
                )
            except:
                # 如果没能成功检测到 URL 变化，回退到普通等待
                await page.wait_for_load_state("domcontentloaded")
            
            await asyncio.sleep(2)
        else:
            search_url = f"https://www.boulanger.com/resultats?tr={urllib.parse.quote(keyword)}"
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)

        current_url = page.url
        current_title = await page.title()
        
        # 检查是否直接跳转
        if "/ref/" in current_url:
            if validate_link(current_url, keyword, current_title):
                return current_url

        # 提取结果列表，强制过滤可见元素，防止抓到建议层的隐藏内容
        links = await page.locator("a[href*='/ref/']:visible").all()
        for link_locator in links:
            if not await link_locator.is_visible():
                continue
                
            href = await link_locator.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = "https://www.boulanger.com" + href
                
                # 获取该链接可能的文本描述
                desc = await link_locator.inner_text()
                if validate_link(href, keyword, desc):
                    return href

    except Exception as e:
        print(f"  [Boulanger搜索失败] {e}")
    return None

async def get_first_result_amazon(page, keyword):
    """在 Amazon UK 搜索并提取第一个结果"""
    print(f"  正在 Amazon UK 搜索: {keyword} ...")
    try:
        if "amazon.co.uk" not in page.url:
            await page.goto("https://www.amazon.co.uk", wait_until='domcontentloaded', timeout=30000)
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
            await page.goto(url, wait_until='domcontentloaded')

        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(3)

        links = await page.locator("div.s-main-slot a[href*='/dp/']").all()
        for link in links:
            href = await link.get_attribute("href")
            if href and "slredirect" not in href and "#" not in href and "/dp/" in href:
                if not href.startswith("http"):
                    href = "https://www.amazon.co.uk" + href
                
                # 检查标题
                parent_h2 = await page.evaluate_handle("el => el.closest('div.s-result-item').querySelector('h2')", link)
                title_text = ""
                if parent_h2: 
                    title_text = await (await parent_h2.get_property("innerText")).json_value()
                
                if validate_link(href, keyword, title_text):
                    print(f"  -> 找到链接: {href}")
                    return href
    except Exception as e:
        print(f"  [Amazon搜索失败] {e}")
    return None

async def get_first_result_fnac(page, keyword):
    """在 Fnac 搜索并提取第一个结果"""
    print(f"  正在 Fnac 搜索: {keyword} ...")
    search_url = f"https://www.fnac.com/SearchResult/ResultList.aspx?Search={urllib.parse.quote(keyword)}"
    try:
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
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
                if not href.startswith("http"): 
                    href = "https://www.fnac.com" + href
                
                if ("/a" in href or "/mp" in href) and not "avis" in href:
                    if validate_link(href, keyword, title):
                        print(f"  -> 找到链接: {href}")
                        return href
    except Exception as e:
        print(f"  [Fnac搜索失败] {e}")
    return None

async def get_first_result_currys(page, keyword):
    """在 Currys.co.uk 搜索并提取第一个结果"""
    print(f"  正在 Currys 搜索: {keyword} ...")
    try:
        await page.goto("https://www.currys.co.uk", wait_until='domcontentloaded', timeout=30000)
        await handle_antibot_page(page, keyword)
        await asyncio.sleep(random.uniform(1, 2))
        
        try:
            if await page.is_visible("#onetrust-accept-btn-handler", timeout=3000):
                await page.click("#onetrust-accept-btn-handler")
                await asyncio.sleep(1)
        except: pass
        
        search_input = None
        for selector in ["input[name='search']", "input[type='search']", "input[data-test='search-input']"]:
            try:
                loc = page.locator(selector).first
                if await loc.count() > 0 and await loc.is_visible():
                    search_input = loc
                    break
            except: pass
        
        if search_input:
            await search_input.click()
            await search_input.fill("")
            await asyncio.sleep(0.5)
            await page.keyboard.type(keyword, delay=80)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(4)
        else:
            search_url = f"https://www.currys.co.uk/search/{urllib.parse.quote(keyword)}"
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await handle_antibot_page(page, keyword)
            await asyncio.sleep(3)
        
        current_url = page.url
        # 验证直接跳转
        if "/products/" in current_url:
            if validate_link(current_url, keyword, await page.title()):
                return current_url
        
        # 查找结果
        links = await page.locator("a[href*='/products/']").all()
        for link_locator in links:
            href = await link_locator.get_attribute("href")
            title = await link_locator.inner_text()
            if href:
                if not href.startswith("http"):
                    href = "https://www.currys.co.uk" + href
                
                if validate_link(href, keyword, title):
                    print(f"  -> Validated link: {href}")
                    return href
        
    except Exception as e:
        print(f"  [Currys搜索失败] {e}")
    return None

async def get_first_result_mediamarkt(page, keyword):
    """在 MediaMarkt.de 搜索并提取第一个结果"""
    print(f"  正在 MediaMarkt 搜索: {keyword} ...")
    try:
        await page.goto("https://www.mediamarkt.de", wait_until='domcontentloaded', timeout=30000)
        await handle_antibot_page(page, keyword)
        await asyncio.sleep(random.uniform(1.5, 3.0))

        # 接受 Cookie（德语按钮）
        for btn in ["[data-testid='mms-accept-all-button']", "button:has-text('Alle akzeptieren')", "button:has-text('Akzeptieren')", "#onetrust-accept-btn-handler"]:
            try:
                if await page.is_visible(btn, timeout=2000):
                    await page.click(btn)
                    await asyncio.sleep(1)
                    break
            except: pass

        # 尝试搜索框
        search_input = None
        for sel in ["input[data-test='mms-search-input']", "input[name='query']", "input[placeholder*='Suchen']", "input[type='search']"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    search_input = loc
                    break
            except: pass

        if search_input:
            await search_input.click(force=True)
            await search_input.fill("")
            await asyncio.sleep(0.3)
            await page.keyboard.type(keyword, delay=100)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            try: await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except: pass
            await asyncio.sleep(3)
        else:
            search_url = f"https://www.mediamarkt.de/search?query={urllib.parse.quote(keyword)}"
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await handle_antibot_page(page, keyword)
            await asyncio.sleep(3)

        # 提取结果链接
        selectors = ["a[href*='/product/']", "a[data-test='mms-router-link']", "article a", "li a[href*='/de/product/']"]
        for sel in selectors:
            locators = await page.locator(sel).all()
            for loc in locators:
                if await loc.is_visible():
                    href = await loc.get_attribute("href")
                    if href and "/product/" in href:
                        if not href.startswith("http"):
                            href = "https://www.mediamarkt.de" + href
                        title = await loc.inner_text()
                        if validate_link(href, keyword, title):
                            print(f"  -> 找到链接: {href}")
                            return href
    except Exception as e:
        print(f"  [MediaMarkt搜索失败] {e}")
    return None


async def get_first_result_coolblue(page, keyword):
    """在 Coolblue.de 搜索并提取第一个结果"""
    print(f"  正在 Coolblue 搜索: {keyword} ...")
    try:
        await page.goto("https://www.coolblue.de", wait_until='domcontentloaded', timeout=30000)
        await handle_antibot_page(page, keyword)
        await asyncio.sleep(random.uniform(1.5, 3.0))

        # 接受 Cookie
        for btn in ["button:has-text('Akzeptieren')", "button:has-text('Alle akzeptieren')", "#onetrust-accept-btn-handler", "[data-test='accept-cookies']"]:
            try:
                if await page.is_visible(btn, timeout=2000):
                    await page.click(btn)
                    await asyncio.sleep(1)
                    break
            except: pass

        # 尝试搜索框
        search_input = None
        for sel in ["input[data-test='search-input']", "input[name='query']", "input[placeholder*='Suchen']", "input[type='search']"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    search_input = loc
                    break
            except: pass

        if search_input:
            await search_input.click(force=True)
            await search_input.fill("")
            await asyncio.sleep(0.3)
            await page.keyboard.type(keyword, delay=100)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            try: await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except: pass
            await asyncio.sleep(3)
        else:
            search_url = f"https://www.coolblue.de/de/suche?query={urllib.parse.quote(keyword)}"
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await handle_antibot_page(page, keyword)
            await asyncio.sleep(3)

        # 提取结果链接
        selectors = ["a[href*='/product/']", "a[href*='/produkt/']", "li[data-test='product'] a", "article a"]
        for sel in selectors:
            locators = await page.locator(sel).all()
            for loc in locators:
                if await loc.is_visible():
                    href = await loc.get_attribute("href")
                    if href and ("/product/" in href or "/produkt/" in href):
                        if not href.startswith("http"):
                            href = "https://www.coolblue.de" + href
                        title = await loc.inner_text()
                        if validate_link(href, keyword, title):
                            print(f"  -> 找到链接: {href}")
                            return href
    except Exception as e:
        print(f"  [Coolblue搜索失败] {e}")
    return None


# ================= 辅助函数 =================

def update_product_link_in_csv(product_name, platform, new_url):
    """
    更新 CSV 中的链接，严格匹配商品名和平台
    """
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.csv")
    if not os.path.exists(csv_path): return False
        
    temp_rows = []
    updated = False
    fieldnames = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames: return False
            for row in reader:
                p_name = row.get("Product Name") or row.get("型号")
                p_plat = row.get("Platform") or row.get("平台")
                
                if p_name and p_plat and p_name.strip() == product_name.strip() and p_plat.strip().lower() == platform.strip().lower():
                    if "Link" in row: row["Link"] = new_url
                    elif "链接" in row: row["链接"] = new_url
                    elif "url" in row: row["url"] = new_url
                    updated = True
                temp_rows.append(row)
        
        if updated:
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(temp_rows)
            return True
    except Exception as e:
        print(f"  [系统错误] 更新 CSV 失败: {e}")
    return False

# ================= 主程序 (Async) =================

async def run_filler_async(headless=False):
    print("启动自动填充器 (Async)...")
    
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.csv")
    if not os.path.exists(csv_path):
        print(f"错误: 找不到 {csv_path}")
        return

    rows = []
    fieldnames = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    to_fill_idx = []
    for i, row in enumerate(rows):
        link = row.get("Link") or row.get("链接") or row.get("url")
        name = row.get("Product Name") or row.get("型号")
        platform = row.get("Platform") or row.get("平台")
        
        if (not link or len(link) < 10) and name and platform:
            to_fill_idx.append(i)

    if not to_fill_idx:
        print("所有商品都已有链接，无需填充。")
        return
    
    print(f"发现 {len(to_fill_idx)} 个商品缺少链接，准备开始并发搜索...")

    async with async_playwright() as p:
        browser_args = ['--disable-blink-features=AutomationControlled']
        browser = await p.chromium.launch(headless=headless, args=browser_args)
        sem = asyncio.Semaphore(3)

        async def process_item(idx):
            async with sem:
                row = rows[idx]
                name = row.get("Product Name") or row.get("型号")
                country = row.get("Country") or row.get("国家", "")
                country_upper = country.strip().upper()
                
                locale_str = "en-GB"
                tz_str = "Europe/London"
                if "FR" in country_upper:
                    locale_str = "fr-FR"
                    tz_str = "Europe/Paris"
                elif "DE" in country_upper:
                    locale_str = "de-DE"
                    tz_str = "Europe/Berlin"
                elif "US" in country_upper:
                    locale_str = "en-US"
                    tz_str = "America/New_York"
                    
                context = await browser.new_context(user_agent=USER_AGENT_STR, locale=locale_str, timezone_id=tz_str, viewport={'width': 1920, 'height': 1080})
                
                # 动态替换 STEALTH_JS 里的硬编码语言
                lang_short = locale_str.split("-")[0]
                dynamic_stealth = STEALTH_JS.replace("'de-DE'", f"'{locale_str}'").replace("'de'", f"'{lang_short}'")
                await context.add_init_script(dynamic_stealth)
                
                page = await context.new_page()
                brand = row.get("Brand") or ""
                platform_val = row.get("Platform") or row.get("平台", "")
                platform_lower = platform_val.strip().lower()
                
                print(f"正在处理 [{platform_val}] {name} ...")
                new_link = None
                target_keyword = f"{brand} {name}".strip()
                
                try:
                    if "darty" in platform_lower:
                        new_link = await get_first_result_darty(page, target_keyword)
                    elif "boulanger" in platform_lower:
                        new_link = await get_first_result_boulanger(page, target_keyword)
                    elif "fnac" in platform_lower:
                        new_link = await get_first_result_fnac(page, target_keyword)
                    elif "amazon" in platform_lower:
                        new_link = await get_first_result_amazon(page, target_keyword)
                    elif "currys" in platform_lower:
                        new_link = await get_first_result_currys(page, target_keyword)
                    elif "mediamarkt" in platform_lower:
                        new_link = await get_first_result_mediamarkt(page, target_keyword)
                    elif "coolblue" in platform_lower:
                        new_link = await get_first_result_coolblue(page, target_keyword)
                except Exception as e:
                    print(f"  [任务出错] {name}: {e}")
                
                await context.close()
                return idx, new_link

        tasks = [process_item(i) for i in to_fill_idx]
        results = await asyncio.gather(*tasks)
        await browser.close()
        
        updated_count = 0
        for idx, new_link in results:
            if new_link:
                row = rows[idx]
                if "Link" in row: row["Link"] = new_link
                elif "链接" in row: row["链接"] = new_link
                elif "url" in row: row["url"] = new_link
                updated_count += 1
                print(f"  [成功] 填充验证通过的链接: {new_link}")

    if updated_count > 0:
        print("正在保存更新后的 CSV ...")
        try:
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print("CSV 文件已更新！")
        except Exception as e:
            print(f"[错误] 保存 CSV 失败: {e}")
    else:
        print("没有新的链接被填充。")

def run_filler(headless=False):
    asyncio.run(run_filler_async(headless))

if __name__ == "__main__":
    run_filler(headless=True)
