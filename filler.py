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

# ================= 辅助验证函数 =================

def validate_link(link, keyword, page_title=""):
    """
    验证搜索到的链接是否与关键词匹配。
    逻辑：检查关键词中的重要部分（如品牌和型号）是否出现在链接或页面标题中。
    """
    if not link: return False
    
    # 将关键词拆分为部分，过滤短词
    parts = [p.strip().lower() for p in keyword.split() if len(p.strip()) > 2]
    if not parts: return True # 如果没法拆分出有效词，保守处理

    link_lower = link.lower()
    title_lower = page_title.lower() if page_title else ""
    
    # 核心验证逻辑：至少要包含品牌或型号中的一个“硬性”识别词
    # 比如 "Samsung 65Q7F", 如果链接或标题里完全没出现 "Samsung" 也没出现 "65Q7F", 就认为不匹配
    matches = 0
    for p in parts:
        if p in link_lower or p in title_lower:
            matches += 1
            
    # 如果匹配到的关键词部分占比太低，认为有误搜风险
    # 特别是针对长关键词，如果一个识别度高的词都没对上，直接排除
    if matches == 0:
        return False
    
    # 针对三星这类品牌，如果搜出来了 DJI 这种完全不相干的品牌关键字，直接拍死
    anti_keywords = ["dji", "drone", "mavic", "fly-more"]
    if any(ak in keyword.lower() for ak in ["samsung", "tcl", "hisense", "tv"]):
        if any(ak in link_lower or ak in title_lower for ak in anti_keywords):
            return False

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
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(4)
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

        # 提取结果列表
        links = await page.locator("a[href*='/ref/']").all()
        for link_locator in links:
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
        await asyncio.sleep(random.uniform(2, 4))
        
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
            await asyncio.sleep(4)
        
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

# ================= 辅助函数 =================

def update_product_link_in_csv(product_name, new_url):
    """
    更新 CSV 中的链接
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
                if p_name and p_name.strip() == product_name.strip():
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
                context = await browser.new_context(user_agent=USER_AGENT_STR, locale="en-US")
                await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                page = await context.new_page()
                
                row = rows[idx]
                name = row.get("Product Name") or row.get("型号")
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
