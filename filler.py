import csv
import os
import time
import random
import urllib.parse
import asyncio
from playwright.async_api import async_playwright

# 基础配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "products.csv")
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

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
            if await page.is_visible(sel):
                link = await page.get_attribute(sel, "href")
                if link:
                    if not link.startswith("http"):
                        link = "https://www.darty.com" + link
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
            elif await page.is_visible("button:has-text('Accepter')", timeout=1000):
                await page.click("button:has-text('Accepter')")
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
            await asyncio.sleep(4)  # 多等一秒确保结果加载
        else:
            print("  [提示] 未找到搜索框，回退到 URL 拼接模式")
            search_url = f"https://www.boulanger.com/resultats?tr={urllib.parse.quote(keyword)}"
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)

        # 调试日志: 当前 URL 和标题
        current_url = page.url
        current_title = await page.title()
        print(f"  [调试] 搜索后URL: {current_url}")
        print(f"  [调试] 页面标题: {current_title}")

        # 检查 1: 是否直接跳转到了商品页
        if "/ref/" in current_url:
            print(f"  -> 直接跳转到了商品页: {current_url}")
            return current_url

        # 检查 2: 主选择器 - /ref/ 格式链接
        links = await page.locator("a[href*='/ref/']").all()
        print(f"  [调试] 找到 {len(links)} 个 /ref/ 链接")
        for link_locator in links:
            href = await link_locator.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = "https://www.boulanger.com" + href
                print(f"  -> 找到潜在链接: {href}")
                return href

        # 检查 3: 备用选择器 - 搜索结果卡片中的商品链接
        fallback_selectors = [
            ".product-list a[href*='boulanger.com']",
            ".product-card a",
            ".productList a[href]",
            "a.product-thumb",
            "article a[href]",
        ]
        for sel in fallback_selectors:
            try:
                fallback_links = await page.locator(sel).all()
                for fl in fallback_links:
                    href = await fl.get_attribute("href")
                    if href and ("boulanger.com" in href or href.startswith("/")):
                        if not href.startswith("http"):
                            href = "https://www.boulanger.com" + href
                        # 排除首页/分类页等非商品链接
                        if "/resultats" not in href and "/c/" not in href and len(href) > 35:
                            print(f"  -> 备用选择器找到链接: {href}")
                            return href
            except: pass
        
        print(f"  [Boulanger] 未在搜索结果中找到任何商品链接")

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

        # 暴力查找 /dp/ 链接
        try:
            links = await page.locator("div.s-main-slot a[href*='/dp/']").all()
            for link in links:
                href = await link.get_attribute("href")
                if href and "slredirect" not in href and "#" not in href and "/dp/" in href:
                    if not href.startswith("http"):
                        href = "https://www.amazon.co.uk" + href
                    print(f"  -> 找到潜在链接: {href}")
                    return href
        except Exception as e:
            print(f"  [Amazon提取失败] {e}")       
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
            # 简化逻辑
            if href and "fnac.com" in href and ("/a" in href or "/mp" in href) and not "avis" in href:
                print(f"  -> 找到链接: {href}")
                return href
            elif href and not href.startswith("http"): 
                full_link = "https://www.fnac.com" + href
                if ("/a" in full_link or "/mp" in full_link) and not "avis" in full_link:
                    print(f"  -> 找到链接: {full_link}")
                    return full_link
        # Fallback
        first_el = page.locator(".Article-title a").first
        if await first_el.count() > 0:
             fallback_link = await first_el.get_attribute("href")
             if fallback_link:
                 if not fallback_link.startswith("http"):
                     fallback_link = "https://www.fnac.com" + fallback_link
                 return fallback_link
    except Exception as e:
        print(f"  [Fnac搜索失败] {e}")
    return None

# ================= 辅助函数 =================

def update_product_link_in_csv(product_name, new_url):
    """
    更新 CSV 中的链接 (同步操作，因为文件IO不需要async)
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

    # 1. 读取所有行
    rows = []
    fieldnames = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    # 2. 检查是否有需要填充的
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

    # 3. 启动 Async Playwright
    async with async_playwright() as p:
        browser_args = ['--disable-blink-features=AutomationControlled', '--start-maximized']
        browser = await p.chromium.launch(headless=headless, args=browser_args)
        
        # 限制并发数
        sem = asyncio.Semaphore(3)

        async def process_item(idx):
            async with sem:
                context = await browser.new_context(
                    user_agent=USER_AGENT_STR,
                    locale="en-US"
                )
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
                    else:
                        print(f"  [跳过] 未知平台: {platform_val}")
                except Exception as e:
                    print(f"  [任务出错] {name}: {e}")
                
                await context.close()
                return idx, new_link

        # 创建任务
        tasks = [process_item(i) for i in to_fill_idx]
        results = await asyncio.gather(*tasks)

        await browser.close()
        
        # 4. 更新结果
        updated_count = 0
        for idx, new_link in results:
            if new_link:
                row = rows[idx]
                if "Link" in row: row["Link"] = new_link
                elif "链接" in row: row["链接"] = new_link
                elif "url" in row: row["url"] = new_link
                updated_count += 1
                print(f"  [成功] 填充链接: {new_link}")

    # 5. 写回文件
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
    """入口函数"""
    asyncio.run(run_filler_async(headless))

if __name__ == "__main__":
    # Headless=False 方便调试
    run_filler(headless=False)
