import csv
import os
import time
import random
import urllib.parse
from playwright.sync_api import sync_playwright

# 基础配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "products.csv")
USER_AGENT_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

def get_first_result_darty(page, keyword):
    """在 Darty 搜索并提取第一个结果"""
    print(f"  正在 Darty 搜索: {keyword} ...")
    search_url = f"https://www.darty.com/nav/recherche?text={urllib.parse.quote(keyword)}"
    
    try:
        page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        # 等待搜索结果加载
        # Darty 的搜索结果列表通常在 .product_list 或 .da_product_list 中
        # 我们尝试找第一个商品链接
        # 选择器策略: 找包含 href 的商品标题或图片链接
        
        # 尝试常见的商品卡片链接选择器
        selectors = [
            ".product_detail_link", 
            "a[data-automation-id='product_details_link']",
            ".product-card__link",
            "div.product_list a" # 宽泛匹配
        ]
        
        for sel in selectors:
            if page.is_visible(sel):
                link = page.get_attribute(sel, "href")
                if link:
                    if not link.startswith("http"):
                        link = "https://www.darty.com" + link
                    print(f"  -> 找到链接: {link}")
                    return link
    except Exception as e:
        print(f"  [Darty搜索失败] {e}")
        
    return None

def get_first_result_boulanger(page, keyword):
    """在 Boulanger 搜索并提取第一个结果"""
    print(f"  正在 Boulanger 搜索: {keyword} ...")
    search_url = f"https://www.boulanger.com/resultats?tr={urllib.parse.quote(keyword)}"
    
    try:
        page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        
        # === 处理 Cookie 弹窗 ===
        # 常见 ID: onetrust-accept-btn-handler, 或 文本包含 Accepter
        try:
            # 尝试点击 "Accepter et Fermer" 或 "Tout accepter"
            if page.is_visible("#onetrust-accept-btn-handler", timeout=3000):
                page.click("#onetrust-accept-btn-handler")
                print("  [操作] 已点击关闭 Boulanger Cookie 弹窗")
                time.sleep(1) # 等待弹窗消失
            elif page.is_visible("button:has-text('Accepter')", timeout=1000):
                page.click("button:has-text('Accepter')")
                print("  [操作] 点击了 'Accepter' 按钮")
                time.sleep(1)
            elif page.is_visible("a:has-text('Continuer sans accepter')", timeout=1000):
                page.click("a:has-text('Continuer sans accepter')")
                print("  [操作] 点击了 'Continuer sans accepter'")
                time.sleep(1)
        except:
            pass # 没弹窗就算了
            
        # Boulanger 商品列表项
        # 策略 2: 暴力搜索包含 '/ref/' 的链接 (Boulanger 商品页特征)
        # 这比依赖 CSS class 更稳健
        try:
            # 等待一会确保内容加载
            time.sleep(2) 
            
            # 获取所有链接
            links = page.locator("a[href*='/ref/']").all()
            
            for link_locator in links:
                href = link_locator.get_attribute("href")
                if href:
                    if not href.startswith("http"):
                        href = "https://www.boulanger.com" + href
                        
                    # 过滤掉一些非商品链接 (如果有的话)
                    # 通常 /ref/\d+ 就是商品
                    print(f"  -> 找到潜在链接: {href}")
                    return href
        except Exception as e:
            print(f"  [Error] 查找 /ref/ 链接失败: {e}")
                
    # 如果没找到，保存截图调试
        print(f"  [警告] 未能在页面上找到商品链接。页面标题: {page.title()}")
        screenshot_path = f"debug_filler_boulanger_{int(time.time())}.png"
        page.screenshot(path=screenshot_path)
        print(f"  [调试] 已保存截图: {screenshot_path}")

    except Exception as e:
        print(f"  [Boulanger搜索失败] {e}")
            
    return None

def get_first_result_fnac(page, keyword):
    """在 Fnac 搜索并提取第一个结果"""
    print(f"  正在 Fnac 搜索: {keyword} ...")
    # Fnac 搜索 URL
    search_url = f"https://www.fnac.com/SearchResult/ResultList.aspx?Search={urllib.parse.quote(keyword)}"
    
    try:
        page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        time.sleep(2)

        # === 处理 Cookie 弹窗 ===
        try:
            # Fnac 也常用 OneTrust
            if page.is_visible("#onetrust-accept-btn-handler", timeout=3000):
                page.click("#onetrust-accept-btn-handler")
                print("  [操作] 已点击关闭 Fnac Cookie 弹窗")
                time.sleep(1)
        except: pass
            
        # 策略: 寻找搜索结果列表中的第一个商品链接
        # Fnac 的列表项通常是 article.Article-itemJS 或 .Article-item
        # 链接通常在 .Article-title a 或 .Article-item a
        
        potential_links = page.locator("article a").all()
        # 也可以尝试找包含 /a (livre/produit) 的链接
        
        for link in potential_links:
            href = link.get_attribute("href")
            # 过滤无效链接
            if href and "fnac.com" in href and ("/a" in href or "/mp" in href) and not "avis" in href:
                print(f"  -> 找到链接: {href}")
                return href
            elif href and not href.startswith("http"): # 相对路径
                full_link = "https://www.fnac.com" + href
                if ("/a" in full_link or "/mp" in full_link) and not "avis" in full_link:
                    print(f"  -> 找到链接: {full_link}")
                    return full_link
        
        # 备用策略
        fallback_link = page.locator(".Article-title a").first.get_attribute("href")
        if fallback_link:
             if not fallback_link.startswith("http"):
                 fallback_link = "https://www.fnac.com" + fallback_link
             return fallback_link

    except Exception as e:
        print(f"  [Fnac搜索失败] {e}")
            
    return None

def run_filler(headless=False):
    print("启动自动填充器 (Filler)...")
    
    if not os.path.exists(CSV_FILE):
        print(f"错误: 找不到 {CSV_FILE}")
        return

    # 1. 读取所有行
    rows = []
    fieldnames = []
    needs_update = False
    
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    # 2. 检查是否有需要填充的
    to_fill = []
    for i, row in enumerate(rows):
        link = row.get("Link") or row.get("链接") or row.get("url")
        name = row.get("Product Name") or row.get("型号")
        platform = row.get("Platform") or row.get("平台")
        
        # 如果链接为空或太短，且有名字和平台
        if (not link or len(link) < 10) and name and platform:
            to_fill.append(i)

    if not to_fill:
        print("所有商品都已有链接，无需填充。")
        return

    print(f"发现 {len(to_fill)} 个商品缺少链接，准备开始搜索...")

    # 3. 启动浏览器
    with sync_playwright() as p:
        browser_args = ['--disable-blink-features=AutomationControlled', '--start-maximized']
        try:
            browser = p.chromium.launch(headless=headless, channel="chrome", slow_mo=50, args=browser_args)
        except:
            browser = p.chromium.launch(headless=headless, slow_mo=50, args=browser_args)
            
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            locale="fr-FR"
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = context.new_page()
        
        # 预热
        try:
            page.goto("https://www.boulanger.com", timeout=10000)
        except: pass

        # 4. 遍历处理
        for idx in to_fill:
            row = rows[idx]
            name = row.get("Product Name") or row.get("型号")
            platform_val = row.get("Platform") or row.get("平台", "")
            platform = platform_val.lower()
            
            print(f"正在处理 [{platform_val}] {name} ...")
            
            found_link = None
            if "darty" in platform:
                found_link = get_first_result_darty(page, name)
            elif "boulanger" in platform:
                found_link = get_first_result_boulanger(page, name)
            elif "fnac" in platform:
                found_link = get_first_result_fnac(page, name)
            else:
                print(f"  [跳过] 未知平台: {platform_val}")
                
            if found_link:
                # 更新内存中的数据
                if "Link" in row: row["Link"] = found_link
                elif "链接" in row: row["链接"] = found_link
                elif "url" in row: row["url"] = found_link
                needs_update = True
                print(f"  [成功] 填充链接: {found_link}")
            else:
                print(f"  [失败] 未搜到链接")
                
            time.sleep(random.uniform(2, 4))

        browser.close()

    # 5. 写回文件
    if needs_update:
        print("正在保存更新后的 CSV ...")
        with open(CSV_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print("CSV 文件已更新！")
    else:
        print("没有新的链接被填充。")

if __name__ == "__main__":
    run_filler(headless=True)

