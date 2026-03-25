"""
Coolblue.de 价格提取测试脚本
用法: python test_coolblue.py
会打开非 headless 浏览器，访问一个 TV 产品页并测试价格提取。
"""
import asyncio
import json
import re
from playwright.async_api import async_playwright

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [{ name: 'Chrome PDF Plugin' }] });
Object.defineProperty(navigator, 'languages', { get: () => ['de-DE', 'de', 'en-GB', 'en'] });
if (window.chrome) { window.chrome.runtime = undefined; }
Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth });
Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight + 85 });
"""

def clean_price(text):
    if not text: return None
    text = text.strip()
    currency = "EUR"
    clean_text = text.replace("€", "").replace("EUR", "").replace("\xa0", "").strip()
    clean_text = clean_text.replace(" ", "")
    if "," in clean_text and "." in clean_text:
        clean_text = clean_text.replace(".", "").replace(",", ".")
    else:
        clean_text = clean_text.replace(",", ".")
    match = re.search(r"(\d+(\.\d+)?)", clean_text)
    if match:
        return float(match.group(1)), currency
    return None

async def test_coolblue_price():
    test_url = "https://www.coolblue.de/de/suche?query=samsung+qled+tv"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            locale="de-DE",
            timezone_id="Europe/Berlin",
            viewport={"width": 1920, "height": 1080}
        )
        await context.add_init_script(STEALTH_JS)
        page = await context.new_page()

        print(f"[测试] 导航到: {test_url}")
        await page.goto(test_url, wait_until='domcontentloaded', timeout=40000)
        await asyncio.sleep(4)

        title = await page.title()
        print(f"[测试] 页面标题: {title}")

        # 尝试接受 Cookie
        for btn in ["button:has-text('Akzeptieren')", "button:has-text('Alle akzeptieren')", "#onetrust-accept-btn-handler", "[data-test='accept-cookies']"]:
            try:
                if await page.is_visible(btn, timeout=2000):
                    await page.click(btn)
                    await asyncio.sleep(1)
                    print(f"[测试] 已点击 Cookie 按钮: {btn}")
                    break
            except: pass

        # 保存搜索页 HTML
        content = await page.content()
        with open("debug_coolblue_search.html", "w", encoding="utf-8") as f:
            f.write(content[:20000])
        print("[测试] 已保存页面 HTML 到 debug_coolblue_search.html")

        # 查找产品链接
        links = await page.locator("a[href*='/product/'], a[href*='/produkt/']").all()
        print(f"[测试] 找到 {len(links)} 个产品链接")
        for i, link in enumerate(links[:3]):
            href = await link.get_attribute("href")
            text = (await link.inner_text())[:60]
            print(f"  [{i+1}] {text!r} -> {href}")

        # 测试价格提取（从第一个产品页）
        if links:
            first_href = await links[0].get_attribute("href")
            if first_href:
                if not first_href.startswith("http"):
                    first_href = "https://www.coolblue.de" + first_href
                print(f"\n[测试] 导航到产品页: {first_href}")
                await page.goto(first_href, wait_until='domcontentloaded', timeout=40000)
                await asyncio.sleep(5)

                prod_title = await page.title()
                print(f"[测试] 产品页标题: {prod_title}")

                # Schema 提取
                try:
                    amount = await page.get_attribute("meta[property='product:price:amount']", "content", timeout=500)
                    if amount:
                        print(f"[测试] Meta price: {amount} EUR")
                except: pass

                try:
                    scripts = await page.locator("script[type='application/ld+json']").all()
                    for script in scripts:
                        text = await script.text_content()
                        if text and '"price"' in text:
                            data = json.loads(text)
                            print(f"[测试] JSON-LD 包含 price: {str(data)[:200]}")
                            break
                except: pass

                # CSS 选择器测试
                selectors = [
                    "[class*='sales-price__current']",
                    "[class*='SalesPrice']",
                    "strong[class*='price']",
                    "[data-test='sales-price']",
                    ".price",
                    "span[class*='price']",
                ]
                for sel in selectors:
                    try:
                        els = await page.locator(sel).all()
                        visible = [e for e in els if await e.is_visible()]
                        if visible:
                            text = await visible[0].inner_text()
                            result = clean_price(text)
                            print(f"[测试] 选择器 {sel!r} -> {text!r} -> {result}")
                            if result:
                                break
                    except: pass

                # 保存产品页 HTML
                prod_content = await page.content()
                with open("debug_coolblue_product.html", "w", encoding="utf-8") as f:
                    f.write(prod_content[:30000])
                print("[测试] 已保存产品页 HTML 到 debug_coolblue_product.html")

        await browser.close()
        print("\n[测试] 完成！")

if __name__ == "__main__":
    asyncio.run(test_coolblue_price())
