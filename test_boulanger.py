import asyncio
from playwright.async_api import async_playwright
import re

async def test_boulanger():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        url = "https://www.boulanger.com/resultats?tr=SAMSUNG%20TV&numPage=2"
        print(f"Navigating to {url}")
        await page.goto(url, wait_until='domcontentloaded')
        await asyncio.sleep(4)

        
        # Try to find elements that have total counts or exact pagination info
        try:
             # Find standard pager elements
             text_content = await page.content()
             with open("boulanger_test_page.html", "w", encoding="utf-8") as f:
                 f.write(text_content)
             
             # Try to evaluate some JS variables often used like __INITIAL_STATE__ or dataLayer
             state = await page.evaluate("typeof window.__INITIAL_STATE__ !== 'undefined'")
             print(f"Has __INITIAL_STATE__: {state}")
             
             # Also let's check class names for pagination
             pager = await page.locator("ul.pagination, .pagination, [data-test='pagination']").count()
             print(f"Found {pager} pagination elements")
             
             # Check how many ref links are visible initially
             links = await page.locator("a[href*='/ref/']").count()
             print(f"Found {links} ref links without scrolling")
             
        except Exception as e:
             print(f"Error: {e}")
             
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_boulanger())
