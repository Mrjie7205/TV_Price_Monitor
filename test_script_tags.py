import json
from bs4 import BeautifulSoup

html = open('boulanger_test_page.html', encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

scripts = soup.find_all('script')
for idx, script in enumerate(scripts):
    text = script.string
    if text and 'window.__INITIAL_STATE__' in text:
        print("Found __INITIAL_STATE__")
    if text and 'window.__NUXT__' in text:
        print("Found __NUXT__")
    if text and 'total' in text.lower():
        if len(text) > 1000:
            print(f"Large script {idx} contains 'total'. Length: {len(text)}")
        else:
             print(f"Script {idx} contains total: {text[:100]}")

# Look for specific IDs
state_script = soup.find(id='__BLG_STATE__')
if state_script:
    print("Found __BLG_STATE__")

