from bs4 import BeautifulSoup
import json
import re

html = open('boulanger_test_page.html', encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

products = []
for a in soup.find_all('a'):
    href = a.get('href', '')
    if '/ref/' in href:
        title = a.get('title') or a.get_text(strip=True)
        products.append({"href": href, "title": title})

# Deduplicate
unique = {}
for p in products:
    if p['href'] not in unique:
        unique[p['href']] = p

print(f"Total extracted: {len(products)}")
print(f"Total unique: {len(unique)}")
import sys
if len(unique) > 0:
    for i, (_, p) in enumerate(list(unique.items())[:5]):
        print(f"{i}: {p['href']} - {p['title']}")
