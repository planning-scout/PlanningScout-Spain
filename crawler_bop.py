import os
import requests
from bs4 import BeautifulSoup
import json
import time

# 1. Config
BOP_URL = "https://bop.diba.cat/boletin-del-dia"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

def get_daily_urbanismo_links():
    print(f"🌐 Fetching daily bulletin index from: {BOP_URL}")
    response = requests.get(BOP_URL, headers=HEADERS, timeout=20)
    
    if response.status_code != 200:
        print(f"❌ Failed to reach BOP. Status: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    all_links = []

    # Look for the "Urbanismo" section specifically
    # Based on the BOP structure, we search for text "Urbanismo" or "Urbanisme"
    for section in soup.find_all(['h3', 'h4', 'span']):
        if "Urbanismo" in section.get_text() or "Urbanisme" in section.get_text():
            # Find links in the parent container of this section
            container = section.find_parent()
            if container:
                links = container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if "/anuncio/descargar-pdf/" in href:
                        # Construct full URL if needed
                        full_url = href if href.startswith('http') else f"https://bop.diba.cat{href}"
                        all_links.append(full_url)

    # Remove duplicates
    unique_links = list(set(all_links))
    print(f"🎯 Found {len(unique_links)} potential Urbanismo PDFs for today.")
    return unique_links

if __name__ == "__main__":
    links = get_daily_urbanismo_links()
    # Save links to a temporary file for the next step
    with open("found_links.json", "w") as f:
        json.dump(links, f)
