import os
import sqlite3
import logging
import json
import random
import time
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# --- Конфігурація ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_NAME = "amazon_data.db"
templates = Jinja2Templates(directory="templates")
ua = UserAgent()

# --- База даних ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS products (
        asin TEXT PRIMARY KEY,
        title TEXT,
        rank INTEGER,
        price TEXT,
        rating TEXT,
        reviews_count TEXT,
        is_prime BOOLEAN,
        bullet_points TEXT,
        image_url TEXT,
        category_url TEXT,
        updated_at TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS categories (
        url TEXT PRIMARY KEY,
        name TEXT,
        updated_at TIMESTAMP
    )''')
    conn.commit()
    conn.close()

# --- Допоміжні функції ---
def get_headers():
    """Генерація заголовків для імітації браузера."""
    return {
        'User-Agent': ua.random,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.amazon.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

def clean_text(text: Optional[str]) -> str:
    """Очищення тексту від зайвих пробілів."""
    if not text:
        return ""
    return text.strip()

# --- Логіка парсингу з Retry (повторними спробами) ---

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(requests.RequestException))
def fetch_url(url: str):
    """Виконує запит з автоматичними повторами при помилках."""
    time.sleep(random.uniform(1.5, 3.5)) # Затримка, щоб не бути заблокованим
    response = requests.get(url, headers=get_headers(), timeout=15)
    
    if response.status_code == 503:
        raise requests.RequestException("Amazon 503 Service Unavailable")
        
    # Перевірка на капчу Amazon
    if "api-services-support@amazon.com" in response.text:
        raise requests.RequestException("Amazon Captcha Block")
        
    response.raise_for_status()
    return response

def get_product_details(product_url: str):
    """
    Глибокий парсинг: заходимо всередину товару за булетами та Prime.
    """
    try:
        if not product_url.startswith("http"):
            product_url = "https://www.amazon.com" + product_url
            
        response = fetch_url(product_url)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # 1. Bullet Points
        bullets = []
        # Пробуємо різні варіанти верстки
        bullet_div = soup.select_one('#feature-bullets') or soup.select_one('#av-feature-bullets')
        if bullet_div:
            items = bullet_div.select('li span.a-list-item')
            bullets = [clean_text(item.get_text()) for item in items[:5]]
        
        # 2. Prime Status
        is_prime = False
        if soup.select_one('#prime-header-link') or soup.select_one('i.a-icon-prime'):
            is_prime = True
            
        return {"bullets": "\n".join(bullets), "is_prime": is_prime}
    except Exception as e:
        logger.warning(f"Could not fetch details for {product_url}: {e}")
        return {"bullets": "", "is_prime": False}

def scrape_amazon_category(url: str, limit: int = 5):
    logger.info(f"Scraping category: {url}")
    try:
        response = fetch_url(url)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Надійні селектори для сітки товарів
        items = soup.select('div[id^="p13n-asin-index"]')
        if not items:
             items = soup.select('.zg-grid-general-faceout')

        parsed_items = []
        for item in items[:limit]:
            try:
                # 1. ASIN
                asin = "N/A"
                item_json = item.get('data-p13n-asin-metadata')
                if item_json:
                    asin = json.loads(item_json)['asin']
                
                # 2. Title (Спроба кількох селекторів)
                title_el = (item.select_one('div[class*="p13n-sc-css-line-clamp"]') or 
                            item.select_one('div.p13n-sc-truncated') or
                            item.select_one('a.a-link-normal span div'))
                title = clean_text(title_el.get_text()) if title_el else "Unknown Title"

                # 3. Rank
                rank_el = item.select_one('.zg-bdg-text')
                rank = int(rank_el.get_text().replace('#', '')) if rank_el else 0
                
                # 4. Price
                price_el = (item.select_one('span.a-color-price') or 
                            item.select_one('span.p13n-sc-price') or
                            item.select_one('span._cDEzb_p13n-sc-price_3mJ9Z'))
                price = clean_text(price_el.get_text()) if price_el else "N/A"

                # 5. Rating & Reviews
                rating_el = item.select_one('i.a-icon-star-small span')
                rating = clean_text(rating_el.get_text()) if rating_el else "N/A"
                
                reviews_el = item.select_one('span.a-size-small')
                reviews = clean_text(reviews_el.get_text()) if reviews_el else "0"
                
                # 6. Image
                img_el = item.select_one('img.a-dynamic-image')
                image_url = img_el.get('src') if img_el else ""
                
                # 7. Deep Scrape (Заходимо на сторінку)
                link_el = item.select_one('a.a-link-normal')
                detail_data = {"bullets": "", "is_prime": False}
                
                if link_el and link_el.get('href'):
                    detail_data = get_product_details(link_el.get('href'))
                
                product_data = {
                    "asin": asin,
                    "title": title,
                    "rank": rank,
                    "price": price,
                    "rating": rating,
                    "reviews_count": reviews,
                    "is_prime": detail_data['is_prime'],
                    "bullet_points": detail_data['bullets'],
                    "image_url": image_url,
                    "category_url": url,
                    "updated_at": datetime.now()
                }
                parsed_items.append(product_data)
                
            except Exception as e:
                logger.error(f"Error parsing specific item: {e}")
                continue
                
        return parsed_items

    except Exception as e:
        logger.error(f"Scraping fatal error: {e}")
        return []

def scrape_root_categories():
    """Оновлення категорій з fallback-селекторами."""
    url = "https://www.amazon.com/gp/bestsellers"
    logger.info("Updating root categories...")
    try:
        response = fetch_url(url)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Спроба знайти бічне меню
        cat_links = soup.select('div[role="group"] div[role="treeitem"] a')
        if not cat_links:
            cat_links = soup.select('ul#zg_browseRoot a')
            
        categories = []
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        for link in cat_links:
            href = link.get('href')
            name = clean_text(link.get_text())
            if href and name:
                full_url = f"https://www.amazon.com{href}"
                c.execute("INSERT OR REPLACE INTO categories (url, name, updated_at) VALUES (?, ?, ?)",
                          (full_url, name, datetime.now()))
                categories.append({"name": name, "url": full_url})
        
        conn.commit()
        conn.close()
        return categories
    except Exception as e:
        logger.error(f"Failed to update categories: {e}")
        return []

# --- FastAPI App ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(scrape_root_categories, 'interval', hours=24)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/categories")
def get_categories():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM categories")
    rows = c.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.post("/api/scrape")
def trigger_scrape(url: str = Query(...)):
    items = scrape_amazon_category(url)
    if not items:
        return {"status": "error", "message": "Amazon blocked requests or changed layout.", "data": []}
        
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for i in items:
        c.execute('''INSERT OR REPLACE INTO products 
        (asin, title, rank, price, rating, reviews_count, is_prime, bullet_points, image_url, category_url, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
        (i['asin'], i['title'], i['rank'], i['price'], i['rating'], i['reviews_count'], i['is_prime'], i['bullet_points'], i['image_url'], i['category_url'], i['updated_at']))
    conn.commit()
    conn.close()
    
    return {"status": "success", "count": len(items), "data": items}