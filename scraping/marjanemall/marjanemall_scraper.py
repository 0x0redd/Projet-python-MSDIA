"""
Marjanemall.ma scraper using Playwright
Uses browser automation to extract fully rendered product data
Based on the working test.py implementation with enhancements for all categories
"""

from playwright.sync_api import sync_playwright, Page
from urllib.parse import urljoin
import time
import logging
import re
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class MarjanemallScraper:
    """Scraper for Marjanemall.ma using Playwright"""
    
    # All available categories on Marjanemall.ma
    CATEGORIES = [
        'telephone-objets-connectes',
        'informatique-gaming',
        'electromenager',
        'tv-image-son',
        'maison-cuisine-deco',
        'beaute-sante',
        'vetements-chaussures-bijoux-accessoires',
        'sport',
        'bebe-jouets',
        'auto-moto',
        'brico-jardin-animalerie',
        'librairie',
        'epicerie-fine'
    ]
    
    def __init__(self, base_url: str = "https://www.marjanemall.ma", headless: bool = True, scroll_delay: float = 2.0):
        """
        Initialize the Marjanemall scraper
        
        Args:
            base_url: Base URL of Marjanemall.ma
            headless: Run browser in headless mode
            scroll_delay: Delay between scrolls in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.headless = headless
        self.scroll_delay = scroll_delay
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
    
    def __enter__(self):
        """Context manager entry"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = self.context.new_page()
        logger.info("Browser initialized successfully")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        self.page = None
        self.context = None
        self.browser = None
        self.playwright = None
        logger.info("Browser closed")
    
    def scrape_page(self, category: str, page_num: int) -> List[Dict]:
        """
        Scrape a single page using Playwright - EXACT implementation from test.py
        
        Args:
            category: Category slug (e.g., 'telephone-objets-connectes')
            page_num: Page number
            
        Returns:
            List of product dictionaries
        """
        url = f"{self.base_url}/{category}?page={page_num}"
        
        logger.info(f"📦 Scraping page {page_num} of category '{category}'")
        
        try:
            # Navigate to page
            self.page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            time.sleep(2)
            
            # Scroll to trigger lazy loading
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            # Scroll back up and down again
            self.page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

            # Extract products using JavaScript - EXACT same code as test.py
            products = self.page.evaluate("""
                () => {
                    const products = [];
                    const cards = document.querySelectorAll('a[href^="/p/"]');
                    
                    cards.forEach(card => {
                        const nameElem = card.querySelector('h3');
                        const priceElem = card.querySelector('span.text-lg');
                        const oldPriceElem = card.querySelector('span.line-through');
                        const sellerElem = card.querySelector('span.text-primary');
                        const imageElem = card.querySelector('img');
                        const link = card.getAttribute('href');
                        
                        if (nameElem || link) {
                            products.push({
                                name: nameElem ? nameElem.textContent.trim() : null,
                                price: priceElem ? priceElem.textContent.trim() : null,
                                old_price: oldPriceElem ? oldPriceElem.textContent.trim() : null,
                                seller: sellerElem ? sellerElem.textContent.replace('Vendu par', '').trim() : null,
                                image: imageElem ? (imageElem.getAttribute('src') || imageElem.getAttribute('data-src')) : null,
                                url: link
                            });
                        }
                    });
                    
                    return products;
                }
            """)
            
            logger.info(f"   🔎 Found {len(products)} product cards")
            
            # Normalize products for database compatibility
            formatted_products = []
            for product in products:
                if product['url']:
                    product['url'] = urljoin(self.base_url, product['url'])
                
                # Extract product ID from URL
                url_match = product.get('url', '').split('/')[-1]
                product['product_id'] = url_match.upper() if url_match else None
                
                # Normalize price fields (same structure as JumiaScraper)
                price_text = product.get('price', '')
                product['price_text'] = price_text if price_text else None
                
                # Extract numeric price
                if price_text:
                    price_clean = re.sub(r'[^\d.]', '', price_text.replace(' ', ''))
                    try:
                        product['price'] = float(price_clean) if price_clean else None
                    except ValueError:
                        product['price'] = None
                else:
                    product['price'] = None
                
                # Normalize old_price
                old_price_text = product.get('old_price', '')
                product['old_price_text'] = old_price_text if old_price_text else None
                if old_price_text:
                    old_price_clean = re.sub(r'[^\d.]', '', old_price_text.replace(' ', ''))
                    try:
                        product['old_price'] = float(old_price_clean) if old_price_clean else None
                    except ValueError:
                        product['old_price'] = None
                else:
                    product['old_price'] = None
                
                # Calculate discount if both prices exist
                if product.get('price') and product.get('old_price'):
                    try:
                        discount_percent = ((product['old_price'] - product['price']) / product['old_price']) * 100
                        product['discount'] = int(discount_percent)
                        product['discount_text'] = f"-{int(discount_percent)}%"
                    except (ValueError, TypeError, ZeroDivisionError):
                        product['discount'] = None
                        product['discount_text'] = None
                else:
                    product['discount'] = None
                    product['discount_text'] = None
                
                # Normalize image field
                product['image_url'] = product.get('image')
                product['image_alt'] = product.get('name')  # Use name as alt text
                
                # Normalize seller field
                seller = product.get('seller')
                product['seller'] = seller if seller else None
                
                # Add rating fields (None for Marjanemall as they're not scraped)
                product['rating'] = None
                product['review_count'] = None
                
                # Add metadata (use scraped_at for database compatibility)
                product['category'] = category
                product['page_number'] = page_num
                product['scraped_at'] = datetime.now().isoformat()  # Database expects 'scraped_at'
                product['scraped_timestamp'] = product['scraped_at']  # Keep both for compatibility
                product['source'] = 'marjanemall.ma'
                
                # Add fields expected by database
                product['raw_price'] = product.get('price')  # Same as price for Marjanemall
                product['price_euro'] = None
                product['old_price_euro'] = None
                product['discount_euro'] = None
                product['express_delivery'] = False  # Can be enhanced later
                product['is_official_store'] = False
                product['is_sponsored'] = False
                product['is_buyable'] = True
                
                formatted_products.append(product)
            
            return formatted_products
        
        except Exception as e:
            logger.error(f"❌ Error scraping page {page_num} of {category}: {e}")
            return []
    
    def scrape_category(self, category: str, max_pages: int = None) -> List[Dict]:
        """
        Scrape all pages of a category until no more products found
        
        Args:
            category: Category slug
            max_pages: Maximum number of pages to scrape (None = unlimited)
            
        Returns:
            List of all product dictionaries from the category
        """
        if not self.page:
            raise RuntimeError("Scraper not initialized. Use 'with' statement")
        
        all_products = []
        page_num = 1
        consecutive_empty = 0
        max_consecutive_empty = 3  # Stop after 3 consecutive empty pages
        
        logger.info(f"🚀 Starting category: {category}")
        
        try:
            while True:
                # Check max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(f"✅ Reached max_pages limit ({max_pages})")
                    break
                
                # Scrape page
                products = self.scrape_page(category, page_num)
                
                # Check if page is empty
                if not products:
                    consecutive_empty += 1
                    logger.info(f"   ⚠️  Empty page ({consecutive_empty}/{max_consecutive_empty})")
                    
                    if consecutive_empty >= max_consecutive_empty:
                        logger.info(f"✅ No more pages (stopped after {consecutive_empty} empty pages)")
                        break
                    
                    page_num += 1
                    time.sleep(1)
                    continue
                
                # Reset empty counter and add products
                consecutive_empty = 0
                all_products.extend(products)
                logger.info(f"   ✓ Added {len(products)} products (total: {len(all_products)})")
                
                page_num += 1
                time.sleep(1)  # Delay between pages
        
        except KeyboardInterrupt:
            logger.warning(f"⚠️  Interrupted by user at page {page_num}")
        except Exception as e:
            logger.error(f"❌ Error scraping category '{category}': {e}", exc_info=True)
        
        logger.info(f"📊 Category '{category}' complete: {len(all_products)} products from {page_num-1} pages")
        return all_products
    
    def scrape_all_categories(self, max_pages_per_category: int = None, categories: List[str] = None) -> Dict[str, List[Dict]]:
        """
        Scrape all categories
        
        Args:
            max_pages_per_category: Maximum pages per category (None = unlimited)
            categories: List of specific categories to scrape (None = all)
            
        Returns:
            Dictionary mapping category names to product lists
        """
        results = {}
        categories_to_scrape = categories if categories else self.CATEGORIES
        
        logger.info(f"\n{'='*70}")
        logger.info(f"🚀 STARTING SCRAPING - {len(categories_to_scrape)} CATEGORIES")
        logger.info(f"{'='*70}")
        
        for idx, category in enumerate(categories_to_scrape, 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"📂 [{idx}/{len(categories_to_scrape)}] Category: {category}")
            logger.info(f"{'='*70}")
            
            try:
                products = self.scrape_category(category, max_pages=max_pages_per_category)
                results[category] = products
                logger.info(f"✅ Completed '{category}': {len(products)} products")
                
                # Delay between categories
                if idx < len(categories_to_scrape):
                    logger.info("⏳ Waiting 3 seconds before next category...")
                    time.sleep(3)
                
            except KeyboardInterrupt:
                logger.warning(f"⚠️  Scraping interrupted by user at category '{category}'")
                results[category] = []
                break
            except Exception as e:
                logger.error(f"❌ Error scraping category '{category}': {e}", exc_info=True)
                results[category] = []
        
        # Final summary
        total = sum(len(p) for p in results.values())
        logger.info(f"\n{'='*70}")
        logger.info("🎉 SCRAPING COMPLETED")
        logger.info(f"{'='*70}")
        logger.info(f"Total categories scraped: {len(results)}")
        logger.info(f"Total products scraped: {total}")
        logger.info(f"\nBreakdown by category:")
        for cat, prods in results.items():
            logger.info(f"  • {cat}: {len(prods)} products")
        logger.info(f"{'='*70}\n")
        
        return results
    
    def start(self):
        """Manually start the browser (alternative to context manager)"""
        if not self.playwright:
            self.playwright = sync_playwright().start()
        if not self.browser:
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=['--disable-blink-features=AutomationControlled']
            )
        if not self.context:
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
        if not self.page:
            self.page = self.context.new_page()
    
    def stop(self):
        """Manually stop the browser"""
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        self.page = None
        self.context = None