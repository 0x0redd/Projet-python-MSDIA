"""
Marjanemall.ma scraper using Playwright
Uses browser automation to extract fully rendered product data
"""

from playwright.sync_api import sync_playwright, Page, BrowserContext
import time
import json
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class MarjanemallScraper:
    """Scraper for Marjanemall.ma using Playwright"""
    
    # Category slugs to scrape
    CATEGORIES = [
        'telephone-objets-connectes',
        'informatique-gaming',
        'electromenager',
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
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def extract_price(self, price_text: str) -> Optional[float]:
        """
        Extract numeric price from text
        
        Args:
            price_text: Price text (e.g., "99.00 DH" or "99 DH")
            
        Returns:
            Float price value or None
        """
        if not price_text:
            return None
        
        # Handle price ranges
        if ' - ' in price_text or ' – ' in price_text or ' — ' in price_text:
            price_text = re.split(r'\s*[-–—]\s*', price_text)[0].strip()
        
        # Remove currency and spaces, keep only digits and dots
        price_clean = re.sub(r'[^\d.]', '', price_text.replace(' ', ''))
        
        try:
            return float(price_clean)
        except ValueError:
            logger.warning(f"Could not parse price: {price_text}")
            return None
    
    def extract_products_from_page(self, page: Page, category: str, page_num: int = 1) -> List[Dict]:
        """
        Extract products from a rendered page using JavaScript
        
        Args:
            page: Playwright page object
            category: Category name
            page_num: Page number
            
        Returns:
            List of product dictionaries
        """
        try:
            # Execute JavaScript to extract product data
            product_data = page.evaluate("""
                () => {
                    const products = [];
                    
                    // Look for product elements
                    const productElements = document.querySelectorAll('div.animate-slideUp');
                    
                    productElements.forEach(element => {
                        const product = {};
                        
                        // Find the main link element
                        const linkElem = element.querySelector('a[href^="/p/"]');
                        if (!linkElem) return;
                        
                        // Get product URL and ID
                        product.product_url = linkElem.getAttribute('href');
                        const urlMatch = product.product_url.match(/([a-z0-9]{8,})$/i);
                        if (urlMatch) {
                            product.product_id = urlMatch[1].toUpperCase();
                        }
                        
                        // Get product name
                        const nameElem = linkElem.querySelector('h3.text-sm.font-medium.text-gray-800');
                        if (!nameElem) {
                            nameElem = linkElem.querySelector('h3');
                        }
                        if (nameElem) {
                            product.product_name = nameElem.textContent.trim();
                        }
                        
                        // Get current price
                        const priceContainer = linkElem.querySelector('div.flex.items-center.gap-0.5');
                        if (priceContainer) {
                            const mainPrice = priceContainer.querySelector('span.text-lg, span.text-xl');
                            if (mainPrice) {
                                const mainPriceText = mainPrice.textContent.trim();
                                
                                // Get decimal parts
                                const decimalSpans = priceContainer.querySelectorAll('span.text-\\[8px\\], span.text-\\[9px\\]');
                                let decimals = '';
                                decimalSpans.forEach(span => {
                                    const text = span.textContent.trim();
                                    if (text && text !== 'DH' && !isNaN(text)) {
                                        decimals += text;
                                    }
                                });
                                
                                if (decimals) {
                                    product.current_price_text = mainPriceText + '.' + decimals + ' DH';
                                } else {
                                    product.current_price_text = mainPriceText + ' DH';
                                }
                            }
                        }
                        
                        // Get original price
                        const oldPriceElem = linkElem.querySelector('span.text-xs.text-gray-400.line-through');
                        if (oldPriceElem) {
                            product.original_price = oldPriceElem.textContent.trim();
                        }
                        
                        // Get discount
                        const discountElem = linkElem.querySelector('span.bg-\\[#e91e63\\]');
                        if (discountElem) {
                            product.discount_percentage = discountElem.textContent.trim();
                        }
                        
                        // Get brand (from image or name)
                        const brandImg = element.querySelector('img[alt="Brand"]');
                        if (brandImg) {
                            const brandSrc = brandImg.getAttribute('src') || '';
                            const brandMatch = brandSrc.match(/([^/]+)_\\d+\\.png/);
                            if (brandMatch) {
                                product.brand = brandMatch[1].toUpperCase();
                            }
                        }
                        
                        // Extract brand from name if not found
                        if (!product.brand && product.product_name) {
                            const nameParts = product.product_name.split(' - ');
                            if (nameParts.length > 0) {
                                product.brand = nameParts[0].trim();
                            }
                        }
                        
                        // Get seller
                        const sellerSpans = linkElem.querySelectorAll('span');
                        for (const span of sellerSpans) {
                            const text = span.textContent.trim();
                            if (text.includes('Vendu par')) {
                                const match = text.match(/Vendu par\\s+(.+)/i);
                                if (match) {
                                    product.seller = match[1].trim();
                                    break;
                                }
                            }
                        }
                        
                        // Get image
                        const imgElem = linkElem.querySelector('img.object-contain, img');
                        if (imgElem) {
                            product.image_url = imgElem.getAttribute('src') || imgElem.getAttribute('data-src');
                            product.image_alt = imgElem.getAttribute('alt');
                        }
                        
                        // Check for fast delivery
                        const expressText = linkElem.textContent || '';
                        product.fast_delivery = expressText.includes('Livraison rapide');
                        
                        // Only add if we have essential data
                        if (product.product_name || product.product_id) {
                            products.push(product);
                        }
                    });
                    
                    return products;
                }
            """)
            
            # Process and format products
            formatted_products = []
            for product in product_data:
                formatted_product = {
                    'product_id': product.get('product_id'),
                    'product_name': product.get('product_name'),
                    'product_url': product.get('product_url'),
                    'full_url': urljoin(self.base_url, product.get('product_url', '')),
                    'current_price_text': product.get('current_price_text'),
                    'current_price': self.extract_price(product.get('current_price_text', '')),
                    'original_price': product.get('original_price'),
                    'original_price_value': self.extract_price(product.get('original_price', '')),
                    'discount_percentage': product.get('discount_percentage'),
                    'brand': product.get('brand'),
                    'seller': product.get('seller'),
                    'image_url': product.get('image_url'),
                    'image_alt': product.get('image_alt'),
                    'fast_delivery': product.get('fast_delivery', False),
                    'category': category,
                    'page_number': page_num,
                    'scraped_timestamp': datetime.now().isoformat() + 'Z',
                    'source': 'marjanemall.ma'
                }
                
                # Calculate discount value
                if formatted_product.get('current_price') and formatted_product.get('original_price_value'):
                    try:
                        current = float(formatted_product['current_price'])
                        original = float(formatted_product['original_price_value'])
                        if original > 0 and current < original:
                            discount_percent = ((original - current) / original) * 100
                            formatted_product['discount_value'] = int(discount_percent)
                    except (ValueError, TypeError):
                        formatted_product['discount_value'] = None
                else:
                    formatted_product['discount_value'] = None
                
                formatted_products.append(formatted_product)
            
            logger.info(f"Extracted {len(formatted_products)} products from page {page_num}")
            return formatted_products
            
        except Exception as e:
            logger.error(f"Error extracting products from page: {e}", exc_info=True)
            return []
    
    def is_empty_page(self, page: Page) -> bool:
        """
        Check if the page is empty (no products found)
        
        Args:
            page: Playwright page object
            
        Returns:
            True if page is empty, False otherwise
        """
        try:
            empty_check = page.evaluate("""
                () => {
                    // Check for "Aucun produit trouvé" message
                    const emptyText = document.body.textContent || '';
                    if (emptyText.includes('Aucun produit trouvé')) {
                        return true;
                    }
                    
                    // Check if grid is empty
                    const grid = document.querySelector('div.grid.grid-cols-2');
                    if (grid) {
                        const productElements = grid.querySelectorAll('div.animate-slideUp');
                        return productElements.length === 0;
                    }
                    
                    return false;
                }
            """)
            return empty_check
        except:
            return False
    
    def scrape_category_page(self, category: str, page_num: int = 1) -> List[Dict]:
        """
        Scrape a single category page
        
        Args:
            category: Category slug
            page_num: Page number
            
        Returns:
            List of product dictionaries
        """
        if not self.context:
            raise RuntimeError("Scraper not initialized. Use 'with' statement or call start() first")
        
        # Construct URL
        if page_num == 1:
            url = f"{self.base_url}/{category}"
        else:
            url = f"{self.base_url}/{category}?page={page_num}"
        
        logger.info(f"Scraping page {page_num} of category '{category}': {url}")
        
        page = self.context.new_page()
        
        try:
            # Navigate to page
            page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            time.sleep(3)
            
            # Check for empty page
            if self.is_empty_page(page):
                logger.info(f"Empty page detected on page {page_num}")
                return []
            
            # Scroll to trigger lazy loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(self.scroll_delay)
            
            # Scroll back up and down again to ensure all content loads
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(self.scroll_delay)
            
            # Extract products
            products = self.extract_products_from_page(page, category, page_num)
            
            return products
            
        except Exception as e:
            logger.error(f"Error scraping page {page_num} of {category}: {e}", exc_info=True)
            return []
        finally:
            page.close()
    
    def scrape_category(self, category: str, max_pages: int = 150) -> List[Dict]:
        """
        Scrape all pages of a category
        
        Args:
            category: Category slug
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of all product dictionaries from the category
        """
        all_products = []
        seen_product_ids = set()
        
        logger.info(f"Starting to scrape category: {category}")
        
        page = 1
        while page <= max_pages:
            products = self.scrape_category_page(category, page)
            
            if not products:
                logger.info(f"No products found on page {page}, stopping")
                break
            
            # Filter duplicates
            new_products = []
            for product in products:
                product_id = product.get('product_id')
                if product_id and product_id not in seen_product_ids:
                    seen_product_ids.add(product_id)
                    new_products.append(product)
                elif not product_id:
                    # Use URL as fallback
                    url = product.get('product_url')
                    if url and url not in seen_product_ids:
                        seen_product_ids.add(url)
                        new_products.append(product)
            
            if not new_products:
                logger.info(f"All products on page {page} are duplicates, stopping")
                break
            
            all_products.extend(new_products)
            logger.info(f"Page {page}: Added {len(new_products)} new products (total: {len(all_products)})")
            
            page += 1
            
            # Delay between pages
            time.sleep(self.scroll_delay)
        
        logger.info(f"Category '{category}': Scraped {len(all_products)} total products from {page-1} pages")
        return all_products
    
    def scrape_all_categories(self, max_pages_per_category: int = 150) -> Dict[str, List[Dict]]:
        """
        Scrape all categories
        
        Args:
            max_pages_per_category: Maximum pages per category
            
        Returns:
            Dictionary mapping category names to product lists
        """
        results = {}
        
        for category in self.CATEGORIES:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting category: {category}")
            logger.info(f"{'='*60}")
            
            try:
                products = self.scrape_category(category, max_pages_per_category)
                results[category] = products
                logger.info(f"Completed '{category}': {len(products)} products")
                
                # Delay between categories
                time.sleep(self.scroll_delay * 2)
                
            except Exception as e:
                logger.error(f"Error scraping category '{category}': {e}", exc_info=True)
                results[category] = []
        
        # Summary
        total = sum(len(p) for p in results.values())
        logger.info(f"\n{'='*60}")
        logger.info("SCRAPING COMPLETED")
        logger.info(f"{'='*60}")
        logger.info(f"Total categories: {len(results)}")
        logger.info(f"Total products: {total}")
        
        for cat, prods in results.items():
            logger.info(f"  {cat}: {len(prods)} products")
        
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
    
    def stop(self):
        """Manually stop the browser"""
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        self.context = None
