"""
Web scraper for Marjanemall.ma with infinite scroll support
Extracts product information from category pages with dynamic loading
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import random
import json
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class MarjanemallScraper:
    """Scraper for Marjanemall.ma website with infinite scroll support"""
    
    # Category URLs to scrape
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
    
    def __init__(self, base_url: str = "https://www.marjanemall.ma", delay: float = 3.0):
        """
        Initialize the Marjanemall scraper
        
        Args:
            base_url: Base URL of Marjanemall.ma
            delay: Delay between requests in seconds (default: 3.0s)
        """
        self.base_url = base_url.rstrip('/')
        self.delay = delay
        self.session = requests.Session()
        
        # Realistic browser headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
    
    def get_page_with_scroll_simulation(self, url: str, scroll_pages: int = 5) -> Optional[str]:
        """
        Get page content by simulating multiple scrolls to trigger infinite loading
        
        Args:
            url: URL to fetch
            scroll_pages: Number of times to "scroll" (load more batches)
            
        Returns:
            HTML content or None if error
        """
        all_html_parts = []
        seen_product_urls = set()
        
        for page_num in range(1, scroll_pages + 1):
            try:
                # Construct URL with page parameter
                if '?' in url:
                    page_url = f"{url}&page={page_num}"
                else:
                    page_url = f"{url}?page={page_num}"
                
                logger.info(f"Loading scroll batch {page_num}/{scroll_pages}: {page_url}")
                
                # Add delay with random jitter
                if page_num > 1:
                    time.sleep(self.delay + random.uniform(1, 3))
                
                response = self.session.get(page_url, timeout=30)
                response.raise_for_status()
                
                # Check if we got valid content
                if len(response.text) < 1000:
                    logger.warning(f"Very small response on batch {page_num}")
                    continue
                
                # Parse the HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if this page has products
                product_count = self.count_products_in_html(response.text)
                
                if product_count == 0:
                    logger.info(f"No products found in batch {page_num}, might be at the end")
                    
                    # Check for empty page message
                    if 'Aucun produit trouvé' in response.text:
                        logger.info(f"'Aucun produit trouvé' found, stopping")
                        break
                    
                    # Try one more page before stopping
                    if page_num < scroll_pages:
                        continue
                    else:
                        break
                
                logger.info(f"Found {product_count} products in batch {page_num}")
                
                # Extract new product URLs to check if we're getting new content
                new_product_urls = self.extract_product_urls_from_html(response.text)
                new_unique_urls = [url for url in new_product_urls if url not in seen_product_urls]
                
                if not new_unique_urls and page_num > 1:
                    logger.info(f"No new products in batch {page_num}, might be duplicate content")
                    # Try one more page to be sure
                    if page_num < scroll_pages - 1:
                        continue
                    else:
                        break
                
                # Add new URLs to seen set
                seen_product_urls.update(new_product_urls)
                
                # Store this HTML part
                all_html_parts.append(response.text)
                
                # If we got fewer than expected products, might be near the end
                if product_count < 20 and page_num > 1:
                    logger.info(f"Few products ({product_count}), might be near end of category")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error loading batch {page_num}: {e}")
                if page_num > 1:
                    # If we already have some conAVtent, return what we have
                    break
                else:
                    return None
        
        if not all_html_parts:
            return None
        
        # Combine all HTML parts (or just use the last one since it should contain all loaded products)
        # For infinite scroll, the last page usually contains all products loaded so far
        return all_html_parts[-1]
    
    def count_products_in_html(self, html_content: str) -> int:
        """Count products in HTML content"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Count product cards
        product_cards = soup.find_all('div', class_='animate-slideUp')
        if product_cards:
            return len(product_cards)
        
        # Count product links
        product_links = soup.find_all('a', href=re.compile(r'^/p/'))
        unique_links = set()
        for link in product_links:
            href = link.get('href', '')
            if href:
                unique_links.add(href)
        
        return len(unique_links)
    
    def extract_product_urls_from_html(self, html_content: str) -> List[str]:
        """Extract product URLs from HTML content"""
        soup = BeautifulSoup(html_content, 'html.parser')
        product_urls = []
        
        product_links = soup.find_all('a', href=re.compile(r'^/p/'))
        for link in product_links:
            href = link.get('href', '')
            if href:
                product_urls.append(href)
        
        return list(set(product_urls))  # Remove duplicates
    
    def get_page(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """
        Fetch a page with retry logic
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            BeautifulSoup object or None if error
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = self.delay * (2 ** attempt) + random.uniform(1, 3)
                    logger.warning(f"Retry {attempt}/{max_retries-1} for {url} after {wait_time:.1f}s")
                    time.sleep(wait_time)
                
                logger.info(f"Fetching: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Check for blocking
                if len(response.content) < 1000:
                    logger.warning(f"Small response ({len(response.content)} bytes)")
                    if attempt < max_retries - 1:
                        continue
                
                # Parse HTML
                try:
                    soup = BeautifulSoup(response.content, 'lxml')
                except:
                    soup = BeautifulSoup(response.content, 'html.parser')
                
                # Basic validation
                if not soup or not soup.find('body'):
                    logger.warning("Invalid HTML")
                    if attempt < max_retries - 1:
                        continue
                    return None
                
                time.sleep(self.delay + random.uniform(0.5, 1.5))
                return soup
                
            except Exception as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    continue
        
        return None
    
    def extract_products_from_html(self, html_content: str, category: str, page_num: int = 1) -> List[Dict]:
        """
        Extract products from HTML content
        
        Args:
            html_content: HTML string
            category: Category name
            page_num: Page number
            
        Returns:
            List of product dictionaries
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        products = []
        
        # Find product cards
        product_cards = soup.find_all('div', class_='animate-slideUp')
        
        # Fallback: look for product links
        if not product_cards:
            product_links = soup.find_all('a', href=re.compile(r'^/p/'))
            for link in product_links:
                # Check if it looks like a product card
                classes = link.get('class', [])
                class_str = ' '.join(classes) if isinstance(classes, list) else str(classes)
                if 'bg-white' in class_str and 'rounded' in class_str:
                    product_cards.append(link)
        
        logger.info(f"Found {len(product_cards)} product cards in HTML")
        
        # Parse each product
        for card in product_cards:
            try:
                product = self.parse_product_card(card)
                if product:
                    product['category'] = category
                    product['page_number'] = page_num
                    product['scraped_timestamp'] = datetime.now().isoformat() + 'Z'
                    products.append(product)
            except Exception as e:
                logger.error(f"Error parsing product card: {e}")
                continue
        
        return products
    
    def parse_product_card(self, product_card) -> Optional[Dict]:
        """
        Parse a single product card element
        
        Args:
            product_card: BeautifulSoup element containing product info
            
        Returns:
            Dictionary with product information or None
        """
        try:
            product = {}
            
            # Get the link element
            if hasattr(product_card, 'name') and product_card.name == 'a':
                link_elem = product_card
            else:
                link_elem = product_card.find('a', href=re.compile(r'^/p/'))
                if not link_elem:
                    return None
            
            # Basic info
            href = link_elem.get('href', '')
            if not href:
                return None
            
            product['product_url'] = href
            product['full_url'] = urljoin(self.base_url, href)
            
            # Product ID
            product['product_id'] = self.extract_product_id(product_card)
            
            # Product name
            name_elem = link_elem.find('h3', class_=re.compile(r'text-sm.*font-medium.*text-gray-800'))
            if not name_elem:
                name_elem = link_elem.find('h3')
            
            if name_elem:
                product['product_name'] = name_elem.get_text(strip=True)
            else:
                # Try to get from image alt
                img_elem = link_elem.find('img')
                if img_elem:
                    alt_text = img_elem.get('alt', '')
                    if alt_text:
                        product['product_name'] = alt_text
                else:
                    return None  # Skip if no name
            
            # Price - look for the price container
            price_container = None
            
            # Try multiple selectors for price container
            selectors = [
                'div.flex.items-center.gap-0\\.5',
                'div.flex.items-center.gap',
                'div.flex.items-center'
            ]
            
            for selector in selectors:
                price_container = link_elem.select_one(selector)
                if price_container and price_container.find('span', class_=re.compile(r'text-lg.*font-extrabold.*text-primary')):
                    break
            
            if price_container:
                # Get main price
                main_price_elem = price_container.find('span', class_=re.compile(r'text-lg.*font-extrabold.*text-primary'))
                if main_price_elem:
                    main_price = main_price_elem.get_text(strip=True)
                    
                    # Get decimal parts
                    decimal_container = price_container.find('div', class_='flex flex-col items-center justify-center')
                    if decimal_container:
                        decimal_spans = decimal_container.find_all('span', class_=re.compile(r'text-\[[89]px\].*font-bold.*text-primary'))
                        decimals = ''.join([span.get_text(strip=True) for span in decimal_spans[:2]])
                        
                        if decimals:
                            price_text = f"{main_price}.{decimals} DH"
                            product['current_price_text'] = price_text
                            
                            # Parse numeric price
                            try:
                                product['current_price'] = float(f"{main_price}.{decimals}")
                            except:
                                product['current_price'] = None
                        else:
                            product['current_price_text'] = f"{main_price} DH"
                            product['current_price'] = self.extract_numeric_price(main_price)
                    else:
                        product['current_price_text'] = f"{main_price} DH"
                        product['current_price'] = self.extract_numeric_price(main_price)
            
            # Original price
            old_price_elem = link_elem.find('span', class_='line-through')
            if old_price_elem:
                old_price_text = old_price_elem.get_text(strip=True)
                product['original_price'] = old_price_text
                product['original_price_value'] = self.extract_numeric_price(old_price_text)
            
            # Discount
            discount_elem = link_elem.find('span', class_=re.compile(r'bg-\[#e91e63\]'))
            if discount_elem:
                discount_text = discount_elem.get_text(strip=True)
                product['discount_percentage'] = discount_text
                
                # Extract numeric discount
                match = re.search(r'(\d+)', discount_text)
                if match:
                    product['discount_value'] = int(match.group(1))
            
            # Brand (from product name)
            if product.get('product_name'):
                parts = product['product_name'].split(' - ')
                if len(parts) > 1:
                    product['brand'] = parts[0].strip()
            
            # Seller
            seller_spans = link_elem.find_all('span')
            for span in seller_spans:
                span_text = span.get_text(strip=True)
                if 'Vendu par' in span_text:
                    match = re.search(r'Vendu par\s+(.+)', span_text, re.IGNORECASE)
                    if match:
                        product['seller'] = match.group(1).strip()
                    break
            
            # Image
            img_elem = link_elem.find('img', class_=re.compile(r'object-contain.*mix-blend-multiply'))
            if not img_elem:
                img_elem = link_elem.find('img')
            
            if img_elem:
                product['image_url'] = img_elem.get('src') or img_elem.get('data-src')
                product['image_alt'] = img_elem.get('alt', '')
            
            # Fast delivery
            if link_elem.find(string=re.compile(r'Livraison rapide', re.IGNORECASE)):
                product['fast_delivery'] = True
            else:
                product['fast_delivery'] = False
            
            return product
            
        except Exception as e:
            logger.error(f"Error parsing product card: {e}")
            return None
    
    def extract_product_id(self, product_card) -> Optional[str]:
        """Extract product ID from product card"""
        # From cart button
        cart_button = product_card.find('button', id=re.compile(r'cart-btn-'))
        if cart_button:
            button_id = cart_button.get('id', '')
            match = re.search(r'cart-btn-(.+)', button_id)
            if match:
                return match.group(1).upper()
        
        # From URL
        link = product_card.find('a', href=re.compile(r'^/p/'))
        if not link and hasattr(product_card, 'get'):
            link = product_card
        
        if link:
            href = link.get('href', '')
            # Extract last part as ID (e.g., /p/product-name-aaali50673)
            parts = href.split('-')
            if parts:
                last_part = parts[-1]
                if re.match(r'^[a-z]{4,5}\d{5}$', last_part, re.IGNORECASE):
                    return last_part.upper()
        
        return None
    
    def extract_numeric_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        
        # Remove currency symbols and text
        price_text = re.sub(r'[^\d.,]', '', price_text)
        
        # Handle European format (1.234,56 -> 1234.56)
        if ',' in price_text and '.' in price_text:
            # Format: 1.234,56
            price_text = price_text.replace('.', '').replace(',', '.')
        elif ',' in price_text:
            # Format: 1234,56
            price_text = price_text.replace(',', '.')
        
        try:
            return float(price_text)
        except ValueError:
            return None
    
    def scrape_category(self, category: str, max_scroll_batches: int = 10) -> List[Dict]:
        """
        Scrape a category with infinite scroll simulation
        
        Args:
            category: Category slug
            max_scroll_batches: Maximum number of scroll batches to simulate
            
        Returns:
            List of product dictionaries
        """
        category_url = f"{self.base_url}/{category}"
        all_products = []
        seen_product_ids = set()
        
        logger.info(f"Starting to scrape category: {category}")
        logger.info(f"Will simulate {max_scroll_batches} scroll batches")
        
        # Get initial page with scroll simulation
        html_content = self.get_page_with_scroll_simulation(category_url, scroll_pages=max_scroll_batches)
        
        if not html_content:
            logger.warning(f"Could not load content for {category}")
            return []
        
        # Extract all products from the loaded content
        products = self.extract_products_from_html(html_content, category, 1)
        
        # Filter duplicates
        for product in products:
            product_id = product.get('product_id')
            product_url = product.get('product_url')
            
            # Use product_id if available, otherwise product_url
            key = product_id or product_url
            
            if key and key not in seen_product_ids:
                seen_product_ids.add(key)
                all_products.append(product)
        
        logger.info(f"Category '{category}': Found {len(all_products)} unique products")
        return all_products
    
    def scrape_all_categories(self, max_scroll_batches: int = 10) -> Dict[str, List[Dict]]:
        """
        Scrape all categories with infinite scroll support
        
        Args:
            max_scroll_batches: Maximum scroll batches per category
            
        Returns:
            Dictionary mapping category names to product lists
        """
        results = {}
        
        for category in self.CATEGORIES:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting category: {category}")
            logger.info(f"{'='*60}")
            
            try:
                products = self.scrape_category(category, max_scroll_batches=max_scroll_batches)
                results[category] = products
                logger.info(f"Completed '{category}': {len(products)} products")
                
                # Longer delay between categories
                time.sleep(self.delay + random.uniform(3, 6))
                
            except Exception as e:
                logger.error(f"Error scraping category '{category}': {e}", exc_info=True)
                results[category] = []
                time.sleep(10)  # Wait longer after error
        
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