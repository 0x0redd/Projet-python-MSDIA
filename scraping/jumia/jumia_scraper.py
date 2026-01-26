"""
Web scraper for Jumia.ma - Téléphone & Tablette category
Extracts product information including name, price, discount, rating, etc.
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import json
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JumiaScraper:
    """Scraper for Jumia.ma website"""
    
    def __init__(self, base_url: str = "https://www.jumia.ma", delay: float = 1.0):
        """
        Initialize the Jumia scraper
        
        Args:
            base_url: Base URL of Jumia.ma
            delay: Delay between requests in seconds (to be respectful)
        """
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get_page(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """
        Fetch a page with retry logic and return BeautifulSoup object
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            BeautifulSoup object or None if error
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = self.delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Retry {attempt}/{max_retries-1} for {url} after {wait_time:.1f}s")
                    time.sleep(wait_time)
                else:
                    logger.info(f"Fetching: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Validate response content
                if not response.content:
                    logger.warning(f"Empty response from {url}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                
                soup = BeautifulSoup(response.content, 'lxml')
                
                # Validate we got actual HTML
                if not soup or not soup.find('body'):
                    logger.warning(f"Invalid HTML from {url}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                
                time.sleep(self.delay)  # Be respectful with requests
                return soup
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    continue
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Page not found (404): {url}")
                    return None  # Don't retry 404s
                elif e.response.status_code >= 500:
                    logger.warning(f"Server error {e.response.status_code} for {url} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        continue
                else:
                    logger.error(f"HTTP error {e.response.status_code} for {url}: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    continue
        
        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def extract_price(self, price_text: str) -> Optional[float]:
        """
        Extract numeric price from text
        Handles price ranges by taking the first (lower) price
        
        Args:
            price_text: Price text (e.g., "1,099.00 Dhs" or "169.00 Dhs - 179.00 Dhs")
            
        Returns:
            Float price value or None
        """
        if not price_text:
            return None
        
        # Handle price ranges (e.g., "169.00 Dhs - 179.00 Dhs")
        # Take the first price (lower price)
        if ' - ' in price_text or ' – ' in price_text or ' — ' in price_text:
            # Split by common range separators
            price_text = re.split(r'\s*[-–—]\s*', price_text)[0].strip()
        
        # Remove currency and spaces, replace comma with dot
        price_clean = re.sub(r'[^\d,.]', '', price_text.replace(' ', ''))
        price_clean = price_clean.replace(',', '')
        
        try:
            return float(price_clean)
        except ValueError:
            logger.warning(f"Could not parse price: {price_text}")
            return None
    
    def extract_rating(self, rating_text: str) -> Optional[float]:
        """
        Extract rating from text
        
        Args:
            rating_text: Rating text (e.g., "4.1 out of 5")
            
        Returns:
            Float rating value or None
        """
        if not rating_text:
            return None
        
        # Extract number before "out of"
        match = re.search(r'(\d+\.?\d*)\s*out\s*of', rating_text)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def extract_review_count(self, review_text: str) -> Optional[int]:
        """
        Extract review count from text
        
        Args:
            review_text: Review text (e.g., "(90)")
            
        Returns:
            Integer review count or None
        """
        if not review_text:
            return None
        
        # Extract number in parentheses
        match = re.search(r'\((\d+)\)', review_text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def extract_discount(self, discount_text: str) -> Optional[float]:
        """
        Extract discount percentage from text
        
        Args:
            discount_text: Discount text (e.g., "25%")
            
        Returns:
            Float discount percentage or None
        """
        if not discount_text:
            return None
        
        # Extract percentage
        match = re.search(r'(\d+)%', discount_text)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def parse_product_card(self, product_article) -> Optional[Dict]:
        """
        Parse a single product card/article element
        
        Args:
            product_article: BeautifulSoup article element
            
        Returns:
            Dictionary with product information or None
        """
        try:
            product = {}
            
            # Product link - get this first as it contains useful data attributes
            link_elem = product_article.find('a', class_='core')
            if not link_elem:
                logger.warning("No product link found, skipping product")
                return None
            
            # Product URL
            if link_elem.get('href'):
                product['url'] = urljoin(self.base_url, link_elem['href'])
                # Extract product ID from data attributes or URL
                product['product_id'] = (
                    link_elem.get('data-gtm-id') or 
                    link_elem.get('data-ga4-item_id') or 
                    self._extract_id_from_url(link_elem['href'])
                )
            else:
                product['url'] = None
                product['product_id'] = None
            
            # Product name - try multiple locations
            name_elem = product_article.find('h3', class_='name')
            if not name_elem:
                # Try finding in info div
                info_div = product_article.find('div', class_='info')
                if info_div:
                    name_elem = info_div.find('h3', class_='name')
            if not name_elem:
                # Try data attributes as fallback
                name_elem = link_elem.get('data-ga4-item_name') or link_elem.get('data-gtm-name')
            
            if name_elem:
                if isinstance(name_elem, str):
                    product['name'] = name_elem.strip()
                else:
                    product['name'] = name_elem.get_text(strip=True)
            else:
                product['name'] = None
            
            # Current price
            price_elem = product_article.find('div', class_='prc')
            if not price_elem:
                # Try finding in info div
                info_div = product_article.find('div', class_='info')
                if info_div:
                    price_elem = info_div.find('div', class_='prc')
            
            price_text = price_elem.get_text(strip=True) if price_elem else None
            if not price_text:
                # Try data attributes as fallback
                price_text = link_elem.get('data-ga4-price') or link_elem.get('data-gtm-price')
                if price_text:
                    # Convert to Dhs format for consistency
                    try:
                        price_float = float(price_text)
                        price_text = f"{price_float:.2f} Dhs"
                    except ValueError:
                        pass
            
            product['price'] = self.extract_price(price_text) if price_text else None
            product['price_text'] = price_text
            
            # Old price - look in s-prc-w section
            old_price_elem = None
            s_prc_w = product_article.find('div', class_='s-prc-w')
            if s_prc_w:
                old_price_elem = s_prc_w.find('div', class_='old')
            
            old_price_text = old_price_elem.get_text(strip=True) if old_price_elem else None
            product['old_price'] = self.extract_price(old_price_text) if old_price_text else None
            product['old_price_text'] = old_price_text
            
            # Discount - try multiple selectors
            discount_elem = None
            # Try finding discount badge in s-prc-w section
            if s_prc_w:
                discount_elem = s_prc_w.find('div', class_='bdg')
                if discount_elem:
                    classes = discount_elem.get('class', [])
                    if '_dsct' not in ' '.join(classes):
                        discount_elem = None
            
            if not discount_elem:
                # Try finding any bdg with _dsct class
                all_badges = product_article.find_all('div', class_='bdg')
                for badge in all_badges:
                    classes = badge.get('class', [])
                    if '_dsct' in ' '.join(classes):
                        discount_elem = badge
                        break
            
            discount_text = discount_elem.get_text(strip=True) if discount_elem else None
            if not discount_text:
                # Try data attributes
                discount_text = link_elem.get('data-ga4-discount')
                if discount_text:
                    try:
                        discount_float = float(discount_text)
                        discount_text = f"{discount_float:.0f}%"
                    except ValueError:
                        pass
            
            product['discount'] = self.extract_discount(discount_text) if discount_text else None
            product['discount_text'] = discount_text
            
            # Rating and review count - try data attributes first (most reliable)
            # Check multiple elements for data attributes
            rating_attr = None
            review_count_attr = None
            
            # Try link element first
            rating_attr = link_elem.get('data-gtm-dimension27') or link_elem.get('data-ga4-item_rating')
            review_count_attr = link_elem.get('data-gtm-dimension26') or link_elem.get('data-ga4-item_review_count')
            
            # If not found, check the article element itself
            if not rating_attr:
                rating_attr = product_article.get('data-gtm-dimension27') or product_article.get('data-ga4-item_rating')
            if not review_count_attr:
                review_count_attr = product_article.get('data-gtm-dimension26') or product_article.get('data-ga4-item_review_count')
            
            # Also check form elements (some products have data in the form)
            if not rating_attr or not review_count_attr:
                form_elem = product_article.find('form')
                if form_elem:
                    if not rating_attr:
                        rating_attr = form_elem.get('data-gtm-dimension27')
                    if not review_count_attr:
                        review_count_attr = form_elem.get('data-gtm-dimension26')
            
            if rating_attr and str(rating_attr).strip():
                try:
                    product['rating'] = float(rating_attr)
                except (ValueError, TypeError):
                    product['rating'] = None
            else:
                product['rating'] = None
            
            if review_count_attr and str(review_count_attr).strip():
                try:
                    product['review_count'] = int(review_count_attr)
                except (ValueError, TypeError):
                    product['review_count'] = None
            else:
                product['review_count'] = None
            
            # If data attributes didn't work, try extracting from HTML
            if product['rating'] is None or product['review_count'] is None:
                rev_elem = product_article.find('div', class_='rev')
                if not rev_elem:
                    # Try finding in info div
                    info_div = product_article.find('div', class_='info')
                    if info_div:
                        rev_elem = info_div.find('div', class_='rev')
                
                if rev_elem:
                    stars_elem = rev_elem.find('div', class_='stars')
                    if stars_elem:
                        rating_text = stars_elem.get_text(strip=True)
                        if product['rating'] is None:
                            product['rating'] = self.extract_rating(rating_text)
                        # Review count is usually after the stars div
                        review_text = rev_elem.get_text(strip=True)
                        if product['review_count'] is None:
                            product['review_count'] = self.extract_review_count(review_text)
                    else:
                        # Try to extract from rev element directly
                        rev_text = rev_elem.get_text(strip=True)
                        if product['rating'] is None:
                            product['rating'] = self.extract_rating(rev_text)
                        if product['review_count'] is None:
                            product['review_count'] = self.extract_review_count(rev_text)
            
            # Image URL - check both src and data-src (for lazy loading)
            img_elem = product_article.find('div', class_='img-c')
            if img_elem:
                img = img_elem.find('img')
                if img:
                    # Prefer data-src for lazy-loaded images, fallback to src
                    product['image_url'] = img.get('data-src') or img.get('src')
                else:
                    product['image_url'] = None
            else:
                product['image_url'] = None
            
            # Brand - try data attributes first, then extract from name
            brand = (
                link_elem.get('data-ga4-item_brand') or 
                link_elem.get('data-gtm-brand') or
                link_elem.get('data-moengage-brand_name')
            )
            if brand:
                product['brand'] = brand
            else:
                # Extract from product name
                product['brand'] = self._extract_brand(product)
            
            # Official store badge
            badge_elem = None
            all_badges = product_article.find_all('div', class_='bdg')
            for badge in all_badges:
                classes = badge.get('class', [])
                if '_mall' in ' '.join(classes):
                    badge_elem = badge
                    break
            product['is_official_store'] = badge_elem is not None
            
            # Express delivery badge - look for svg with xprss class
            express_elem = product_article.find('svg', class_='xprss')
            if not express_elem:
                # Also check for aria-label
                express_elem = product_article.find('svg', {'aria-label': 'Livraison rapide'})
            product['express_delivery'] = express_elem is not None
            
            # Timestamp
            product['scraped_at'] = datetime.now().isoformat()
            
            # Source
            product['source'] = 'jumia.ma'
            
            return product
            
        except Exception as e:
            logger.error(f"Error parsing product card: {e}", exc_info=True)
            return None
    
    def _extract_id_from_url(self, url: str) -> Optional[str]:
        """Extract product ID from URL"""
        # Jumia URLs often have format: /product-name-productid.html
        match = re.search(r'-(\w+)\.html', url)
        if match:
            return match.group(1)
        return None
    
    def _extract_brand(self, product: Dict) -> Optional[str]:
        """Extract brand from product name or data attributes"""
        name = product.get('name', '')
        if not name:
            return None
        
        # Common brands to look for
        brands = ['Samsung', 'XIAOMI', 'Xiaomi', 'Apple', 'iPhone', 'Itel', 'Honor', 
                  'Oppo', 'Tecno', 'Infinix', 'Realme', 'Redmi', 'Huawei', 'Nokia']
        
        for brand in brands:
            if brand.lower() in name.lower():
                return brand
        
        return None
    
    def _extract_json_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """
        Extract JSON data from window.__STORE__ script tag
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary with store data or None
        """
        try:
            # Find script tag containing window.__STORE__
            scripts = soup.find_all('script')
            for script in scripts:
                script_text = script.string
                if not script_text or 'window.__STORE__' not in script_text:
                    continue
                
                # Extract the JSON part - find window.__STORE__ = { ... };
                # Need to match balanced braces
                start_idx = script_text.find('window.__STORE__')
                if start_idx == -1:
                    continue
                
                # Find the = sign after __STORE__
                equals_idx = script_text.find('=', start_idx)
                if equals_idx == -1:
                    continue
                
                # Find the opening brace
                brace_start = script_text.find('{', equals_idx)
                if brace_start == -1:
                    continue
                
                # Count braces to find the matching closing brace
                brace_count = 0
                i = brace_start
                while i < len(script_text):
                    if script_text[i] == '{':
                        brace_count += 1
                    elif script_text[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            # Found the matching closing brace
                            json_str = script_text[brace_start:i+1]
                            try:
                                store_data = json.loads(json_str)
                                return store_data
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON data: {e}")
                                # Try to fix common JSON issues
                                try:
                                    # Remove trailing commas before closing braces/brackets
                                    json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
                                    store_data = json.loads(json_str)
                                    return store_data
                                except json.JSONDecodeError:
                                    break
                    i += 1
            return None
        except Exception as e:
            logger.warning(f"Error extracting JSON data: {e}")
            return None
    
    def _validate_product(self, product: Dict) -> bool:
        """
        Validate that a product has minimum required fields
        
        Args:
            product: Product dictionary
            
        Returns:
            True if product is valid, False otherwise
        """
        if not product:
            return False
        
        # Must have product_id or name
        if not product.get('product_id') and not product.get('name'):
            logger.warning("Product missing both product_id and name, skipping")
            return False
        
        # Must have URL
        if not product.get('url'):
            logger.warning(f"Product missing URL: {product.get('name', 'Unknown')}, skipping")
            return False
        
        # Must have at least price or name
        if not product.get('price') and not product.get('name'):
            logger.warning("Product missing both price and name, skipping")
            return False
        
        return True
    
    def _parse_json_product(self, json_product: Dict) -> Dict:
        """
        Parse a product from JSON data structure
        
        Args:
            json_product: Product dictionary from JSON
            
        Returns:
            Formatted product dictionary
        """
        product = {}
        
        # Basic info
        product['product_id'] = json_product.get('sku')
        product['name'] = json_product.get('displayName') or json_product.get('name')
        product['brand'] = json_product.get('brand')
        
        # URL
        url_path = json_product.get('url', '')
        if url_path:
            product['url'] = urljoin(self.base_url, url_path)
        else:
            product['url'] = None
        
        # Prices
        prices = json_product.get('prices', {})
        if prices:
            price_text = prices.get('price', '')
            product['price_text'] = price_text if price_text else None
            product['price'] = self.extract_price(price_text) if price_text else None
            
            # Raw price (numeric)
            raw_price = prices.get('rawPrice', '')
            product['raw_price'] = self.extract_price(raw_price) if raw_price else None
            
            old_price_text = prices.get('oldPrice', '')
            product['old_price_text'] = old_price_text if old_price_text else None
            product['old_price'] = self.extract_price(old_price_text) if old_price_text else None
            
            discount_text = prices.get('discount', '')
            product['discount_text'] = discount_text if discount_text else None
            product['discount'] = self.extract_discount(discount_text) if discount_text else None
            
            # Euro prices
            product['price_euro'] = self.extract_price(prices.get('priceEuro', '')) if prices.get('priceEuro') else None
            product['old_price_euro'] = self.extract_price(prices.get('oldPriceEuro', '')) if prices.get('oldPriceEuro') else None
            product['discount_euro'] = self.extract_price(prices.get('discountEuro', '')) if prices.get('discountEuro') else None
        else:
            product['price_text'] = None
            product['price'] = None
            product['raw_price'] = None
            product['old_price_text'] = None
            product['old_price'] = None
            product['discount_text'] = None
            product['discount'] = None
            product['price_euro'] = None
            product['old_price_euro'] = None
            product['discount_euro'] = None
        
        # Rating
        rating_data = json_product.get('rating', {})
        if rating_data:
            rating_avg = rating_data.get('average')
            # Handle 0 rating (might mean no ratings yet)
            product['rating'] = rating_avg if rating_avg and rating_avg > 0 else None
            product['review_count'] = rating_data.get('totalRatings') or None
        else:
            product['rating'] = None
            product['review_count'] = None
        
        # Image
        product['image_url'] = json_product.get('image')
        product['image_alt'] = json_product.get('imageAlt')
        
        # Categories
        categories = json_product.get('categories', [])
        product['categories'] = categories if categories else None
        product['category'] = categories[0] if categories else None
        
        # Tags
        product['tags'] = json_product.get('tags')
        
        # Badges and flags
        badges = json_product.get('badges', {})
        if badges and 'main' in badges:
            main_badge = badges.get('main', {})
            product['is_official_store'] = main_badge.get('identifier') == 'JMALL'
            product['official_store_name'] = main_badge.get('name')
        else:
            product['is_official_store'] = False
            product['official_store_name'] = None
        
        # Campaign badges
        if badges and 'campaign' in badges:
            campaign_badge = badges.get('campaign', {})
            product['campaign_name'] = campaign_badge.get('name')
            product['campaign_identifier'] = campaign_badge.get('identifier')
        else:
            product['campaign_name'] = None
            product['campaign_identifier'] = None
        
        # Express delivery - check multiple sources
        product['express_delivery'] = (
            json_product.get('isShopExpress', False) or 
            'shopExpress' in json_product
        )
        
        # Tracking info
        tracking = json_product.get('tracking', {})
        if tracking:
            product['category_key'] = tracking.get('categoryKey')
            product['brand_key'] = tracking.get('brandKey')
            product['is_second_chance'] = tracking.get('isSecondChance', False)
        else:
            product['category_key'] = None
            product['brand_key'] = None
            product['is_second_chance'] = False
        
        # Other flags
        product['is_sponsored'] = json_product.get('isSponsored', False)
        product['is_buyable'] = json_product.get('isBuyable', True)
        
        # Seller info
        product['seller_id'] = json_product.get('sellerId')
        
        # Timestamp
        product['scraped_at'] = datetime.now().isoformat()
        
        # Source
        product['source'] = 'jumia.ma'
        
        return product
    
    def scrape_category_page(self, category_url: str, max_pages: int = 1) -> List[Dict]:
        """
        Scrape products from a category page
        
        Args:
            category_url: URL of the category page
            max_pages: Maximum number of pages to scrape (default: 1)
            
        Returns:
            List of product dictionaries
        """
        all_products = []
        
        for page in range(1, max_pages + 1):
            # Construct URL with page parameter
            if page == 1:
                url = category_url
            else:
                # Add page parameter
                separator = '&' if '?' in category_url else '?'
                url = f"{category_url}{separator}page={page}#catalog-listing"
            
            logger.info(f"Scraping page {page}...")
            soup = self.get_page(url)
            
            if not soup:
                logger.warning(f"Could not fetch page {page}")
                continue
            
            # Track products before this page
            products_before_page = len(all_products)
            
            # Try to extract products from JSON first (more reliable)
            store_data = self._extract_json_data(soup)
            if store_data and 'products' in store_data:
                json_products = store_data['products']
                if json_products:
                    logger.info(f"Found {len(json_products)} products in JSON data on page {page}")
                    
                    # Parse JSON products
                    for json_product in json_products:
                        product = self._parse_json_product(json_product)
                        if product:
                            all_products.append(product)
                else:
                    logger.info(f"No products found in JSON data on page {page}")
            else:
                # Fallback to HTML parsing
                logger.info("JSON data not found, falling back to HTML parsing...")
                
                # Find all product articles (including sponsored products)
                # Products have class 'prd' and may have additional classes like '_fb', '_spn', 'col', 'c-prd'
                all_articles = soup.find_all('article')
                product_articles = [
                    article for article in all_articles 
                    if article.get('class') and 'prd' in article.get('class', [])
                ]
                
                if not product_articles:
                    logger.warning(f"No products found on page {page}")
                    break
                
                logger.info(f"Found {len(product_articles)} products in HTML on page {page}")
                
                # Parse each product from HTML
                for article in product_articles:
                    try:
                        product = self.parse_product_card(article)
                        if product and self._validate_product(product):
                            all_products.append(product)
                        elif product:
                            logger.warning(f"Skipping invalid product from HTML: {product.get('product_id', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Error parsing product card: {e}", exc_info=True)
                        continue
            
            # Check if there's a next page
            if page < max_pages:
                # Look for next page link
                next_page = None
                # Try finding next page button/link
                pg_links = soup.find_all('a', class_='pg')
                for pg_link in pg_links:
                    href = pg_link.get('href', '')
                    aria_label = pg_link.get('aria-label', '')
                    if f'page={page+1}' in href or 'suivante' in aria_label.lower() or 'next' in aria_label.lower():
                        next_page = pg_link
                        break
                
                if not next_page:
                    logger.info(f"No more pages available. Stopped at page {page}")
                    break
        
        logger.info(f"Total products scraped: {len(all_products)}")
        return all_products
    
    def _get_last_page_number(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract the last page number from pagination HTML
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Last page number or None if not found
        """
        try:
            # Look for pagination div
            pg_w = soup.find('div', class_='pg-w')
            if not pg_w:
                return None
            
            # Find the "Dernière page" (last page) link
            last_page_link = pg_w.find('a', {'aria-label': 'Dernière page'})
            if last_page_link:
                href = last_page_link.get('href', '')
                # Extract page number from URL like "/category/?page=50#catalog-listing"
                match = re.search(r'page=(\d+)', href)
                if match:
                    return int(match.group(1))
            
            # Alternative: find all page links and get the highest number
            pg_links = pg_w.find_all('a', class_='pg')
            max_page = 1
            for link in pg_links:
                href = link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
            
            # Also check for active page span
            active_page = pg_w.find('span', class_='pg _act')
            if active_page:
                try:
                    page_num = int(active_page.get_text(strip=True))
                    max_page = max(max_page, page_num)
                except ValueError:
                    pass
            
            return max_page if max_page > 1 else None
        except Exception as e:
            logger.warning(f"Error extracting last page number: {e}")
            return None
    
    def scrape_category(self, category: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape a category with automatic page detection
        
        Args:
            category: Category slug (e.g., 'telephone-tablette')
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages
            
        Returns:
            List of product dictionaries
        """
        category_url = f"{self.base_url}/{category}/"
        
        # Get first page to determine total pages
        logger.info(f"Fetching first page of category '{category}' to determine total pages...")
        soup = self.get_page(category_url)
        
        if not soup:
            logger.error(f"Could not fetch category: {category}")
            return []
        
        # Determine total pages
        if max_pages is None:
            last_page = self._get_last_page_number(soup)
            if last_page:
                total_pages = last_page
                logger.info(f"Found {total_pages} pages for category '{category}'")
            else:
                # If we can't determine, use a large number and let it stop when no more products
                # The scrape_category_page will stop when no products are found
                total_pages = 1000  # Large number, will stop when no more pages
                logger.info(f"Could not determine total pages for '{category}', will scrape until no more products found")
        else:
            total_pages = max_pages
            logger.info(f"Scraping {total_pages} pages for category '{category}'")
        
        # Scrape all pages
        return self.scrape_category_page(category_url, max_pages=total_pages)
    
    def scrape_telephone_tablette(self, max_pages: int = 1) -> List[Dict]:
        """
        Scrape the Téléphone & Tablette category
        
        Args:
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of product dictionaries
        """
        return self.scrape_category('telephone-tablette', max_pages=max_pages)
    
    def scrape_all_categories(self, max_pages_per_category: Optional[int] = None) -> Dict[str, List[Dict]]:
        """
        Scrape all main categories from Jumia.ma
        
        Args:
            max_pages_per_category: Maximum pages per category. If None, scrapes all pages
            
        Returns:
            Dictionary mapping category names to product lists
        """
        categories = [
            'telephone-tablette',
            'electronique',
            'ordinateurs-accessoires-informatique',
            'maison-cuisine-jardin',
            'fashion-mode',
            'beaute-hygiene-sante',
            'jeux-videos-consoles',
            'epicerie',
            'sports-loisirs',
            'bebe-puericulture'
        ]
        
        all_results = {}
        
        for category in categories:
            logger.info(f"\n{'='*60}")
            logger.info(f"Scraping category: {category}")
            logger.info(f"{'='*60}")
            
            try:
                products = self.scrape_category(category, max_pages=max_pages_per_category)
                all_results[category] = products
                logger.info(f"Scraped {len(products)} products from category '{category}'")
            except Exception as e:
                logger.error(f"Error scraping category '{category}': {e}", exc_info=True)
                all_results[category] = []
        
        total_products = sum(len(products) for products in all_results.values())
        logger.info(f"\n{'='*60}")
        logger.info(f"TOTAL: Scraped {total_products} products across {len(categories)} categories")
        logger.info(f"{'='*60}")
        
        return all_results


if __name__ == "__main__":
    # Example usage
    scraper = JumiaScraper()
    products = scraper.scrape_telephone_tablette(max_pages=1)
    
    print(f"\nScraped {len(products)} products")
    if products:
        print("\nFirst product:")
        print(products[0])
