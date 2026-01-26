"""
Marjanemall API scraper using the actual API endpoints
Uses appli.marjanemall.ma API for clean, structured data
"""

import requests
import json
import time
import re
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlencode
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class MarjanemallAPIScraper:
    """Scraper that uses the actual Marjane Mall API endpoints"""
    
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
    
    def __init__(self, api_base_url: str = "https://appli.marjanemall.ma", delay: float = 1.5):
        """
        Initialize the API scraper
        
        Args:
            api_base_url: Base URL for the API (default: appli.marjanemall.ma)
            delay: Delay between requests in seconds
        """
        self.api_base_url = api_base_url.rstrip('/')
        self.delay = delay
        self.session = requests.Session()
        
        # Headers that mimic the actual website requests
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Origin': 'https://www.marjanemall.ma',
            'Referer': 'https://www.marjanemall.ma/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'DNT': '1',
        })
        
        # Try to get initial cookies by visiting the main page
        try:
            self.session.get('https://www.marjanemall.ma', timeout=10)
            time.sleep(0.5)
        except:
            pass
    
    def get_menu(self) -> Optional[Dict]:
        """
        Get the menu/dynamic categories from the API
        
        Returns:
            Menu data or None
        """
        url = f"{self.api_base_url}/api/appmobilemm/v10/menu/dyanmic"
        logger.info(f"Fetching menu from: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Got menu data")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching menu: {e}")
            return None
    
    def get_category_products(self, category_slug: str, page: int = 1, page_size: int = 60) -> Optional[Dict]:
        """
        Get products for a category from the API
        
        Args:
            category_slug: Category slug/URL key
            page: Page number
            page_size: Products per page
            
        Returns:
            API response or None
        """
        # Try different API endpoint patterns
        endpoints_to_try = [
            # Pattern 1: Direct category endpoint
            f"{self.api_base_url}/api/appmobilemm/v19/data/{category_slug}?page={page}",
            
            # Pattern 2: With page size
            f"{self.api_base_url}/api/appmobilemm/v19/data/{category_slug}?page={page}&pageSize={page_size}",
            
            # Pattern 3: Alternative endpoint format
            f"{self.api_base_url}/api/appmobilemm/v16/category/path.json?category={category_slug}&page={page}",
        ]
        
        for url in endpoints_to_try:
            try:
                logger.info(f"Trying API endpoint: {url}")
                time.sleep(self.delay)
                
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check if data looks valid
                    if self.is_valid_product_data(data):
                        logger.info(f"Success with endpoint: {url}")
                        return data
                    else:
                        logger.debug(f"Endpoint returned data but structure doesn't look like products")
                
                elif response.status_code == 404:
                    logger.debug(f"Endpoint not found (404): {url}")
                    continue
                else:
                    logger.debug(f"Endpoint failed with status {response.status_code}: {url}")
                    
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request error with endpoint {url}: {e}")
                continue
            except json.JSONDecodeError:
                logger.debug(f"Invalid JSON response from {url}")
                continue
            except Exception as e:
                logger.debug(f"Error with endpoint {url}: {e}")
                continue
        
        # If none worked, try to discover the endpoint from HTML
        return self.discover_category_endpoint(category_slug, page)
    
    def discover_category_endpoint(self, category_slug: str, page: int = 1) -> Optional[Dict]:
        """
        Try to discover the correct API endpoint by analyzing the webpage
        
        Args:
            category_slug: Category slug
            page: Page number
            
        Returns:
            API response or None
        """
        logger.info(f"Trying to discover API endpoint for {category_slug}")
        
        category_url = f"https://www.marjanemall.ma/{category_slug}?page={page}"
        
        try:
            response = requests.get(category_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }, timeout=30)
            
            if response.status_code == 200:
                html_content = response.text
                
                # Search for API patterns in HTML/JavaScript
                api_patterns = [
                    r'/api/appmobilemm/v\d+/[^"\'>\s]+',
                    r'https://appli\.marjanemall\.ma/api/[^"\'>\s]+',
                    r'fetch\(["\'][^"\']*api[^"\']*["\']',
                ]
                
                found_endpoints = []
                for pattern in api_patterns:
                    matches = re.findall(pattern, html_content)
                    found_endpoints.extend(matches)
                
                # Clean and try endpoints
                unique_endpoints = list(set(found_endpoints))[:5]  # Try first 5
                
                for endpoint in unique_endpoints:
                    # Clean up the endpoint
                    if endpoint.startswith('fetch('):
                        match = re.search(r'["\']([^"\']+)["\']', endpoint)
                        if match:
                            endpoint = match.group(1)
                    
                    # Make sure it's a full URL
                    if not endpoint.startswith('http'):
                        if endpoint.startswith('/'):
                            endpoint = f"{self.api_base_url}{endpoint}"
                        else:
                            endpoint = f"{self.api_base_url}/api/appmobilemm/{endpoint}"
                    
                    # Add parameters if not present
                    if '?' not in endpoint:
                        endpoint = f"{endpoint}?category={category_slug}&page={page}"
                    
                    try:
                        logger.info(f"Trying discovered endpoint: {endpoint}")
                        time.sleep(self.delay)
                        
                        api_response = self.session.get(endpoint, timeout=30)
                        if api_response.status_code == 200:
                            data = api_response.json()
                            if self.is_valid_product_data(data):
                                logger.info(f"Discovered working endpoint: {endpoint}")
                                return data
                    except:
                        continue
                        
        except Exception as e:
            logger.error(f"Error discovering endpoint: {e}")
        
        return None
    
    def is_valid_product_data(self, data: Any) -> bool:
        """
        Check if data looks like valid product data
        
        Args:
            data: API response data
            
        Returns:
            True if data looks like products, False otherwise
        """
        if not data:
            return False
        
        # Check for common product data structures
        if isinstance(data, dict):
            # Check for items/products arrays
            if 'items' in data and isinstance(data['items'], list) and len(data['items']) > 0:
                first_item = data['items'][0]
                return isinstance(first_item, dict) and any(key in first_item for key in ['id', 'sku', 'name', 'price', 'entity_id'])
            
            if 'products' in data and isinstance(data['products'], list) and len(data['products']) > 0:
                first_product = data['products'][0]
                return isinstance(first_product, dict) and any(key in first_product for key in ['id', 'sku', 'name', 'price', 'entity_id'])
            
            # Magento-style response
            if all(key in data for key in ['items', 'total_count', 'search_criteria']):
                return True
        
        elif isinstance(data, list) and len(data) > 0:
            first_item = data[0] if isinstance(data[0], dict) else {}
            return any(key in first_item for key in ['id', 'sku', 'name', 'price', 'entity_id'])
        
        return False
    
    def parse_products_from_api(self, api_data: Dict, category_slug: str, page: int) -> List[Dict]:
        """
        Parse products from API response
        
        Args:
            api_data: API response data
            category_slug: Category slug
            page: Page number
            
        Returns:
            List of product dictionaries
        """
        products = []
        
        if not api_data:
            return products
        
        # Try different response structures
        
        # Structure 1: Magento-style response with items
        if isinstance(api_data, dict) and 'items' in api_data:
            items = api_data['items']
            if isinstance(items, list):
                for item in items:
                    product = self.parse_product_item(item, category_slug, page)
                    if product:
                        products.append(product)
        
        # Structure 2: Direct products array
        elif isinstance(api_data, dict) and 'products' in api_data:
            products_list = api_data['products']
            if isinstance(products_list, list):
                for item in products_list:
                    product = self.parse_product_item(item, category_slug, page)
                    if product:
                        products.append(product)
        
        # Structure 3: Direct list of products
        elif isinstance(api_data, list):
            for item in api_data:
                product = self.parse_product_item(item, category_slug, page)
                if product:
                    products.append(product)
        
        # Structure 4: Other nested structures
        elif isinstance(api_data, dict):
            # Look for any array that might contain products
            for key, value in api_data.items():
                if isinstance(value, list) and value:
                    first_item = value[0] if isinstance(value[0], dict) else {}
                    if any(field in first_item for field in ['id', 'sku', 'name', 'price', 'entity_id']):
                        for item in value:
                            product = self.parse_product_item(item, category_slug, page)
                            if product:
                                products.append(product)
                        break
        
        logger.info(f"Parsed {len(products)} products from API response")
        return products
    
    def parse_product_item(self, item: Any, category_slug: str, page: int) -> Optional[Dict]:
        """Parse a single product item from API response"""
        try:
            if not isinstance(item, dict):
                return None
            
            product = {}
            
            # Extract identifiers
            product['product_id'] = str(item.get('id') or item.get('entity_id') or item.get('sku', ''))
            product['sku'] = item.get('sku') or product['product_id']
            
            # Extract name and brand
            product['product_name'] = item.get('name') or item.get('title') or item.get('label') or ''
            product['brand'] = item.get('brand') or item.get('manufacturer') or None
            
            # Extract price information
            price_info = item.get('price_info') or {}
            if isinstance(price_info, dict):
                product['current_price'] = price_info.get('final_price') or price_info.get('price')
                product['original_price'] = price_info.get('regular_price') or price_info.get('original_price')
            
            # Fallback to direct price fields
            if not product.get('current_price'):
                product['current_price'] = item.get('price') or item.get('final_price')
            
            if not product.get('original_price'):
                product['original_price'] = item.get('original_price') or item.get('regular_price')
            
            # Convert prices to float
            try:
                if product.get('current_price'):
                    product['current_price'] = float(product['current_price'])
                    product['current_price_text'] = f"{product['current_price']:.2f} DH"
            except (ValueError, TypeError):
                product['current_price'] = None
                product['current_price_text'] = None
            
            try:
                if product.get('original_price'):
                    product['original_price'] = float(product['original_price'])
                    product['original_price_text'] = f"{product['original_price']:.2f} DH"
            except (ValueError, TypeError):
                product['original_price'] = None
                product['original_price_text'] = None
            
            # Calculate discount
            if product.get('current_price') and product.get('original_price'):
                try:
                    current = float(product['current_price'])
                    original = float(product['original_price'])
                    if original > 0 and current < original:
                        discount_percent = ((original - current) / original) * 100
                        product['discount_percentage'] = f"-{int(discount_percent)}%"
                        product['discount_value'] = int(discount_percent)
                except (ValueError, TypeError):
                    product['discount_percentage'] = None
                    product['discount_value'] = None
            else:
                product['discount_percentage'] = None
                product['discount_value'] = None
            
            # Extract URLs
            url_key = item.get('url_key') or item.get('url_path')
            if url_key:
                product['product_url'] = f"/p/{url_key}" if not url_key.startswith('/') else url_key
                product['full_url'] = f"https://www.marjanemall.ma{product['product_url']}"
            else:
                # Try to construct from name/sku
                if product.get('product_name'):
                    url_slug = product['product_name'].lower().replace(' ', '-')[:50]
                    product['product_url'] = f"/p/{url_slug}-{product.get('sku', '')}"
                    product['full_url'] = f"https://www.marjanemall.ma{product['product_url']}"
                else:
                    product['product_url'] = None
                    product['full_url'] = None
            
            # Extract images
            media_gallery = item.get('media_gallery_entries') or item.get('media_gallery') or []
            if media_gallery and isinstance(media_gallery, list) and len(media_gallery) > 0:
                first_image = media_gallery[0]
                if isinstance(first_image, dict):
                    product['image_url'] = first_image.get('file') or first_image.get('url')
                else:
                    product['image_url'] = str(first_image)
            else:
                product['image_url'] = item.get('image') or item.get('thumbnail')
            
            # Extract seller/vendor
            product['seller'] = item.get('seller') or item.get('vendor') or 'MARJANEMALL'
            
            # Extract additional info
            product['description'] = item.get('description') or item.get('short_description')
            product['in_stock'] = item.get('stock_status') or item.get('is_in_stock', True)
            product['qty_available'] = item.get('qty') or item.get('quantity')
            
            # Ratings
            product['rating'] = item.get('rating_summary') or item.get('rating')
            product['review_count'] = item.get('review_count')
            
            # Fast delivery (check various fields)
            product['fast_delivery'] = item.get('express_delivery') or item.get('fast_delivery', False)
            
            # Add metadata
            product['category'] = category_slug
            product['page_number'] = page
            product['scraped_timestamp'] = datetime.now().isoformat() + 'Z'
            product['data_source'] = 'real_api'
            
            return product
            
        except Exception as e:
            logger.error(f"Error parsing product item: {e}", exc_info=True)
            return None
    
    def scrape_category(self, category_slug: str, max_pages: int = 20) -> List[Dict]:
        """
        Scrape a category using the real API
        
        Args:
            category_slug: Category slug
            max_pages: Maximum pages to scrape
            
        Returns:
            List of product dictionaries
        """
        all_products = []
        seen_product_ids = set()
        
        logger.info(f"Scraping category via real API: {category_slug}")
        
        for page in range(1, max_pages + 1):
            logger.info(f"Fetching page {page}/{max_pages}")
            
            api_data = self.get_category_products(category_slug, page)
            
            if not api_data:
                logger.info(f"No API data for page {page}, stopping")
                break
            
            # Parse products
            products = self.parse_products_from_api(api_data, category_slug, page)
            
            if not products:
                logger.info(f"No products parsed from page {page}, stopping")
                break
            
            # Filter duplicates
            new_products = []
            for product in products:
                product_id = product.get('product_id') or product.get('sku')
                if product_id and product_id not in seen_product_ids:
                    seen_product_ids.add(product_id)
                    new_products.append(product)
            
            if new_products:
                all_products.extend(new_products)
                logger.info(f"Page {page}: Added {len(new_products)} new products (total: {len(all_products)})")
            else:
                logger.info(f"No new products on page {page}, stopping")
                break
            
            # Delay between pages
            time.sleep(self.delay)
        
        logger.info(f"Category '{category_slug}': {len(all_products)} total products")
        return all_products
    
    def scrape_all_categories(self, category_slugs: List[str] = None, max_pages_per_category: int = 20) -> Dict[str, List[Dict]]:
        """
        Scrape all categories
        
        Args:
            category_slugs: List of category slugs to scrape (default: common categories)
            max_pages_per_category: Maximum pages per category
            
        Returns:
            Dictionary mapping category slugs to product lists
        """
        if category_slugs is None:
            category_slugs = self.CATEGORIES
        
        results = {}
        
        for category_slug in category_slugs:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting category: {category_slug}")
            logger.info(f"{'='*60}")
            
            try:
                products = self.scrape_category(category_slug, max_pages_per_category)
                results[category_slug] = products
                logger.info(f"Completed '{category_slug}': {len(products)} products")
                
                # Delay between categories
                time.sleep(self.delay * 2)
                
            except Exception as e:
                logger.error(f"Error scraping category '{category_slug}': {e}", exc_info=True)
                results[category_slug] = []
        
        # Summary
        total = sum(len(p) for p in results.values())
        logger.info(f"\n{'='*60}")
        logger.info("REAL API SCRAPING COMPLETED")
        logger.info(f"{'='*60}")
        logger.info(f"Total categories: {len(results)}")
        logger.info(f"Total products: {total}")
        
        for cat, prods in results.items():
            logger.info(f"  {cat}: {len(prods)} products")
        
        return results
