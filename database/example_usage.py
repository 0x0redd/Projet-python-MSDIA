"""
Example usage of the database manager with MongoDB
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager
from scraping.jumia_scraper import JumiaScraper
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_save_products():
    """Example: Scrape and save products to database"""
    # Initialize database with MongoDB (loads from .env file)
    db = DatabaseManager()
    
    # Initialize scraper
    scraper = JumiaScraper(delay=1.0)
    
    # Scrape a category (1 page for example)
    logger.info("Scraping products...")
    products = scraper.scrape_category('telephone-tablette', max_pages=1)
    
    # Save to database
    logger.info(f"Saving {len(products)} products to database...")
    stats = db.save_products(products, detect_price_changes=True)
    
    print("\n" + "="*50)
    print("SAVE STATISTICS")
    print("="*50)
    print(f"New products: {stats['new_products']}")
    print(f"Updated products: {stats['updated_products']}")
    print(f"New price records: {stats['new_price_records']}")
    print(f"Price changes detected: {stats['price_changes_detected']}")
    
    db.close()


def example_query_price_history():
    """Example: Query price history for a product"""
    db = DatabaseManager()
    
    # Get a product ID (you would get this from your database)
    # For example, get first product
    first_product = db.db.products.find_one()
    if first_product:
        product_id = first_product['product_id']
        print(f"\nPrice history for: {first_product['name']}")
        print("="*50)
        
        # Get price history
        history = db.get_product_price_history(product_id, limit=10)
        for record in history:
            print(f"{record['scraped_at']}: {record['price_text']} "
                  f"(Rating: {record['rating']}, Reviews: {record['review_count']})")
    else:
        print("No products in database yet")
    
    db.close()


def example_get_price_drops():
    """Example: Find products with significant price drops"""
    db = DatabaseManager()
    
    # Get products with price drops > 10% in last 7 days
    price_drops = db.get_products_with_price_drops(min_percentage=10.0, days=7)
    
    print("\n" + "="*50)
    print(f"PRODUCTS WITH PRICE DROPS (>10% in last 7 days)")
    print("="*50)
    
    if price_drops:
        for drop in price_drops[:10]:  # Show first 10
            print(f"\n{drop['product_name']}")
            print(f"  Brand: {drop['brand']}")
            print(f"  Previous: {drop['previous_price']} Dhs")
            print(f"  Current: {drop['current_price']} Dhs")
            print(f"  Drop: {drop['price_drop']:.2f} Dhs ({drop['percentage_drop']:.1f}%)")
            print(f"  Changed: {drop['changed_at']}")
    else:
        print("No significant price drops found")
    
    db.close()


def example_get_price_changes():
    """Example: Get all price changes"""
    db = DatabaseManager()
    
    # Get price changes in last 24 hours
    since = datetime.utcnow() - timedelta(days=1)
    changes = db.get_price_changes(since=since, min_percentage=5.0)
    
    print("\n" + "="*50)
    print(f"PRICE CHANGES (last 24 hours, >5%)")
    print("="*50)
    
    if changes:
        for change in changes[:20]:  # Show first 20
            print(f"\nProduct: {change['product_id']}")
            print(f"  Type: {change['change_type']}")
            print(f"  {change['previous_price']} → {change['current_price']} Dhs")
            print(f"  Change: {change['price_difference']:.2f} Dhs ({change['percentage_change']:.1f}%)")
            print(f"  Time: {change['changed_at']}")
    else:
        print("No price changes found")
    
    db.close()


def example_get_current_prices():
    """Example: Get current prices for all products"""
    db = DatabaseManager()
    
    # Get current prices (optionally filter by category or brand)
    current_prices = db.get_current_prices(category="Phones & Tablets")
    
    print("\n" + "="*50)
    print(f"CURRENT PRICES (Category: Phones & Tablets)")
    print("="*50)
    print(f"Total products: {len(current_prices)}")
    
    for product in current_prices[:10]:  # Show first 10
        print(f"\n{product['name']}")
        print(f"  Price: {product.get('price_text', 'N/A')}")
        print(f"  Rating: {product.get('rating', 'N/A')}")
        print(f"  URL: {product['url']}")
    
    db.close()


def example_statistics():
    """Example: Get database statistics"""
    db = DatabaseManager()
    
    stats = db.get_statistics()
    
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    db.close()


def example_create_alert():
    """Example: Create an alert for a price drop"""
    db = DatabaseManager()
    
    # Get a product
    first_product = db.db.products.find_one()
    if first_product:
        # Create alert
        db.create_alert(
            product_id=first_product['product_id'],
            alert_type='price_drop',
            message=f"Price dropped for {first_product['name']}",
            price_value=100.0,
            threshold_value=10.0
        )
        print(f"Alert created for {first_product['name']}")
    
    db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database usage examples')
    parser.add_argument('example', choices=[
        'save', 'history', 'drops', 'changes', 'prices', 'stats', 'alert'
    ], help='Example to run')
    
    args = parser.parse_args()
    
    if args.example == 'save':
        example_save_products()
    elif args.example == 'history':
        example_query_price_history()
    elif args.example == 'drops':
        example_get_price_drops()
    elif args.example == 'changes':
        example_get_price_changes()
    elif args.example == 'prices':
        example_get_current_prices()
    elif args.example == 'stats':
        example_statistics()
    elif args.example == 'alert':
        example_create_alert()
