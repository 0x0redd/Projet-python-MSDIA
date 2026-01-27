"""
Main script for web scraping
Scrapes Jumia.ma and saves data to CSV/Parquet
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraping.jumia.jumia_scraper import JumiaScraper

# Ensure logs directory exists before configuring logging
Path('logs').mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def ensure_directories():
    """Ensure necessary directories exist"""
    directories = ['data/raw', 'data/processed', 'logs', 'outputs']
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    logger.info("Directories ensured")


def save_data(products: list, format: str = 'both'):
    """
    Save scraped data to file(s)
    
    Args:
        products: List of product dictionaries
        format: 'csv', 'parquet', or 'both'
    """
    if not products:
        logger.warning("No products to save")
        return
    
    df = pd.DataFrame(products)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format in ['csv', 'both']:
        csv_path = f"scraping/data/raw/jumia_products_{timestamp}.csv"
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to {csv_path}")
    
    if format in ['parquet', 'both']:
        parquet_path = f"scraping/data/raw/jumia_products_{timestamp}.parquet"
        Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Data saved to {parquet_path}")
    
    logger.info(f"Total products saved: {len(products)}")
    logger.info(f"Columns: {list(df.columns)}")


def save_all_categories_data(all_results: dict, format: str = 'both'):
    """
    Save data from all categories
    
    Args:
        all_results: Dictionary mapping category names to product lists
        format: 'csv', 'parquet', or 'both'
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save combined data
    all_products = []
    for category, products in all_results.items():
        all_products.extend(products)
    
    if all_products:
        save_data(all_products, format=format)
        logger.info(f"Combined data: {len(all_products)} products from {len(all_results)} categories")
    
    # Save per-category data
    for category, products in all_results.items():
        if products:
            df = pd.DataFrame(products)
            category_safe = category.replace('/', '_')
            
            if format in ['csv', 'both']:
                csv_path = f"scraping/data/raw/jumia_{category_safe}_{timestamp}.csv"
                Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Category '{category}': {len(products)} products saved to {csv_path}")
            
            if format in ['parquet', 'both']:
                parquet_path = f"scraping/data/raw/jumia_{category_safe}_{timestamp}.parquet"
                Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(parquet_path, index=False)
                logger.info(f"Category '{category}': {len(products)} products saved to {parquet_path}")


def main():
    """Main scraping function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape products from Jumia.ma')
    parser.add_argument('--category', type=str, help='Specific category to scrape (e.g., telephone-tablette)')
    parser.add_argument('--all', action='store_true', help='Scrape all categories')
    parser.add_argument('--max-pages', type=int, default=None, help='Maximum pages per category (default: all pages)')
    args = parser.parse_args()
    
    logger.info("Starting Jumia.ma scraper...")
    
    # Ensure directories exist
    ensure_directories()
    
    # Initialize scraper
    scraper = JumiaScraper(delay=1.0)  # 1 second delay between requests
    
    # Scrape products
    try:
        if args.all:
            # Scrape all categories
            logger.info("Scraping ALL categories...")
            all_results = scraper.scrape_all_categories(max_pages_per_category=args.max_pages)
            
            # Save all data
            save_all_categories_data(all_results, format='both')
            
            # Print summary
            print("\n" + "="*60)
            print("SCRAPING SUMMARY - ALL CATEGORIES")
            print("="*60)
            total_products = sum(len(products) for products in all_results.values())
            print(f"Total products scraped: {total_products}")
            print(f"\nProducts per category:")
            for category, products in all_results.items():
                print(f"  {category}: {len(products)} products")
            
            # Show sample products
            print(f"\nSample products (first 3 from first category):")
            first_category = next(iter(all_results.keys()))
            for i, product in enumerate(all_results[first_category][:3], 1):
                print(f"\n{i}. {product.get('name', 'N/A')}")
                print(f"   Price: {product.get('price_text', 'N/A')}")
                print(f"   Discount: {product.get('discount_text', 'N/A')}")
                print(f"   Rating: {product.get('rating', 'N/A')} ({product.get('review_count', 'N/A')} reviews)")
                print(f"   Category: {product.get('category', 'N/A')}")
                print(f"   URL: {product.get('url', 'N/A')}")
        
        elif args.category:
            # Scrape specific category
            logger.info(f"Scraping category: {args.category}")
            products = scraper.scrape_category(args.category, max_pages=args.max_pages)
            
            if products:
                # Save data
                save_data(products, format='both')
                
                # Print summary
                print("\n" + "="*50)
                print("SCRAPING SUMMARY")
                print("="*50)
                print(f"Category: {args.category}")
                print(f"Total products scraped: {len(products)}")
                print(f"\nSample products:")
                for i, product in enumerate(products[:3], 1):
                    print(f"\n{i}. {product.get('name', 'N/A')}")
                    print(f"   Price: {product.get('price_text', 'N/A')}")
                    print(f"   Discount: {product.get('discount_text', 'N/A')}")
                    print(f"   Rating: {product.get('rating', 'N/A')} ({product.get('review_count', 'N/A')} reviews)")
                    print(f"   URL: {product.get('url', 'N/A')}")
            else:
                logger.warning("No products were scraped")
        
        else:
            # Default: scrape telephone-tablette category (1 page for quick test)
            logger.info("Scraping default category: telephone-tablette (1 page)")
            products = scraper.scrape_telephone_tablette(max_pages=1)
            
            if products:
                # Save data
                save_data(products, format='both')
                
                # Print summary
                print("\n" + "="*50)
                print("SCRAPING SUMMARY")
                print("="*50)
                print(f"Total products scraped: {len(products)}")
                print(f"\nSample products:")
                for i, product in enumerate(products[:3], 1):
                    print(f"\n{i}. {product.get('name', 'N/A')}")
                    print(f"   Price: {product.get('price_text', 'N/A')}")
                    print(f"   Discount: {product.get('discount_text', 'N/A')}")
                    print(f"   Rating: {product.get('rating', 'N/A')} ({product.get('review_count', 'N/A')} reviews)")
                    print(f"   URL: {product.get('url', 'N/A')}")
            else:
                logger.warning("No products were scraped")
            
    except Exception as e:
        logger.error(f"Error during scraping: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
