"""
Main script for testing Marjanemall.ma scraper
Scrapes products and saves data to CSV/Parquet
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scraping.marjanemall.marjanemall_scraper import MarjanemallScraper
from scraping.marjanemall.marjanemall_api_scraper import MarjanemallAPIScraper

# Ensure logs directory exists before configuring logging
Path('logs').mkdir(parents=True, exist_ok=True)
Path('scraping/data/raw').mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraping_marjanemall.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def save_data(products: list, format: str = 'both', prefix: str = 'marjanemall'):
    """
    Save scraped data to file(s)
    
    Args:
        products: List of product dictionaries
        format: 'csv', 'parquet', or 'both'
        prefix: Prefix for filename
    """
    if not products:
        logger.warning("No products to save")
        return
    
    df = pd.DataFrame(products)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format in ['csv', 'both']:
        csv_path = f"scraping/data/raw/{prefix}_products_{timestamp}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to {csv_path}")
    
    if format in ['parquet', 'both']:
        parquet_path = f"scraping/data/raw/{prefix}_products_{timestamp}.parquet"
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
        save_data(all_products, format=format, prefix='marjanemall_all')
        logger.info(f"Combined data: {len(all_products)} products from {len(all_results)} categories")
    
    # Save per-category data
    for category, products in all_results.items():
        if products:
            df = pd.DataFrame(products)
            category_safe = category.replace('/', '_')
            
            if format in ['csv', 'both']:
                csv_path = f"scraping/data/raw/marjanemall_{category_safe}_{timestamp}.csv"
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Category '{category}': {len(products)} products saved to {csv_path}")
            
            if format in ['parquet', 'both']:
                parquet_path = f"scraping/data/raw/marjanemall_{category_safe}_{timestamp}.parquet"
                df.to_parquet(parquet_path, index=False)
                logger.info(f"Category '{category}': {len(products)} products saved to {parquet_path}")


def main():
    """Main scraping function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape products from Marjanemall.ma')
    parser.add_argument('--category', type=str, help='Specific category to scrape (e.g., telephone-objets-connectes)')
    parser.add_argument('--all', action='store_true', help='Scrape all categories')
    parser.add_argument('--max-pages', type=int, default=10, help='Maximum scroll batches per category (default: 10)')
    parser.add_argument('--delay', type=float, default=3.0, help='Delay between requests in seconds (default: 3.0)')
    parser.add_argument('--format', type=str, default='both', choices=['csv', 'parquet', 'both'],
                       help='Output format (default: both)')
    parser.add_argument('--use-playwright', action='store_true', 
                       help='Use Playwright scraper instead of requests-based scraper (requires playwright)')
    parser.add_argument('--use-api', action='store_true',
                       help='Use real API scraper (appli.marjanemall.ma) instead of HTML parsing')
    args = parser.parse_args()
    
    logger.info("Starting Marjanemall.ma scraper...")
    
    # Initialize scraper
    # Option 1: Real API scraper (recommended - clean structured data)
    if args.use_api:
        scraper = MarjanemallAPIScraper(delay=args.delay)
        logger.info(f"Using real API scraper with {args.delay}s delay")
    # Option 2: Requests-based scraper with scroll simulation (default)
    elif not args.use_playwright:
        scraper = MarjanemallScraper(delay=args.delay)
        logger.info(f"Using requests-based scraper with {args.delay}s delay")
    else:
        # Option 3: Playwright scraper with real scrolling (if available)
        try:
            from scraping.marjanemall.marjanemall_playwright import MarjanemallPlaywrightScraper
            scraper = MarjanemallPlaywrightScraper(headless=True, scroll_delay=args.delay)
            logger.info(f"Using Playwright scraper with {args.delay}s scroll delay")
        except ImportError:
            logger.error("Playwright scraper not available. Install playwright: pip install playwright")
            logger.info("Falling back to requests-based scraper")
            scraper = MarjanemallScraper(delay=args.delay)
    
    # Scrape products
    try:
        if args.all:
            # Scrape all categories
            logger.info("Scraping ALL categories...")
            # Handle different scraper interfaces
            if args.use_api:
                all_results = scraper.scrape_all_categories(max_pages_per_category=args.max_pages)
            else:
                all_results = scraper.scrape_all_categories(max_scroll_batches=args.max_pages)
            
            # Save all data
            save_all_categories_data(all_results, format=args.format)
            
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
            if all_results:
                first_category = next(iter(all_results.keys()))
                if all_results[first_category]:
                    print(f"\nSample products (first 3 from '{first_category}'):")
                    for i, product in enumerate(all_results[first_category][:3], 1):
                        print(f"\n{i}. {product.get('product_name', 'N/A')}")
                        print(f"   Price: {product.get('current_price_text', 'N/A')}")
                        print(f"   Original Price: {product.get('original_price', 'N/A')}")
                        print(f"   Discount: {product.get('discount_percentage', 'N/A')}")
                        print(f"   Brand: {product.get('brand', 'N/A')}")
                        print(f"   Seller: {product.get('seller', 'N/A')}")
                        print(f"   Fast Delivery: {product.get('fast_delivery', False)}")
                        print(f"   URL: {product.get('full_url', 'N/A')}")
        
        elif args.category:
            # Scrape specific category
            logger.info(f"Scraping category: {args.category}")
            # Handle different scraper interfaces
            if args.use_api:
                products = scraper.scrape_category(args.category, max_pages=args.max_pages)
            else:
                products = scraper.scrape_category(args.category, max_scroll_batches=args.max_pages)
            
            if products:
                # Save data
                save_data(products, format=args.format, prefix=f'marjanemall_{args.category}')
                
                # Print summary
                print("\n" + "="*50)
                print("SCRAPING SUMMARY")
                print("="*50)
                print(f"Category: {args.category}")
                print(f"Total products scraped: {len(products)}")
                print(f"\nSample products:")
                for i, product in enumerate(products[:3], 1):
                    print(f"\n{i}. {product.get('product_name', 'N/A')}")
                    print(f"   Price: {product.get('current_price_text', 'N/A')}")
                    print(f"   Original Price: {product.get('original_price', 'N/A')}")
                    print(f"   Discount: {product.get('discount_percentage', 'N/A')}")
                    print(f"   Brand: {product.get('brand', 'N/A')}")
                    print(f"   Seller: {product.get('seller', 'N/A')}")
                    print(f"   Fast Delivery: {product.get('fast_delivery', False)}")
                    print(f"   Product ID: {product.get('product_id', 'N/A')}")
                    print(f"   URL: {product.get('full_url', 'N/A')}")
            else:
                logger.warning("No products were scraped")
        
        else:
            # Default: scrape telephone-objets-connectes category (1 page/batch for quick test)
            logger.info("Scraping default category: telephone-objets-connectes (1 page)")
            # Handle different scraper interfaces
            if args.use_api:
                products = scraper.scrape_category('telephone-objets-connectes', max_pages=1)
            else:
                products = scraper.scrape_category('telephone-objets-connectes', max_scroll_batches=1)
            
            if products:
                # Save data
                save_data(products, format=args.format)
                
                # Print summary
                print("\n" + "="*50)
                print("SCRAPING SUMMARY")
                print("="*50)
                print(f"Total products scraped: {len(products)}")
                print(f"\nSample products:")
                for i, product in enumerate(products[:3], 1):
                    print(f"\n{i}. {product.get('product_name', 'N/A')}")
                    print(f"   Price: {product.get('current_price_text', 'N/A')}")
                    print(f"   Original Price: {product.get('original_price', 'N/A')}")
                    print(f"   Discount: {product.get('discount_percentage', 'N/A')}")
                    print(f"   Brand: {product.get('brand', 'N/A')}")
                    print(f"   Seller: {product.get('seller', 'N/A')}")
                    print(f"   Fast Delivery: {product.get('fast_delivery', False)}")
                    print(f"   Product ID: {product.get('product_id', 'N/A')}")
                    print(f"   URL: {product.get('full_url', 'N/A')}")
            else:
                logger.warning("No products were scraped")
            
    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during scraping: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
