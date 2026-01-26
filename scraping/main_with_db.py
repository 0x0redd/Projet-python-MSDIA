"""
Main script for web scraping with database integration
Scrapes Jumia.ma and saves data to both CSV/Parquet and database
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
import argparse

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraping.jumia.jumia_scraper import JumiaScraper
from database.db_manager import DatabaseManager

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
    directories = ['data/raw', 'data/processed', 'logs', 'outputs', 'data']
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
        csv_path = f"data/raw/jumia_products_{timestamp}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to {csv_path}")
    
    if format in ['parquet', 'both']:
        parquet_path = f"data/raw/jumia_products_{timestamp}.parquet"
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
                csv_path = f"data/raw/jumia_{category_safe}_{timestamp}.csv"
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Category '{category}': {len(products)} products saved to {csv_path}")
            
            if format in ['parquet', 'both']:
                parquet_path = f"data/raw/jumia_{category_safe}_{timestamp}.parquet"
                df.to_parquet(parquet_path, index=False)
                logger.info(f"Category '{category}': {len(products)} products saved to {parquet_path}")


def main():
    """Main scraping function with database support"""
    parser = argparse.ArgumentParser(description='Scrape products from Jumia.ma')
    parser.add_argument('--category', type=str, help='Specific category to scrape')
    parser.add_argument('--all', action='store_true', help='Scrape all categories')
    parser.add_argument('--max-pages', type=int, default=None, help='Maximum pages per category')
    parser.add_argument('--no-db', action='store_true', help='Skip database saving (only save to files)')
    parser.add_argument('--no-files', action='store_true', help='Skip file saving (only save to database)')
    parser.add_argument('--db-name', type=str, default=None, 
                       help='MongoDB database name (default: from .env file)')
    parser.add_argument('--public-key', type=str, default=None,
                       help='MongoDB Atlas public key (default: from .env file)')
    parser.add_argument('--private-key', type=str, default=None,
                       help='MongoDB Atlas private key (default: from .env file)')
    parser.add_argument('--cluster-name', type=str, default=None,
                       help='MongoDB Atlas cluster name (default: from .env file)')
    parser.add_argument('--connection-string', type=str, default=None,
                       help='MongoDB connection string (default: from .env file)')
    args = parser.parse_args()
    
    logger.info("Starting Jumia.ma scraper...")
    
    # Ensure directories exist
    ensure_directories()
    
    # Initialize database (if not skipped)
    db = None
    if not args.no_db:
        try:
            # Use command-line args if provided, otherwise use .env file
            db = DatabaseManager(
                connection_string=args.connection_string,
                database_name=args.db_name,
                public_key=args.public_key,
                private_key=args.private_key,
                cluster_name=args.cluster_name,
                use_env=True  # Load from .env if args not provided
            )
            logger.info("Database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            logger.info("Continuing without database...")
            db = None
    
    # Initialize scraper
    scraper = JumiaScraper(delay=1.0)
    
    try:
        if args.all:
            # Scrape all categories
            logger.info("Scraping ALL categories...")
            all_results = scraper.scrape_all_categories(max_pages_per_category=args.max_pages)
            
            # Save all data
            if not args.no_files:
                save_all_categories_data(all_results, format='both')
            
            # Save to database
            if db:
                total_products = sum(len(products) for products in all_results.values())
                logger.info(f"Saving {total_products} products to database...")
                
                all_products = []
                for category, products in all_results.items():
                    all_products.extend(products)
                
                if all_products:
                    db_stats = db.save_products(all_products, detect_price_changes=True)
                    logger.info(f"Database stats: {db_stats}")
                    
                    # Get overall statistics
                    overall_stats = db.get_statistics()
                    logger.info(f"Database statistics: {overall_stats}")
            
            # Print summary
            print("\n" + "="*60)
            print("SCRAPING SUMMARY - ALL CATEGORIES")
            print("="*60)
            total_products = sum(len(products) for products in all_results.values())
            print(f"Total products scraped: {total_products}")
            print(f"\nProducts per category:")
            for category, products in all_results.items():
                print(f"  {category}: {len(products)} products")
        
        elif args.category:
            # Scrape specific category
            logger.info(f"Scraping category: {args.category}")
            products = scraper.scrape_category(args.category, max_pages=args.max_pages)
            
            if products:
                # Save to files
                if not args.no_files:
                    save_data(products, format='both')
                
                # Save to database
                if db:
                    logger.info(f"Saving {len(products)} products to database...")
                    db_stats = db.save_products(products, detect_price_changes=True)
                    logger.info(f"Database stats: {db_stats}")
                    
                    # Show price changes if any
                    if db_stats['price_changes_detected'] > 0:
                        logger.info(f"Detected {db_stats['price_changes_detected']} price changes")
                
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
                # Save to files
                if not args.no_files:
                    save_data(products, format='both')
                
                # Save to database
                if db:
                    logger.info(f"Saving {len(products)} products to database...")
                    db_stats = db.save_products(products, detect_price_changes=True)
                    logger.info(f"Database stats: {db_stats}")
                
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
    finally:
        # Close database connection
        if db:
            db.close()


if __name__ == "__main__":
    main()
