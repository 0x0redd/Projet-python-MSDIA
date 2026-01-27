"""
Main script for Marjanemall.ma scraper
Uses Playwright to scrape products and saves data to CSV/JSON
Supports scraping all categories or specific ones
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
import json

# Ensure marjanemall_scraper.py is importable
sys.path.insert(0, str(Path(__file__).parent))

from marjanemall_scraper import MarjanemallScraper

# Ensure output directories exist
Path('logs').mkdir(parents=True, exist_ok=True)
Path('data').mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/marjanemall_scraping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def save_to_csv(products: list, filename: str):
    """Save products to CSV file"""
    if not products:
        logger.warning("No products to save")
        return
    
    df = pd.DataFrame(products)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logger.info(f"💾 Saved {len(products)} products to {filename}")


def save_to_json(products: list, filename: str):
    """Save products to JSON file"""
    if not products:
        logger.warning("No products to save")
        return
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 Saved {len(products)} products to {filename}")


def save_category_results(category: str, products: list, timestamp: str, output_format: str = 'both'):
    """Save results for a single category"""
    if not products:
        return
    
    category_safe = category.replace('/', '_').replace(' ', '_')
    
    if output_format in ['csv', 'both']:
        csv_file = f"data/marjanemall_{category_safe}_{timestamp}.csv"
        save_to_csv(products, csv_file)
    
    if output_format in ['json', 'both']:
        json_file = f"data/marjanemall_{category_safe}_{timestamp}.json"
        save_to_json(products, json_file)


def save_all_results(all_results: dict, timestamp: str, output_format: str = 'both'):
    """Save combined results from all categories"""
    # Combine all products
    all_products = []
    for category, products in all_results.items():
        all_products.extend(products)
    
    if not all_products:
        logger.warning("No products to save")
        return
    
    # Save combined file
    if output_format in ['csv', 'both']:
        csv_file = f"data/marjanemall_ALL_CATEGORIES_{timestamp}.csv"
        save_to_csv(all_products, csv_file)
    
    if output_format in ['json', 'both']:
        json_file = f"data/marjanemall_ALL_CATEGORIES_{timestamp}.json"
        save_to_json(all_products, json_file)
    
    # Save individual category files
    for category, products in all_results.items():
        if products:
            save_category_results(category, products, timestamp, output_format)
    
    # Print summary
    print("\n" + "="*70)
    print("📊 SCRAPING SUMMARY")
    print("="*70)
    print(f"Total categories: {len(all_results)}")
    print(f"Total products: {len(all_products)}")
    print(f"\nProducts per category:")
    for category, products in sorted(all_results.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  • {category}: {len(products)} products")
    print("="*70)


def print_sample_products(products: list, category: str = None, num_samples: int = 3):
    """Print sample products"""
    if not products:
        return
    
    print("\n" + "="*70)
    if category:
        print(f"📦 SAMPLE PRODUCTS from '{category}'")
    else:
        print(f"📦 SAMPLE PRODUCTS")
    print("="*70)
    
    for i, product in enumerate(products[:num_samples], 1):
        print(f"\n{i}. {product.get('name', 'N/A')}")
        print(f"   💰 Price: {product.get('price', 'N/A')}")
        if product.get('old_price'):
            print(f"   🏷️  Original Price: {product.get('old_price', 'N/A')}")
        print(f"   🏪 Seller: {product.get('seller', 'N/A')}")
        print(f"   🆔 Product ID: {product.get('product_id', 'N/A')}")
        print(f"   🔗 URL: {product.get('url', 'N/A')}")
    print("="*70)


def main():
    """Main scraping function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape products from Marjanemall.ma using Playwright',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape ALL categories (all pages)
  python main.py --all
  
  # Scrape ALL categories with page limit
  python main.py --all --max-pages 5
  
  # Scrape specific category (all pages)
  python main.py --category informatique-gaming
  
  # Scrape specific category with page limit
  python main.py --category telephone-objets-connectes --max-pages 10
  
  # Scrape multiple specific categories
  python main.py --categories informatique-gaming telephone-objets-connectes electromenager
  
  # Show browser window (not headless)
  python main.py --all --no-headless
  
  # Save only CSV format
  python main.py --all --format csv
        """
    )
    
    parser.add_argument('--category', type=str, 
                       help='Scrape a specific category (e.g., telephone-objets-connectes)')
    parser.add_argument('--categories', nargs='+', 
                       help='Scrape multiple specific categories')
    parser.add_argument('--all', action='store_true', 
                       help='Scrape ALL categories')
    parser.add_argument('--max-pages', type=int, 
                       help='Maximum pages per category (default: unlimited)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--no-headless', dest='headless', action='store_false',
                       help='Show browser window')
    parser.add_argument('--format', type=str, default='both', 
                       choices=['csv', 'json', 'both'],
                       help='Output format (default: both)')
    parser.add_argument('--list-categories', action='store_true',
                       help='List all available categories and exit')
    
    args = parser.parse_args()
    
    # List categories if requested
    if args.list_categories:
        print("\n📂 Available categories:")
        for i, cat in enumerate(MarjanemallScraper.CATEGORIES, 1):
            print(f"  {i}. {cat}")
        print()
        return
    
    # Validate arguments
    if not (args.all or args.category or args.categories):
        parser.print_help()
        print("\n⚠️  Error: You must specify --all, --category, or --categories")
        sys.exit(1)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    logger.info("="*70)
    logger.info("🚀 Starting Marjanemall.ma scraper (Playwright)")
    logger.info("="*70)
    
    try:
        with MarjanemallScraper(headless=args.headless) as scraper:
            
            if args.all:
                # Scrape ALL categories
                logger.info("📂 Mode: SCRAPE ALL CATEGORIES")
                if args.max_pages:
                    logger.info(f"⚙️  Max pages per category: {args.max_pages}")
                else:
                    logger.info(f"⚙️  Max pages per category: unlimited (until no more products)")
                
                all_results = scraper.scrape_all_categories(max_pages_per_category=args.max_pages)
                save_all_results(all_results, timestamp, args.format)
                
                # Show sample from first category
                if all_results:
                    first_category = next(iter(all_results.keys()))
                    if all_results[first_category]:
                        print_sample_products(all_results[first_category], first_category)
            
            elif args.categories:
                # Scrape multiple specific categories
                logger.info(f"📂 Mode: SCRAPE SPECIFIC CATEGORIES ({len(args.categories)} categories)")
                logger.info(f"📋 Categories: {', '.join(args.categories)}")
                
                all_results = scraper.scrape_all_categories(
                    max_pages_per_category=args.max_pages,
                    categories=args.categories
                )
                save_all_results(all_results, timestamp, args.format)
                
                # Show sample from first category
                if all_results:
                    first_category = next(iter(all_results.keys()))
                    if all_results[first_category]:
                        print_sample_products(all_results[first_category], first_category)
            
            elif args.category:
                # Scrape single category
                logger.info(f"📂 Mode: SCRAPE SINGLE CATEGORY")
                logger.info(f"📋 Category: {args.category}")
                if args.max_pages:
                    logger.info(f"⚙️  Max pages: {args.max_pages}")
                else:
                    logger.info(f"⚙️  Max pages: unlimited")
                
                products = scraper.scrape_category(args.category, max_pages=args.max_pages)
                
                if products:
                    save_category_results(args.category, products, timestamp, args.format)
                    
                    # Print summary
                    print("\n" + "="*70)
                    print("📊 SCRAPING SUMMARY")
                    print("="*70)
                    print(f"Category: {args.category}")
                    print(f"Total products: {len(products)}")
                    print("="*70)
                    
                    print_sample_products(products, args.category)
                else:
                    logger.warning("⚠️  No products were scraped")
    
    except KeyboardInterrupt:
        logger.warning("⚠️  Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error during scraping: {e}", exc_info=True)
        sys.exit(1)
    
    logger.info("\n✅ Scraping completed successfully!")


if __name__ == "__main__":
    main()