"""
Main entry point for the project
Scrapes both Jumia.ma and Marjanemall.ma and saves to MongoDB
Optimized for daily scheduled runs with comprehensive logging and error handling
"""

import sys
import time
from pathlib import Path
from datetime import datetime
import logging
import traceback

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Ensure logs directory exists
Path('logs').mkdir(parents=True, exist_ok=True)

# Configure comprehensive logging
log_filename = f"logs/scraping_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import scrapers and database
from scraping.jumia_scraper import JumiaScraper
from scraping.marjanemall_scraper import MarjanemallScraper
from database.db_manager import DatabaseManager


class ScrapingOrchestrator:
    """Orchestrates scraping from multiple sources with error handling and logging"""
    
    def __init__(self):
        self.start_time = time.time()
        self.stats = {
            'jumia': {'categories': 0, 'products': 0, 'errors': 0},
            'marjanemall': {'categories': 0, 'products': 0, 'errors': 0},
            'database': {'new_products': 0, 'updated_products': 0, 'price_changes': 0}
        }
        self.db = None
        
    def initialize_database(self):
        """Initialize database connection"""
        try:
            logger.info("="*80)
            logger.info("Initializing MongoDB connection...")
            self.db = DatabaseManager(use_env=True)
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            return False
    
    def scrape_jumia(self, max_pages_per_category=None):
        """Scrape all categories from Jumia.ma"""
        logger.info("="*80)
        logger.info("STARTING JUMIA.MA SCRAPING")
        logger.info("="*80)
        
        jumia_start = time.time()
        scraper = JumiaScraper(delay=1.0)
        
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
        
        all_products = []
        
        for idx, category in enumerate(categories, 1):
            category_start = time.time()
            logger.info(f"\n[{idx}/{len(categories)}] Scraping Jumia category: {category}")
            
            try:
                products = scraper.scrape_category(category, max_pages=max_pages_per_category)
                
                if products:
                    all_products.extend(products)
                    self.stats['jumia']['categories'] += 1
                    self.stats['jumia']['products'] += len(products)
                    category_time = time.time() - category_start
                    logger.info(f"✓ Category '{category}': {len(products)} products in {category_time:.2f}s")
                else:
                    logger.warning(f"⚠ No products found in category '{category}'")
                    self.stats['jumia']['errors'] += 1
                    
            except Exception as e:
                logger.error(f"✗ Error scraping category '{category}': {e}", exc_info=True)
                self.stats['jumia']['errors'] += 1
                # Continue with next category
                continue
        
        jumia_time = time.time() - jumia_start
        logger.info(f"\n{'='*80}")
        logger.info(f"JUMIA SCRAPING COMPLETE")
        logger.info(f"Total: {len(all_products)} products from {self.stats['jumia']['categories']} categories")
        logger.info(f"Time: {jumia_time:.2f} seconds ({jumia_time/60:.2f} minutes)")
        logger.info(f"{'='*80}\n")
        
        return all_products
    
    def scrape_marjanemall(self, max_pages_per_category=None):
        """Scrape all categories from Marjanemall.ma"""
        logger.info("="*80)
        logger.info("STARTING MARJANEMALL.MA SCRAPING")
        logger.info("="*80)
        
        marjanemall_start = time.time()
        scraper = MarjanemallScraper(delay=1.0)
        
        categories = [
            'telephone-objets-connectes',
            'tv-son-photo',
            'informatique-gaming',
            'electromenager',
            'maison-cuisine-deco',
            'beaute-sante',
            'vetements-chaussures-bijoux-accessoires',
            'bebe-jouets',
            'sport',
            'auto-moto',
            'brico-jardin-animalerie',
            'librairie',
            'epicerie-fine'
        ]
        
        all_products = []
        
        for idx, category in enumerate(categories, 1):
            category_start = time.time()
            logger.info(f"\n[{idx}/{len(categories)}] Scraping Marjanemall category: {category}")
            
            try:
                products = scraper.scrape_category(category, max_pages=max_pages_per_category)
                
                if products:
                    all_products.extend(products)
                    self.stats['marjanemall']['categories'] += 1
                    self.stats['marjanemall']['products'] += len(products)
                    category_time = time.time() - category_start
                    logger.info(f"✓ Category '{category}': {len(products)} products in {category_time:.2f}s")
                else:
                    logger.warning(f"⚠ No products found in category '{category}'")
                    self.stats['marjanemall']['errors'] += 1
                    
            except Exception as e:
                logger.error(f"✗ Error scraping category '{category}': {e}", exc_info=True)
                self.stats['marjanemall']['errors'] += 1
                # Continue with next category
                continue
        
        marjanemall_time = time.time() - marjanemall_start
        logger.info(f"\n{'='*80}")
        logger.info(f"MARJANEMALL SCRAPING COMPLETE")
        logger.info(f"Total: {len(all_products)} products from {self.stats['marjanemall']['categories']} categories")
        logger.info(f"Time: {marjanemall_time:.2f} seconds ({marjanemall_time/60:.2f} minutes)")
        logger.info(f"{'='*80}\n")
        
        return all_products
    
    def save_to_database(self, products, source_name):
        """Save products to database with error handling"""
        if not self.db or not products:
            return
        
        logger.info(f"Saving {len(products)} {source_name} products to database...")
        save_start = time.time()
        
        try:
            # Save in batches to avoid memory issues
            batch_size = 100
            total_batches = (len(products) + batch_size - 1) // batch_size
            
            for batch_idx in range(0, len(products), batch_size):
                batch = products[batch_idx:batch_idx + batch_size]
                batch_num = (batch_idx // batch_size) + 1
                
                try:
                    stats = self.db.save_products(batch, detect_price_changes=True)
                    self.stats['database']['new_products'] += stats['new_products']
                    self.stats['database']['updated_products'] += stats['updated_products']
                    self.stats['database']['price_changes'] += stats['price_changes_detected']
                    
                    logger.info(f"  Batch {batch_num}/{total_batches}: {len(batch)} products saved "
                              f"({stats['new_products']} new, {stats['updated_products']} updated, "
                              f"{stats['price_changes_detected']} price changes)")
                except Exception as e:
                    logger.error(f"Error saving batch {batch_num}: {e}", exc_info=True)
                    # Continue with next batch
                    continue
            
            save_time = time.time() - save_start
            logger.info(f"✓ Database save complete in {save_time:.2f}s")
            
        except Exception as e:
            logger.error(f"✗ Database save failed: {e}", exc_info=True)
    
    def print_final_summary(self):
        """Print final summary of scraping session"""
        total_time = time.time() - self.start_time
        
        logger.info("\n" + "="*80)
        logger.info("SCRAPING SESSION SUMMARY")
        logger.info("="*80)
        logger.info(f"Start Time: {datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total Duration: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info("")
        logger.info("JUMIA.MA:")
        logger.info(f"  Categories scraped: {self.stats['jumia']['categories']}")
        logger.info(f"  Products scraped: {self.stats['jumia']['products']}")
        logger.info(f"  Errors: {self.stats['jumia']['errors']}")
        logger.info("")
        logger.info("MARJANEMALL.MA:")
        logger.info(f"  Categories scraped: {self.stats['marjanemall']['categories']}")
        logger.info(f"  Products scraped: {self.stats['marjanemall']['products']}")
        logger.info(f"  Errors: {self.stats['marjanemall']['errors']}")
        logger.info("")
        logger.info("DATABASE:")
        logger.info(f"  New products: {self.stats['database']['new_products']}")
        logger.info(f"  Updated products: {self.stats['database']['updated_products']}")
        logger.info(f"  Price changes detected: {self.stats['database']['price_changes']}")
        
        if self.db:
            try:
                db_stats = self.db.get_statistics()
                logger.info("")
                logger.info("DATABASE STATISTICS:")
                logger.info(f"  Total products: {db_stats['total_products']}")
                logger.info(f"  Total price records: {db_stats['total_price_records']}")
                logger.info(f"  Total price changes: {db_stats['total_price_changes']}")
            except Exception as e:
                logger.warning(f"Could not get database statistics: {e}")
        
        logger.info("="*80)
    
    def run_full_scrape(self, max_pages_per_category=None):
        """Run full scraping from both sources"""
        try:
            logger.info("="*80)
            logger.info("STARTING FULL SCRAPING SESSION")
            logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("="*80)
            
            # Initialize database
            if not self.initialize_database():
                logger.error("Cannot proceed without database connection")
                return False
            
            # Scrape Jumia
            jumia_products = self.scrape_jumia(max_pages_per_category=max_pages_per_category)
            if jumia_products:
                self.save_to_database(jumia_products, "Jumia")
            
            # Scrape Marjanemall
            marjanemall_products = self.scrape_marjanemall(max_pages_per_category=max_pages_per_category)
            if marjanemall_products:
                self.save_to_database(marjanemall_products, "Marjanemall")
            
            # Print final summary
            self.print_final_summary()
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in scraping session: {e}", exc_info=True)
            return False
        finally:
            # Close database connection
            if self.db:
                try:
                    self.db.close()
                    logger.info("Database connection closed")
                except:
                    pass


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape products from Jumia.ma and Marjanemall.ma')
    parser.add_argument('--max-pages', type=int, default=None, 
                       help='Maximum pages per category (default: all pages)')
    parser.add_argument('--jumia-only', action='store_true', 
                       help='Scrape only Jumia.ma')
    parser.add_argument('--marjanemall-only', action='store_true', 
                       help='Scrape only Marjanemall.ma')
    args = parser.parse_args()
    
    orchestrator = ScrapingOrchestrator()
    
    try:
        if not orchestrator.initialize_database():
            logger.error("Failed to initialize database. Exiting.")
            sys.exit(1)
        
        if args.jumia_only:
            # Scrape only Jumia
            jumia_products = orchestrator.scrape_jumia(max_pages_per_category=args.max_pages)
            if jumia_products:
                orchestrator.save_to_database(jumia_products, "Jumia")
        elif args.marjanemall_only:
            # Scrape only Marjanemall
            marjanemall_products = orchestrator.scrape_marjanemall(max_pages_per_category=args.max_pages)
            if marjanemall_products:
                orchestrator.save_to_database(marjanemall_products, "Marjanemall")
        else:
            # Scrape both (default)
            orchestrator.run_full_scrape(max_pages_per_category=args.max_pages)
        
        orchestrator.print_final_summary()
        
    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if orchestrator.db:
            orchestrator.db.close()


if __name__ == "__main__":
    main()
