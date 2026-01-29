"""
Migration script to upgrade existing database to enhanced structure
"""

import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import pandas as pd

from enhanced_db_manager import EnhancedDatabaseManager
from db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseMigration:
    """Handle migration from old to enhanced database structure"""
    
    def __init__(self):
        self.old_db = None
        self.new_db = None
        
    def connect_databases(self):
        """Connect to both old and new database managers"""
        try:
            # Connect to old database
            self.old_db = DatabaseManager()
            logger.info("Connected to old database structure")
            
            # Connect to enhanced database
            self.new_db = EnhancedDatabaseManager()
            logger.info("Connected to enhanced database structure")
            
        except Exception as e:
            logger.error(f"Error connecting to databases: {e}")
            raise
    
    def migrate_products_and_prices(self):
        """Migrate products and price history"""
        try:
            logger.info("Migrating products and price history...")
            
            # Get all products from old database
            products = list(self.old_db.db.products.find({}))
            logger.info(f"Found {len(products)} products to migrate")
            
            migrated_products = 0
            migrated_prices = 0
            
            for product in products:
                try:
                    product_id = product['product_id']
                    
                    # Get price history for this product
                    price_history = list(self.old_db.db.price_history.find(
                        {'product_id': product_id}
                    ).sort('scraped_at', 1))
                    
                    if not price_history:
                        continue
                    
                    # Prepare products for enhanced save
                    products_to_save = []
                    for price_record in price_history:
                        # Combine product and price data
                        combined_data = {
                            **product,
                            'price': price_record.get('price'),
                            'price_text': price_record.get('price_text'),
                            'old_price': price_record.get('old_price'),
                            'discount': price_record.get('discount'),
                            'rating': price_record.get('rating'),
                            'review_count': price_record.get('review_count'),
                            'scraped_at': price_record.get('scraped_at').isoformat() if price_record.get('scraped_at') else datetime.now().isoformat()
                        }
                        products_to_save.append(combined_data)
                    
                    # Save to enhanced database
                    if products_to_save:
                        source = product.get('source', 'unknown')
                        stats = self.new_db.save_products_enhanced(products_to_save, source)
                        migrated_products += 1
                        migrated_prices += stats['new_price_records']
                
                except Exception as e:
                    logger.error(f"Error migrating product {product.get('product_id', 'unknown')}: {e}")
            
            logger.info(f"Migration completed: {migrated_products} products, {migrated_prices} price records")
            
        except Exception as e:
            logger.error(f"Error in product migration: {e}")
            raise
    
    def migrate_file_based_preferences(self):
        """Migrate file-based alert preferences to database"""
        try:
            logger.info("Migrating file-based alert preferences...")
            
            prefs_file = Path("alerts_history/user_preferences.json")
            if not prefs_file.exists():
                logger.info("No file-based preferences found")
                return
            
            with open(prefs_file, 'r') as f:
                preferences = json.load(f)
            
            migrated_prefs = 0
            
            for pref in preferences:
                try:
                    if not pref.get('active', True):
                        continue
                    
                    success = self.new_db.save_user_alert_preference(
                        user_email=pref['user_email'],
                        product_id=pref['product_id'],
                        price_drop_threshold=pref.get('price_drop_threshold', 10.0),
                        price_below_threshold=pref.get('price_below_threshold'),
                        anomaly_alerts=pref.get('anomaly_alerts', True)
                    )
                    
                    if success:
                        migrated_prefs += 1
                
                except Exception as e:
                    logger.error(f"Error migrating preference: {e}")
            
            logger.info(f"Migrated {migrated_prefs} alert preferences")
            
        except Exception as e:
            logger.error(f"Error migrating preferences: {e}")
    
    def migrate_csv_data(self):
        """Migrate data from CSV files if database is empty"""
        try:
            logger.info("Checking for CSV data to migrate...")
            
            # Check if we have any products in enhanced database
            product_count = self.new_db.db.products.count_documents({})
            if product_count > 0:
                logger.info(f"Database already has {product_count} products, skipping CSV migration")
                return
            
            # Look for cleaned CSV files
            csv_files = [
                ("data/processed/jumia_cleaned.csv", "Jumia"),
                ("data/processed/marjanemall_cleaned.csv", "Marjanemall")
            ]
            
            total_migrated = 0
            
            for csv_path, source in csv_files:
                if not Path(csv_path).exists():
                    logger.info(f"CSV file not found: {csv_path}")
                    continue
                
                logger.info(f"Migrating data from {csv_path}...")
                
                try:
                    df = pd.read_csv(csv_path)
                    df['scraped_at'] = pd.to_datetime(df['scraped_at'])
                    
                    # Convert DataFrame to list of dicts
                    products_data = df.to_dict('records')
                    
                    # Save to enhanced database
                    stats = self.new_db.save_products_enhanced(products_data, source)
                    total_migrated += stats['new_products']
                    
                    logger.info(f"Migrated {stats['new_products']} products from {csv_path}")
                
                except Exception as e:
                    logger.error(f"Error migrating {csv_path}: {e}")
            
            logger.info(f"Total products migrated from CSV: {total_migrated}")
            
        except Exception as e:
            logger.error(f"Error in CSV migration: {e}")
    
    def verify_migration(self):
        """Verify migration was successful"""
        try:
            logger.info("Verifying migration...")
            
            stats = self.new_db.get_enhanced_statistics()
            
            logger.info("Enhanced Database Statistics:")
            logger.info(f"  Total products: {stats['products']['total']}")
            logger.info(f"  Active products: {stats['products']['active']}")
            logger.info(f"  Total users: {stats['users']['total']}")
            logger.info(f"  Users with alerts: {stats['users']['with_alerts']}")
            logger.info(f"  Alert preferences: {stats['alerts']['total_preferences']}")
            logger.info(f"  Price records: {stats['price_data']['total_records']}")
            logger.info(f"  Price changes: {stats['price_data']['total_changes']}")
            
            # Test API endpoints
            logger.info("Testing enhanced database functionality...")
            
            # Test user creation
            test_email = "test@example.com"
            user_id = self.new_db.create_user(test_email, "Test User")
            logger.info(f"Test user created: {user_id}")
            
            # Test alert preference
            if stats['products']['total'] > 0:
                # Get a sample product
                sample_product = self.new_db.db.products.find_one({})
                if sample_product:
                    success = self.new_db.save_user_alert_preference(
                        test_email,
                        sample_product['product_id'],
                        price_drop_threshold=15.0
                    )
                    logger.info(f"Test alert preference created: {success}")
            
            logger.info("Migration verification completed successfully!")
            
        except Exception as e:
            logger.error(f"Error in migration verification: {e}")
    
    def run_full_migration(self):
        """Run complete migration process"""
        try:
            logger.info("Starting database migration to enhanced structure...")
            
            # Connect to databases
            self.connect_databases()
            
            # Try to migrate from old database first
            try:
                self.migrate_products_and_prices()
            except Exception as e:
                logger.warning(f"Old database migration failed, trying CSV: {e}")
                self.migrate_csv_data()
            
            # Migrate file-based preferences
            self.migrate_file_based_preferences()
            
            # Verify migration
            self.verify_migration()
            
            logger.info("Database migration completed successfully!")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            # Close connections
            if self.old_db:
                self.old_db.close()
            if self.new_db:
                self.new_db.close()


def main():
    """Run migration"""
    migration = DatabaseMigration()
    migration.run_full_migration()


if __name__ == "__main__":
    main()