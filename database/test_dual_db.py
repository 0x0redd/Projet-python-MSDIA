
"""
Test script to verify both MongoDB and SQLite database connections and operations.
"""

import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def test_mongodb_connection():
    
    try:
        from pymongo import MongoClient
        
        # Get MongoDB connection string from environment or use default
        connection_string = os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017/')
        database_name = os.getenv('MONGODB_DATABASE', 'project10')
        
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[database_name]
        
        # Test connection
        db.command('ping')
        logger.info("✅ MongoDB connection successful")
        
        # Test collection access
        products = db.products
        count = products.count_documents({})
        logger.info(f"📊 MongoDB - Found {count} products in the database")
        
        # Test insert and query
        test_product = {
            'product_id': 'test_mongo_123',
            'name': 'Test Product MongoDB',
            'price': 999.99,
            'source': 'test.ma',
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Insert test product
        result = products.insert_one(test_product)
        logger.info(f"✅ MongoDB - Inserted test product with ID: {result.inserted_id}")
        
        # Query test product
        found = products.find_one({'product_id': 'test_mongo_123'})
        if found:
            logger.info(f"✅ MongoDB - Found test product: {found['name']}")
        else:
            logger.warning("❌ MongoDB - Test product not found")
        
        # Clean up
        products.delete_one({'product_id': 'test_mongo_123'})
        logger.info("🧹 MongoDB - Cleaned up test data")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB test failed: {e}")
        return False

def test_sqlite_connection():
    """Test SQLite connection and basic operations."""
    try:
        import sqlite3
        from pathlib import Path
        
        # Get SQLite database path from environment or use default
        db_path = os.getenv('SQLITE_DB_PATH', './data/database.sqlite')
        
        # Ensure directory exists
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Connect to SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create test table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            source TEXT NOT NULL,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        conn.commit()
        
        logger.info("✅ SQLite connection and table creation successful")
        
        # Test insert
        test_product = ('test_sqlite_123', 'Test Product SQLite', 888.88, 'test.ma', datetime.now(timezone.utc).isoformat())
        
        try:
            cursor.execute('''
            INSERT INTO test_products (product_id, name, price, source, scraped_at)
            VALUES (?, ?, ?, ?, ?)
            ''', test_product)
            conn.commit()
            logger.info("✅ SQLite - Inserted test product")
            
            # Test query
            cursor.execute('SELECT * FROM test_products WHERE product_id = ?', ('test_sqlite_123',))
            found = cursor.fetchone()
            
            if found:
                logger.info(f"✅ SQLite - Found test product: {found[1]}")
            else:
                logger.warning("❌ SQLite - Test product not found")
                
        except sqlite3.IntegrityError as e:
            logger.warning(f"⚠️ SQLite - Test product already exists: {e}")
        
        # Clean up
        cursor.execute('DELETE FROM test_products WHERE product_id = ?', ('test_sqlite_123',))
        conn.commit()
        logger.info("🧹 SQLite - Cleaned up test data")
        
        # Close connection
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SQLite test failed: {e}")
        return False

def test_unified_database():
    
    try:
        from database.database_manager import get_database_manager
        
        logger.info("\n🔍 Testing Unified Database Manager")
        
        # Create a test product
        test_product = {
            'product_id': 'unified_test_123',
            'name': 'Unified Test Product',
            'price': 777.77,
            'price_text': '777.77 Dhs',
            'source': 'test.ma',
            'category': 'Test Category',
            'brand': 'Test Brand',
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Test with both databases
        with get_database_manager(use_mongodb=True, use_sqlite=True) as db:
            # Save product to both databases
            success = db.save_product(test_product)
            logger.info(f"💾 Save product to databases: {'✅' if success else '❌'}")
            
            # Retrieve product
            product = db.get_product('unified_test_123', 'test.ma')
            if product:
                logger.info(f"🔍 Found product in database: {product['name']}")
            else:
                logger.warning("❌ Could not find test product in any database")
            
            # Clean up
            # Note: In a real application, you'd have delete methods in your database manager
            logger.info("ℹ️  Note: Cleanup of test data should be implemented in the database manager")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Unified database test failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting database tests...\n")
    
    # Test MongoDB
    logger.info("=== Testing MongoDB ===")
    mongo_success = test_mongodb_connection()
    
    # Test SQLite
    logger.info("\n=== Testing SQLite ===")
    sqlite_success = test_sqlite_connection()
    
    # Test Unified Database Manager
    logger.info("\n=== Testing Unified Database Manager ===")
    unified_success = test_unified_database()
    
    # Print summary
    logger.info("\n=== Test Summary ===")
    logger.info(f"MongoDB: {'✅' if mongo_success else '❌'}")
    logger.info(f"SQLite: {'✅' if sqlite_success else '❌'}")
    logger.info(f"Unified Manager: {'✅' if unified_success else '❌'}")
    
    if mongo_success and sqlite_success and unified_success:
        logger.info("\n🎉 All database tests completed successfully!")
    else:
        logger.warning("\n⚠️ Some database tests failed. Please check the logs above for details.")
