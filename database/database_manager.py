

import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

logger = logging.getLogger(__name__)

# Import database managers with error handling
try:
    from .db_manager import DatabaseManager as MongoDBManager
except ImportError:
    logger.warning("MongoDB manager not available. Install pymongo to use MongoDB.")
    MongoDBManager = None

try:
    from .sqlite_manager import SQLiteManager
except ImportError as e:
    logger.warning(f"SQLite manager not available: {e}")
    SQLiteManager = None


class DatabaseManager:
    """
    Unified database manager that can work with MongoDB, SQLite, or both.
    """
    
    def __init__(self, use_mongodb: bool = True, use_sqlite: bool = True):
        """
        Initialize the database manager with the specified backends.
        
        Args:
            use_mongodb: Whether to use MongoDB
            use_sqlite: Whether to use SQLite
        """
        self.mongodb = None
        self.sqlite = None
        
        if use_mongodb and MongoDBManager:
            try:
                self.mongodb = MongoDBManager()
                logger.info("MongoDB connection established")
            except Exception as e:
                logger.error(f"Failed to initialize MongoDB: {e}")
        
        if use_sqlite and SQLiteManager:
            try:
                self.sqlite = SQLiteManager()
                logger.info("SQLite connection established")
            except Exception as e:
                logger.error(f"Failed to initialize SQLite: {e}")
        
        if not self.mongodb and not self.sqlite:
            raise RuntimeError("No database backends available. Please check your configuration.")
    
    def save_product(self, product_data: Dict[str, Any]) -> bool:
        """
        Save or update a product in the configured databases.
        
        Args:
            product_data: Dictionary containing product information
            
        Returns:
            bool: True if saved to at least one database, False otherwise
        """
        success = False
        
        if self.mongodb:
            try:
                # Convert datetime to string for MongoDB compatibility
                product_copy = product_data.copy()
                if 'scraped_at' in product_copy and hasattr(product_copy['scraped_at'], 'isoformat'):
                    product_copy['scraped_at'] = product_copy['scraped_at'].isoformat()
                
                self.mongodb.save_product(product_copy)
                success = True
            except Exception as e:
                logger.error(f"Error saving to MongoDB: {e}")
        
        if self.sqlite:
            try:
                self.sqlite.save_product(product_data)
                success = True
            except Exception as e:
                logger.error(f"Error saving to SQLite: {e}")
        
        return success
    
    def get_product(self, product_id: str, source: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a product by ID and source.
        
        Args:
            product_id: The product ID
            source: The source (e.g., 'jumia.ma', 'marjanemall.ma')
            
        Returns:
            Optional[Dict]: Product data if found in any database, None otherwise
        """
        # Try MongoDB first if available
        if self.mongodb:
            try:
                product = self.mongodb.get_product(product_id, source)
                if product:
                    return product
            except Exception as e:
                logger.error(f"Error retrieving from MongoDB: {e}")
        
        # Fall back to SQLite
        if self.sqlite:
            try:
                return self.sqlite.get_product(product_id, source)
            except Exception as e:
                logger.error(f"Error retrieving from SQLite: {e}")
        
        return None
    
    def get_price_history(self, product_id: str, source: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get price history for a product.
        
        Args:
            product_id: The product ID
            source: The source (e.g., 'jumia.ma', 'marjanemall.ma')
            days: Number of days of history to retrieve
            
        Returns:
            List[Dict]: List of price history records
        """
        # Try to get from MongoDB first
        if self.mongodb:
            try:
                history = self.mongodb.get_price_history(product_id, source, days)
                if history:
                    return history
            except Exception as e:
                logger.error(f"Error getting history from MongoDB: {e}")
        
        # Fall back to SQLite
        if self.sqlite:
            try:
                return self.sqlite.get_price_history(product_id, source, days)
            except Exception as e:
                logger.error(f"Error getting history from SQLite: {e}")
        
        return []
    
    def close(self):
        """Close all database connections"""
        if self.mongodb:
            try:
                self.mongodb.close()
            except Exception as e:
                logger.error(f"Error closing MongoDB connection: {e}")
        
        if self.sqlite:
            try:
                self.sqlite.close()
            except Exception as e:
                logger.error(f"Error closing SQLite connection: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def get_database_manager(use_mongodb: bool = True, use_sqlite: bool = True) -> DatabaseManager:
    """
    Factory function to get a database manager instance.
    
    Args:
        use_mongodb: Whether to use MongoDB
        use_sqlite: Whether to use SQLite
        
    Returns:
        DatabaseManager: An instance of the database manager
    """
    return DatabaseManager(use_mongodb=use_mongodb, use_sqlite=use_sqlite)


# Example usage
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Example product data
    sample_product = {
        'product_id': 'test123',
        'name': 'Test Product',
        'price': 999.99,
        'price_text': '999.99 Dhs',
        'old_price': 1299.99,
        'old_price_text': '1,299.99 Dhs',
        'discount': 23,
        'discount_text': '-23%',
        'url': 'https://example.com/product/test123',
        'image_url': 'https://example.com/images/test123.jpg',
        'image_alt': 'Test Product Image',
        'category': 'Electronics',
        'source': 'example.ma',
        'brand': 'TestBrand',
        'rating': 4.5,
        'review_count': 42,
        'scraped_at': '2023-01-01T12:00:00Z'
    }
    
    # Create a database manager that uses both MongoDB and SQLite
    with get_database_manager(use_mongodb=True, use_sqlite=True) as db:
        # Save a product
        db.save_product(sample_product)
        
        # Retrieve the product
        product = db.get_product('test123', 'example.ma')
        print("Retrieved product:", product)
        
        # Get price history
        history = db.get_price_history('test123', 'example.ma')
        print("Price history:", history)
