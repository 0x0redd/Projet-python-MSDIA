"""
SQLite database manager for product scraping and price tracking.
"""

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

logger = logging.getLogger(__name__)

class SQLiteManager:
    """Manages SQLite database operations for product and price tracking"""
    
    def __init__(self, db_path: str = None):
        """
        Initialize SQLite database manager
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path or os.getenv('SQLITE_DB_PATH', './data/database.sqlite')
        self._ensure_db_directory()
        self.conn = self._create_connection()
        self._initialize_database()
    
    def _ensure_db_directory(self):
        """Ensure the directory for the database file exists"""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_connection(self):
        """Create a database connection to the SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def _initialize_database(self):
        """Initialize the database with required tables"""
        try:
            cursor = self.conn.cursor()
            
            # Products table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                price_text TEXT,
                old_price REAL,
                old_price_text TEXT,
                discount INTEGER,
                discount_text TEXT,
                url TEXT NOT NULL,
                image_url TEXT,
                image_alt TEXT,
                category TEXT,
                source TEXT NOT NULL,
                brand TEXT,
                rating REAL,
                review_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id, source)
            )
            ''')
            
            # Price history table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                source TEXT NOT NULL,
                price REAL NOT NULL,
                price_text TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id, source) 
                    REFERENCES products (id, source) 
                    ON DELETE CASCADE
            )
            ''')
            
            # Price changes table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                source TEXT NOT NULL,
                old_price REAL NOT NULL,
                new_price REAL NOT NULL,
                price_difference REAL NOT NULL,
                percent_change REAL NOT NULL,
                change_detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id, source) 
                    REFERENCES products (id, source) 
                    ON DELETE CASCADE
            )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_products_id_source ON products(id, source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_product_id ON price_history(product_id, source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_changes_product_id ON price_changes(product_id, source)')
            
            self.conn.commit()
            logger.info("Database tables initialized successfully")
            
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def save_product(self, product_data: Dict[str, Any]) -> bool:
        """
        Save or update a product in the database
        
        Args:
            product_data: Dictionary containing product information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            
            # Prepare data for insertion/update
            current_time = datetime.utcnow().isoformat()
            
            # Check if product exists
            cursor.execute(
                'SELECT id FROM products WHERE id = ? AND source = ?',
                (product_data['product_id'], product_data['source'])
            )
            exists = cursor.fetchone() is not None
            
            if exists:
                # Update existing product
                cursor.execute('''
                UPDATE products 
                SET name = ?, price = ?, price_text = ?, old_price = ?, old_price_text = ?,
                    discount = ?, discount_text = ?, url = ?, image_url = ?, image_alt = ?,
                    category = ?, brand = ?, rating = ?, review_count = ?, updated_at = ?
                WHERE id = ? AND source = ?
                ''', (
                    product_data.get('name'),
                    product_data.get('price'),
                    product_data.get('price_text'),
                    product_data.get('old_price'),
                    product_data.get('old_price_text'),
                    product_data.get('discount'),
                    product_data.get('discount_text'),
                    product_data.get('url'),
                    product_data.get('image_url'),
                    product_data.get('image_alt'),
                    product_data.get('category'),
                    product_data.get('brand'),
                    product_data.get('rating'),
                    product_data.get('review_count'),
                    current_time,
                    product_data['product_id'],
                    product_data['source']
                ))
            else:
                # Insert new product
                cursor.execute('''
                INSERT INTO products (
                    id, name, price, price_text, old_price, old_price_text,
                    discount, discount_text, url, image_url, image_alt,
                    category, source, brand, rating, review_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product_data['product_id'],
                    product_data.get('name'),
                    product_data.get('price'),
                    product_data.get('price_text'),
                    product_data.get('old_price'),
                    product_data.get('old_price_text'),
                    product_data.get('discount'),
                    product_data.get('discount_text'),
                    product_data.get('url'),
                    product_data.get('image_url'),
                    product_data.get('image_alt'),
                    product_data.get('category'),
                    product_data['source'],
                    product_data.get('brand'),
                    product_data.get('rating'),
                    product_data.get('review_count'),
                    current_time,
                    current_time
                ))
            
            # Save to price history
            cursor.execute('''
            INSERT INTO price_history (product_id, source, price, price_text, scraped_at)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                product_data['product_id'],
                product_data['source'],
                product_data.get('price'),
                product_data.get('price_text'),
                current_time
            ))
            
            self.conn.commit()
            logger.debug(f"Product {product_data['product_id']} saved/updated successfully")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error saving product {product_data.get('product_id')}: {e}")
            self.conn.rollback()
            return False
    
    def get_product(self, product_id: str, source: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a product by ID and source
        
        Args:
            product_id: The product ID
            source: The source (e.g., 'jumia.ma', 'marjanemall.ma')
            
        Returns:
            Optional[Dict]: Product data if found, None otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT * FROM products WHERE id = ? AND source = ?',
                (product_id, source)
            )
            
            row = cursor.fetchone()
            if not row:
                return None
                
            # Convert row to dict
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
            
        except sqlite3.Error as e:
            logger.error(f"Error retrieving product {product_id}: {e}")
            return None
    
    def get_price_history(self, product_id: str, source: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get price history for a product
        
        Args:
            product_id: The product ID
            source: The source (e.g., 'jumia.ma', 'marjanemall.ma')
            days: Number of days of history to retrieve
            
        Returns:
            List[Dict]: List of price history records
        """
        try:
            cursor = self.conn.cursor()
            
            # Calculate date range
            start_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            cursor.execute('''
            SELECT * FROM price_history 
            WHERE product_id = ? AND source = ? AND scraped_at >= ?
            ORDER BY scraped_at DESC
            ''', (product_id, source, start_date))
            
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
            
        except sqlite3.Error as e:
            logger.error(f"Error retrieving price history for {product_id}: {e}")
            return []
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
            logger.info("SQLite database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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
        'review_count': 42
    }
    
    # Test the SQLite manager
    with SQLiteManager(':memory:') as db:
        # Save a product
        db.save_product(sample_product)
        
        # Retrieve the product
        product = db.get_product('test123', 'example.ma')
        print("Retrieved product:", product)
        
        # Get price history
        history = db.get_price_history('test123', 'example.ma')
        print("Price history:", history)
