"""
Database manager for product scraping using MongoDB
Handles saving products and tracking price history
"""

import logging
import os
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
from bson import ObjectId

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from project root
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, will use environment variables directly
    pass

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database operations for product and price tracking using MongoDB"""
    
    def __init__(self, 
                 connection_string: str = None,
                 database_name: str = None,
                 public_key: str = None,
                 private_key: str = None,
                 cluster_name: str = None,
                 use_env: bool = True):
        """
        Initialize MongoDB database manager
        
        Args:
            connection_string: MongoDB connection string (if provided, will use this)
            database_name: Name of the database (defaults to MONGODB_DATABASE from .env)
            public_key: MongoDB Atlas public key (username)
            private_key: MongoDB Atlas private key (password)
            cluster_name: MongoDB Atlas cluster name (e.g., "cluster0.xxxxx" or full hostname)
            use_env: Whether to load credentials from .env file (default: True)
        """
        # Check if we should use local MongoDB
        use_local = os.getenv('MONGODB_USE_LOCAL', 'false').lower() == 'true'
        
        # Load from environment variables if use_env is True
        if use_env:
            if not connection_string:
                connection_string = os.getenv('MONGODB_CONNECTION_STRING')
            if not database_name:
                database_name = os.getenv('MONGODB_DATABASE', 'project10' if use_local else 'jumia_products')
            if not public_key:
                public_key = os.getenv('MONGODB_USERNAME')
            if not private_key:
                private_key = os.getenv('MONGODB_PASSWORD')
            if not cluster_name:
                cluster_name = os.getenv('MONGODB_CLUSTER')
        
        # Set default database name
        if not database_name:
            database_name = 'project10' if use_local else 'jumia_products'
        
        self.database_name = database_name
        
        # Build connection string
        if use_local or (not connection_string and not public_key and not private_key):
            # Use local MongoDB
            local_host = os.getenv('MONGODB_LOCAL_HOST', 'localhost')
            local_port = os.getenv('MONGODB_LOCAL_PORT', '27017')
            self.connection_string = f"mongodb://{local_host}:{local_port}/"
            logger.info(f"Using local MongoDB: {local_host}:{local_port}")
        elif connection_string:
            # Replace <db_password> placeholder if present
            if '<db_password>' in connection_string:
                if private_key:
                    self.connection_string = connection_string.replace('<db_password>', private_key)
                else:
                    raise ValueError("Connection string contains <db_password> placeholder but no password provided. Set MONGODB_PASSWORD in .env file or provide private_key parameter.")
            else:
                self.connection_string = connection_string
        elif public_key and private_key:
            # MongoDB Atlas connection string format
            # URL encode the password in case it contains special characters
            from urllib.parse import quote_plus
            encoded_password = quote_plus(private_key)
            
            if cluster_name:
                # Use provided cluster name
                if '.' in cluster_name:
                    # Full hostname provided
                    host = cluster_name
                else:
                    # Just cluster name, add .mongodb.net
                    host = f"{cluster_name}.mongodb.net"
            else:
                # Try common default
                host = "cluster0.mongodb.net"
            
            self.connection_string = f"mongodb+srv://{public_key}:{encoded_password}@{host}/?retryWrites=true&w=majority"
        else:
            # Default local MongoDB
            self.connection_string = "mongodb://localhost:27017/"
            logger.info("Using default local MongoDB: localhost:27017")
        
        # Connect to MongoDB
        try:
            # Handle connection string with <db_password> placeholder
            if '<db_password>' in self.connection_string:
                logger.warning("Connection string contains <db_password> placeholder. Please replace it with your actual password.")
                raise ValueError("Connection string contains <db_password> placeholder. Replace it with your actual password.")
            
            # Connect to MongoDB
            # Workaround for Python 3.13 SSL/cryptography compatibility issue
            try:
                self.client = MongoClient(
                    self.connection_string, 
                    serverSelectionTimeoutMS=10000
                )
            except AttributeError as ssl_error:
                if 'X509_get_default_cert_dir_env' in str(ssl_error):
                    # Python 3.13 compatibility issue - try with tlsAllowInvalidCertificates
                    logger.warning("SSL context issue detected, trying with relaxed TLS settings...")
                    # Modify connection string to add tls options
                    conn_str_with_tls = self.connection_string
                    if '?' in conn_str_with_tls:
                        conn_str_with_tls += '&tlsAllowInvalidCertificates=true'
                    else:
                        conn_str_with_tls += '?tlsAllowInvalidCertificates=true'
                    self.client = MongoClient(
                        conn_str_with_tls,
                        serverSelectionTimeoutMS=10000,
                        tlsAllowInvalidCertificates=True
                    )
                else:
                    raise
            # Test connection
            self.client.server_info()
            self.db = self.client[database_name]
            self._create_indexes()
            logger.info(f"Connected to MongoDB database: {database_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.error(f"Connection string used: {self.connection_string.split('@')[0]}@***")
            logger.error("\nTo fix this:")
            if '<db_password>' in self.connection_string:
                logger.error("1. Replace <db_password> in connection string with your actual password")
                logger.error("   Or provide private_key parameter when using connection_string")
            logger.error("2. Get your MongoDB Atlas connection string from:")
            logger.error("   https://cloud.mongodb.com -> Clusters -> Connect -> Connect your application")
            logger.error("3. Use the full connection string like:")
            logger.error("   mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/")
            logger.error("4. Or provide cluster_name parameter with your actual cluster hostname")
            logger.error("5. If SSL errors occur, try: pip install --upgrade cryptography")
            raise
    
    def _create_indexes(self):
        """Create indexes for better query performance"""
        try:
            # Products collection indexes
            self.db.products.create_index("product_id", unique=True)
            self.db.products.create_index("brand")
            self.db.products.create_index("category")
            self.db.products.create_index("source")
            self.db.products.create_index("last_updated_at")
            
            # Price history collection indexes
            self.db.price_history.create_index([("product_id", 1), ("scraped_at", -1)])
            self.db.price_history.create_index("product_id")
            self.db.price_history.create_index("scraped_at")
            
            # Price changes collection indexes
            self.db.price_changes.create_index("product_id")
            self.db.price_changes.create_index("changed_at")
            self.db.price_changes.create_index("change_type")
            
            # Alerts collection indexes
            self.db.alerts.create_index("product_id")
            self.db.alerts.create_index("created_at")
            self.db.alerts.create_index("alert_type")
            self.db.alerts.create_index([("is_read", 1), ("is_resolved", 1)])
            
            logger.info("Database indexes created/verified")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")
    
    def save_products(self, products: List[Dict], detect_price_changes: bool = True) -> Dict[str, int]:
        """
        Save or update products and their price history
        
        Args:
            products: List of product dictionaries from scraper
            detect_price_changes: Whether to detect and record price changes
            
        Returns:
            Dictionary with statistics: {
                'new_products': int,
                'updated_products': int,
                'new_price_records': int,
                'price_changes_detected': int
            }
        """
        stats = {
            'new_products': 0,
            'updated_products': 0,
            'new_price_records': 0,
            'price_changes_detected': 0
        }
        
        try:
            for product_data in products:
                product_id = product_data.get('product_id')
                if not product_id:
                    logger.warning(f"Product missing product_id: {product_data.get('name')}")
                    continue
                
                # Prepare product document
                product_doc = self._prepare_product_document(product_data)
                
                # Check if product exists
                existing_product = self.db.products.find_one({"product_id": product_id})
                
                if existing_product:
                    # Update existing product
                    product_doc['last_updated_at'] = datetime.utcnow()
                    self.db.products.update_one(
                        {"product_id": product_id},
                        {"$set": product_doc}
                    )
                    stats['updated_products'] += 1
                else:
                    # Insert new product
                    product_doc['first_seen_at'] = datetime.utcnow()
                    product_doc['last_updated_at'] = datetime.utcnow()
                    self.db.products.insert_one(product_doc)
                    stats['new_products'] += 1
                
                # Save price history
                price_record = self._prepare_price_history_document(product_data)
                result = self.db.price_history.insert_one(price_record)
                price_record['_id'] = result.inserted_id
                stats['new_price_records'] += 1
                
                # Detect price changes if enabled
                if detect_price_changes:
                    price_change = self._detect_price_change(product_id, price_record)
                    if price_change:
                        self.db.price_changes.insert_one(price_change)
                        stats['price_changes_detected'] += 1
            
            logger.info(f"Saved products: {stats}")
            
        except Exception as e:
            logger.error(f"Error saving products: {e}", exc_info=True)
            raise
        
        return stats
    
    def _prepare_product_document(self, product_data: Dict) -> Dict:
        """Prepare product document for MongoDB"""
        categories = product_data.get('categories')
        if isinstance(categories, list):
            categories = json.dumps(categories)
        
        return {
            'product_id': product_data.get('product_id'),
            'name': product_data.get('name'),
            'display_name': product_data.get('displayName') or product_data.get('name'),
            'brand': product_data.get('brand'),
            'url': product_data.get('url'),
            'image_url': product_data.get('image_url'),
            'image_alt': product_data.get('image_alt'),
            'category': product_data.get('category'),
            'categories': categories,
            'category_key': product_data.get('category_key'),
            'tags': product_data.get('tags'),
            'brand_key': product_data.get('brand_key'),
            'seller_id': product_data.get('seller_id'),
            'seller': product_data.get('seller'),  # For Marjanemall
            'is_official_store': product_data.get('is_official_store', False),
            'official_store_name': product_data.get('official_store_name'),
            'is_sponsored': product_data.get('is_sponsored', False),
            'is_buyable': product_data.get('is_buyable', True),
            'is_second_chance': product_data.get('is_second_chance', False),
            'express_delivery': product_data.get('express_delivery', False),
            'campaign_name': product_data.get('campaign_name'),
            'campaign_identifier': product_data.get('campaign_identifier'),
            'last_scraped_at': datetime.utcnow(),
            'source': product_data.get('source', 'jumia.ma')
        }
    
    def _prepare_price_history_document(self, product_data: Dict) -> Dict:
        """Prepare price history document for MongoDB"""
        scraped_at_str = product_data.get('scraped_at')
        if isinstance(scraped_at_str, str):
            try:
                scraped_at = datetime.fromisoformat(scraped_at_str.replace('Z', '+00:00'))
            except:
                scraped_at = datetime.utcnow()
        else:
            scraped_at = datetime.utcnow()
        
        return {
            'product_id': product_data.get('product_id'),
            'scraped_at': scraped_at,
            'price': product_data.get('price'),
            'price_text': product_data.get('price_text'),
            'raw_price': product_data.get('raw_price'),
            'old_price': product_data.get('old_price'),
            'old_price_text': product_data.get('old_price_text'),
            'discount': product_data.get('discount'),
            'discount_text': product_data.get('discount_text'),
            'price_euro': product_data.get('price_euro'),
            'old_price_euro': product_data.get('old_price_euro'),
            'discount_euro': product_data.get('discount_euro'),
            'rating': product_data.get('rating'),
            'review_count': product_data.get('review_count'),
            'is_available': True
        }
    
    def _detect_price_change(self, product_id: str, new_price_record: Dict) -> Optional[Dict]:
        """Detect and create price change record if price changed"""
        # Get previous price record
        previous_record = self.db.price_history.find_one(
            {
                "product_id": product_id,
                "_id": {"$ne": new_price_record.get("_id")}
            },
            sort=[("scraped_at", -1)]
        )
        
        if not previous_record:
            # New product
            if new_price_record.get('price'):
                return {
                    'product_id': product_id,
                    'change_type': 'new_product',
                    'current_price': new_price_record.get('price'),
                    'current_discount': new_price_record.get('discount'),
                    'changed_at': datetime.utcnow(),
                    'current_scrape_id': str(new_price_record.get('_id'))
                }
            return None
        
        # Compare prices
        old_price = previous_record.get('price')
        new_price = new_price_record.get('price')
        
        if old_price is None or new_price is None:
            return None
        
        if old_price == new_price:
            # Check if discount changed
            old_discount = previous_record.get('discount') or 0
            new_discount = new_price_record.get('discount') or 0
            if old_discount != new_discount:
                change_type = 'discount_added' if new_discount > old_discount else 'discount_removed'
                return {
                    'product_id': product_id,
                    'change_type': change_type,
                    'previous_price': old_price,
                    'current_price': new_price,
                    'price_difference': 0,
                    'percentage_change': 0,
                    'previous_discount': old_discount,
                    'current_discount': new_discount,
                    'changed_at': datetime.utcnow(),
                    'previous_scrape_id': str(previous_record.get('_id')),
                    'current_scrape_id': str(new_price_record.get('_id'))
                }
            return None
        
        # Price changed
        price_diff = new_price - old_price
        percentage_change = ((new_price - old_price) / old_price) * 100 if old_price > 0 else 0
        
        change_type = 'decrease' if price_diff < 0 else 'increase'
        
        return {
            'product_id': product_id,
            'change_type': change_type,
            'previous_price': old_price,
            'current_price': new_price,
            'price_difference': price_diff,
            'percentage_change': percentage_change,
            'previous_discount': previous_record.get('discount'),
            'current_discount': new_price_record.get('discount'),
            'changed_at': datetime.utcnow(),
            'previous_scrape_id': str(previous_record.get('_id')),
            'current_scrape_id': str(new_price_record.get('_id'))
        }
    
    def get_product_price_history(self, product_id: str, limit: Optional[int] = None) -> List[Dict]:
        """Get price history for a product"""
        query = {"product_id": product_id}
        cursor = self.db.price_history.find(query).sort("scraped_at", -1)
        
        if limit:
            cursor = cursor.limit(limit)
        
        records = list(cursor)
        # Convert ObjectId to string and datetime to ISO format
        for record in records:
            record['id'] = str(record.pop('_id'))
            if record.get('scraped_at'):
                record['scraped_at'] = record['scraped_at'].isoformat()
        
        return records
    
    def get_current_prices(self, category: Optional[str] = None, brand: Optional[str] = None, source: Optional[str] = None) -> List[Dict]:
        """Get current prices for all products (or filtered)"""
        # Get latest price for each product
        pipeline = [
            {
                "$sort": {"scraped_at": -1}
            },
            {
                "$group": {
                    "_id": "$product_id",
                    "latest_price": {"$first": "$$ROOT"}
                }
            }
        ]
        
        latest_prices = {}
        for result in self.db.price_history.aggregate(pipeline):
            product_id = result['_id']
            latest_prices[product_id] = result['latest_price']
        
        # Get products
        product_query = {}
        if category:
            product_query['category'] = category
        if brand:
            product_query['brand'] = brand
        if source:
            product_query['source'] = source
        
        results = []
        for product in self.db.products.find(product_query):
            product_id = product['product_id']
            if product_id in latest_prices:
                price_record = latest_prices[product_id]
                result = {
                    **product,
                    'price': price_record.get('price'),
                    'price_text': price_record.get('price_text'),
                    'old_price': price_record.get('old_price'),
                    'old_price_text': price_record.get('old_price_text'),
                    'discount': price_record.get('discount'),
                    'discount_text': price_record.get('discount_text'),
                    'rating': price_record.get('rating'),
                    'review_count': price_record.get('review_count'),
                    'last_price_update': price_record.get('scraped_at').isoformat() if price_record.get('scraped_at') else None,
                    'is_available': price_record.get('is_available')
                }
                # Convert ObjectId and datetime
                result['id'] = str(result.pop('_id'))
                if result.get('first_seen_at'):
                    result['first_seen_at'] = result['first_seen_at'].isoformat()
                if result.get('last_updated_at'):
                    result['last_updated_at'] = result['last_updated_at'].isoformat()
                if result.get('last_scraped_at'):
                    result['last_scraped_at'] = result['last_scraped_at'].isoformat()
                results.append(result)
        
        return results
    
    def get_price_changes(self, 
                         change_type: Optional[str] = None,
                         since: Optional[datetime] = None,
                         min_percentage: Optional[float] = None) -> List[Dict]:
        """Get price changes with optional filters"""
        query = {}
        
        if change_type:
            query['change_type'] = change_type
        if since:
            query['changed_at'] = {"$gte": since}
        if min_percentage:
            query['percentage_change'] = {"$gte": abs(min_percentage)}
        
        changes = list(self.db.price_changes.find(query).sort("changed_at", -1))
        
        # Convert ObjectId and datetime
        for change in changes:
            change['id'] = str(change.pop('_id'))
            if change.get('changed_at'):
                change['changed_at'] = change['changed_at'].isoformat()
        
        return changes
    
    def get_products_with_price_drops(self, min_percentage: float = 10.0, days: int = 7) -> List[Dict]:
        """Get products with significant price drops"""
        since = datetime.utcnow() - timedelta(days=days)
        
        changes = list(self.db.price_changes.find({
            "change_type": "decrease",
            "changed_at": {"$gte": since},
            "percentage_change": {"$lte": -min_percentage}
        }).sort("percentage_change", 1))
        
        result = []
        for change in changes:
            product = self.db.products.find_one({"product_id": change['product_id']})
            if product:
                result.append({
                    'product_id': change['product_id'],
                    'product_name': product.get('name'),
                    'brand': product.get('brand'),
                    'previous_price': change.get('previous_price'),
                    'current_price': change.get('current_price'),
                    'price_drop': abs(change.get('price_difference', 0)),
                    'percentage_drop': abs(change.get('percentage_change', 0)),
                    'changed_at': change.get('changed_at').isoformat() if change.get('changed_at') else None,
                    'url': product.get('url')
                })
        
        return result
    
    def create_alert(self, product_id: str, alert_type: str, message: str, 
                    price_value: Optional[float] = None, threshold_value: Optional[float] = None):
        """Create an alert"""
        alert = {
            'product_id': product_id,
            'alert_type': alert_type,
            'message': message,
            'price_value': price_value,
            'threshold_value': threshold_value,
            'created_at': datetime.utcnow(),
            'is_read': False,
            'is_resolved': False
        }
        self.db.alerts.insert_one(alert)
        logger.info(f"Alert created: {alert_type} for product {product_id}")
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        total_products = self.db.products.count_documents({})
        total_price_records = self.db.price_history.count_documents({})
        total_price_changes = self.db.price_changes.count_documents({})
        total_alerts = self.db.alerts.count_documents({})
        unread_alerts = self.db.alerts.count_documents({"is_read": False})
        
        # Products with price history
        products_with_history = len(self.db.price_history.distinct("product_id"))
        
        return {
            'total_products': total_products,
            'products_with_price_history': products_with_history,
            'total_price_records': total_price_records,
            'total_price_changes': total_price_changes,
            'total_alerts': total_alerts,
            'unread_alerts': unread_alerts
        }
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")
