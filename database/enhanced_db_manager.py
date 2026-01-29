"""
Enhanced Database Manager for Professional Price Monitoring System
Supports user management, alert preferences, analytics, and improved data structure
"""

import logging
import os
from pathlib import Path
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import json
from bson import ObjectId
import hashlib
import uuid

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

logger = logging.getLogger(__name__)


class EnhancedDatabaseManager:
    """Enhanced database manager with user management and analytics support"""
    
    def __init__(self, 
                 connection_string: str = None,
                 database_name: str = None,
                 use_env: bool = True):
        """Initialize enhanced database manager"""
        
        # Load from environment if needed
        if use_env:
            if not connection_string:
                connection_string = os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017/')
            if not database_name:
                database_name = os.getenv('MONGODB_DATABASE', 'project10')
        
        self.database_name = database_name or 'project10'
        self.connection_string = connection_string or 'mongodb://localhost:27017/'
        
        # Connect to MongoDB
        try:
            self.client = MongoClient(
                self.connection_string, 
                serverSelectionTimeoutMS=10000
            )
            # Test connection
            self.client.server_info()
            self.db = self.client[self.database_name]
            self._create_collections_and_indexes()
            logger.info(f"Connected to enhanced MongoDB database: {self.database_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_collections_and_indexes(self):
        """Create all collections and indexes for the enhanced system"""
        try:
            # Products collection - core product information
            self.db.products.create_index("product_id", unique=True)
            self.db.products.create_index("brand")
            self.db.products.create_index("category")
            self.db.products.create_index("source")
            self.db.products.create_index("last_updated_at")
            self.db.products.create_index([("category", 1), ("brand", 1)])
            self.db.products.create_index("is_active")
            
            # Price history - historical price data
            self.db.price_history.create_index([("product_id", 1), ("scraped_at", -1)])
            self.db.price_history.create_index("product_id")
            self.db.price_history.create_index("scraped_at")
            self.db.price_history.create_index([("scraped_at", -1)])  # For recent prices
            
            # Price changes - detected price movements
            self.db.price_changes.create_index("product_id")
            self.db.price_changes.create_index("changed_at")
            self.db.price_changes.create_index("change_type")
            self.db.price_changes.create_index([("changed_at", -1)])
            self.db.price_changes.create_index([("change_type", 1), ("percentage_change", -1)])
            
            # Users collection - user management
            self.db.users.create_index("email", unique=True)
            self.db.users.create_index("user_id", unique=True)
            self.db.users.create_index("created_at")
            self.db.users.create_index("is_active")
            
            # User alert preferences - personalized alert settings
            self.db.user_alert_preferences.create_index([("user_email", 1), ("product_id", 1)], unique=True)
            self.db.user_alert_preferences.create_index("user_email")
            self.db.user_alert_preferences.create_index("product_id")
            self.db.user_alert_preferences.create_index("is_active")
            self.db.user_alert_preferences.create_index("created_at")
            
            # Alert history - sent alerts tracking
            self.db.alert_history.create_index("user_email")
            self.db.alert_history.create_index("product_id")
            self.db.alert_history.create_index("sent_at")
            self.db.alert_history.create_index("alert_type")
            self.db.alert_history.create_index([("sent_at", -1)])
            
            # Anomalies - detected price anomalies
            self.db.anomalies.create_index("product_id")
            self.db.anomalies.create_index("detected_at")
            self.db.anomalies.create_index("anomaly_score")
            self.db.anomalies.create_index([("detected_at", -1)])
            self.db.anomalies.create_index([("anomaly_score", -1)])
            
            # Predictions - ML price predictions
            self.db.predictions.create_index("product_id")
            self.db.predictions.create_index("prediction_date")
            self.db.predictions.create_index("model_version")
            self.db.predictions.create_index([("prediction_date", -1)])
            
            # Analytics cache - pre-computed analytics
            self.db.analytics_cache.create_index("metric_name", unique=True)
            self.db.analytics_cache.create_index("last_updated")
            
            # System logs - application events
            self.db.system_logs.create_index("timestamp")
            self.db.system_logs.create_index("level")
            self.db.system_logs.create_index("component")
            self.db.system_logs.create_index([("timestamp", -1)])
            
            logger.info("Enhanced database collections and indexes created/verified")
        except Exception as e:
            logger.warning(f"Error creating enhanced indexes: {e}")
    
    # User Management Methods
    def create_user(self, email: str, name: str = None, preferences: Dict = None) -> str:
        """Create a new user"""
        user_id = str(uuid.uuid4())
        
        user_doc = {
            'user_id': user_id,
            'email': email.lower(),
            'name': name or email.split('@')[0],
            'preferences': preferences or {},
            'created_at': datetime.utcnow(),
            'last_login': None,
            'is_active': True,
            'alert_count': 0,
            'total_products_tracked': 0
        }
        
        try:
            self.db.users.insert_one(user_doc)
            logger.info(f"User created: {email}")
            return user_id
        except DuplicateKeyError:
            logger.warning(f"User already exists: {email}")
            existing_user = self.db.users.find_one({'email': email.lower()})
            return existing_user['user_id'] if existing_user else None
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email"""
        user = self.db.users.find_one({'email': email.lower()})
        if user:
            user['id'] = str(user.pop('_id'))
        return user
    
    def update_user_login(self, email: str):
        """Update user's last login time"""
        self.db.users.update_one(
            {'email': email.lower()},
            {'$set': {'last_login': datetime.utcnow()}}
        )
    
    # Enhanced Product Methods
    def save_products_enhanced(self, products: List[Dict], source: str) -> Dict[str, int]:
        """Enhanced product saving with better tracking"""
        stats = {
            'new_products': 0,
            'updated_products': 0,
            'new_price_records': 0,
            'price_changes_detected': 0,
            'errors': 0
        }
        
        try:
            for product_data in products:
                try:
                    product_id = product_data.get('product_id')
                    if not product_id:
                        stats['errors'] += 1
                        continue
                    
                    # Enhanced product document
                    product_doc = self._prepare_enhanced_product_document(product_data, source)
                    
                    # Upsert product
                    result = self.db.products.update_one(
                        {"product_id": product_id},
                        {
                            "$set": product_doc,
                            "$setOnInsert": {
                                "first_seen_at": datetime.utcnow(),
                                "total_price_changes": 0,
                                "avg_price": product_data.get('price', 0),
                                "min_price": product_data.get('price', 0),
                                "max_price": product_data.get('price', 0)
                            }
                        },
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        stats['new_products'] += 1
                    else:
                        stats['updated_products'] += 1
                    
                    # Save price history
                    price_record = self._prepare_enhanced_price_history(product_data)
                    self.db.price_history.insert_one(price_record)
                    stats['new_price_records'] += 1
                    
                    # Detect price changes
                    price_change = self._detect_enhanced_price_change(product_id, price_record)
                    if price_change:
                        self.db.price_changes.insert_one(price_change)
                        stats['price_changes_detected'] += 1
                        
                        # Update product statistics
                        self._update_product_price_stats(product_id, price_record['price'])
                
                except Exception as e:
                    logger.error(f"Error processing product {product_data.get('product_id', 'unknown')}: {e}")
                    stats['errors'] += 1
            
            logger.info(f"Enhanced product save completed: {stats}")
            
        except Exception as e:
            logger.error(f"Error in enhanced product saving: {e}")
            raise
        
        return stats
    
    def _prepare_enhanced_product_document(self, product_data: Dict, source: str) -> Dict:
        """Prepare enhanced product document"""
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
            'tags': product_data.get('tags', []),
            'brand_key': product_data.get('brand_key'),
            'seller_id': product_data.get('seller_id'),
            'seller': product_data.get('seller'),
            'is_official_store': product_data.get('is_official_store', False),
            'official_store_name': product_data.get('official_store_name'),
            'is_sponsored': product_data.get('is_sponsored', False),
            'is_buyable': product_data.get('is_buyable', True),
            'is_second_chance': product_data.get('is_second_chance', False),
            'express_delivery': product_data.get('express_delivery', False),
            'campaign_name': product_data.get('campaign_name'),
            'campaign_identifier': product_data.get('campaign_identifier'),
            'last_scraped_at': datetime.utcnow(),
            'last_updated_at': datetime.utcnow(),
            'source': source,
            'is_active': True,
            'quality_score': self._calculate_product_quality_score(product_data)
        }
    
    def _prepare_enhanced_price_history(self, product_data: Dict) -> Dict:
        """Prepare enhanced price history document"""
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
            'is_available': True,
            'scrape_session_id': product_data.get('scrape_session_id'),
            'data_quality': self._assess_price_data_quality(product_data)
        }
    
    def _calculate_product_quality_score(self, product_data: Dict) -> float:
        """Calculate product data quality score (0-1)"""
        score = 0.0
        total_checks = 0
        
        # Check for essential fields
        essential_fields = ['name', 'price', 'category', 'brand', 'url']
        for field in essential_fields:
            total_checks += 1
            if product_data.get(field):
                score += 1
        
        # Check for additional useful fields
        bonus_fields = ['image_url', 'rating', 'review_count', 'discount']
        for field in bonus_fields:
            total_checks += 1
            if product_data.get(field):
                score += 0.5
        
        return min(score / total_checks, 1.0) if total_checks > 0 else 0.0
    
    def _assess_price_data_quality(self, product_data: Dict) -> str:
        """Assess price data quality"""
        price = product_data.get('price')
        if not price or price <= 0:
            return 'poor'
        
        if product_data.get('price_text') and product_data.get('category'):
            return 'excellent'
        elif product_data.get('price_text') or product_data.get('category'):
            return 'good'
        else:
            return 'fair'
    
    def _update_product_price_stats(self, product_id: str, new_price: float):
        """Update product price statistics"""
        if not new_price or new_price <= 0:
            return
        
        # Get current stats
        product = self.db.products.find_one({'product_id': product_id})
        if not product:
            return
        
        current_min = product.get('min_price', new_price)
        current_max = product.get('max_price', new_price)
        current_avg = product.get('avg_price', new_price)
        price_count = product.get('price_history_count', 0) + 1
        
        # Calculate new average
        new_avg = ((current_avg * (price_count - 1)) + new_price) / price_count
        
        # Update product
        self.db.products.update_one(
            {'product_id': product_id},
            {
                '$set': {
                    'min_price': min(current_min, new_price),
                    'max_price': max(current_max, new_price),
                    'avg_price': new_avg,
                    'price_history_count': price_count,
                    'last_price': new_price,
                    'price_volatility': abs(new_price - current_avg) / current_avg if current_avg > 0 else 0
                }
            }
        )
    
    # User Alert Preferences
    def save_user_alert_preference(self, user_email: str, product_id: str, 
                                  price_drop_threshold: float = 10.0,
                                  price_below_threshold: Optional[float] = None,
                                  anomaly_alerts: bool = True) -> bool:
        """Save user alert preference"""
        try:
            # Ensure user exists
            user = self.get_user_by_email(user_email)
            if not user:
                self.create_user(user_email)
            
            preference_doc = {
                'user_email': user_email.lower(),
                'product_id': product_id,
                'price_drop_threshold': price_drop_threshold,
                'price_below_threshold': price_below_threshold,
                'anomaly_alerts': anomaly_alerts,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'is_active': True,
                'alert_count': 0,
                'last_triggered': None
            }
            
            # Upsert preference
            result = self.db.user_alert_preferences.update_one(
                {'user_email': user_email.lower(), 'product_id': product_id},
                {
                    '$set': {k: v for k, v in preference_doc.items() if k != 'created_at'},
                    '$setOnInsert': {'created_at': preference_doc['created_at']}
                },
                upsert=True
            )
            
            # Update user's alert count
            self.db.users.update_one(
                {'email': user_email.lower()},
                {'$inc': {'alert_count': 1 if result.upserted_id else 0}}
            )
            
            logger.info(f"Alert preference saved: {user_email} -> {product_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving alert preference: {e}")
            return False
    
    def get_user_alert_preferences(self, user_email: str) -> List[Dict]:
        """Get user's alert preferences"""
        preferences = list(self.db.user_alert_preferences.find(
            {'user_email': user_email.lower(), 'is_active': True}
        ).sort('created_at', -1))
        
        # Convert ObjectId and add product info
        for pref in preferences:
            pref['id'] = str(pref.pop('_id'))
            if pref.get('created_at'):
                pref['created_at'] = pref['created_at'].isoformat()
            if pref.get('updated_at'):
                pref['updated_at'] = pref['updated_at'].isoformat()
            if pref.get('last_triggered'):
                pref['last_triggered'] = pref['last_triggered'].isoformat()
            
            # Add product info
            product = self.db.products.find_one({'product_id': pref['product_id']})
            if product:
                pref['product_name'] = product.get('name', 'Unknown')
                pref['product_brand'] = product.get('brand', 'Unknown')
                pref['product_category'] = product.get('category', 'Unknown')
                pref['current_price'] = product.get('last_price')
        
        return preferences
    
    def remove_user_alert_preference(self, user_email: str, product_id: str) -> bool:
        """Remove user alert preference"""
        try:
            result = self.db.user_alert_preferences.update_one(
                {'user_email': user_email.lower(), 'product_id': product_id},
                {'$set': {'is_active': False, 'updated_at': datetime.utcnow()}}
            )
            
            if result.modified_count > 0:
                # Update user's alert count
                self.db.users.update_one(
                    {'email': user_email.lower()},
                    {'$inc': {'alert_count': -1}}
                )
                logger.info(f"Alert preference removed: {user_email} -> {product_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error removing alert preference: {e}")
            return False
    
    def check_user_alerts(self) -> List[Dict]:
        """Check all user alerts and return those that should be triggered"""
        alerts_to_send = []
        
        try:
            # Get all active preferences
            preferences = list(self.db.user_alert_preferences.find({'is_active': True}))
            
            for pref in preferences:
                product_id = pref['product_id']
                user_email = pref['user_email']
                
                # Get recent price history
                recent_prices = list(self.db.price_history.find(
                    {'product_id': product_id}
                ).sort('scraped_at', -1).limit(2))
                
                if len(recent_prices) < 2:
                    continue
                
                current_price = recent_prices[0].get('price')
                previous_price = recent_prices[1].get('price')
                
                if not current_price or not previous_price:
                    continue
                
                # Check price drop threshold
                if pref.get('price_drop_threshold'):
                    price_change_pct = ((current_price - previous_price) / previous_price * 100) if previous_price > 0 else 0
                    
                    if price_change_pct < -pref['price_drop_threshold']:
                        alerts_to_send.append({
                            'user_email': user_email,
                            'product_id': product_id,
                            'alert_type': 'price_drop',
                            'current_price': current_price,
                            'previous_price': previous_price,
                            'change_percent': price_change_pct,
                            'threshold': pref['price_drop_threshold'],
                            'preference_id': str(pref['_id'])
                        })
                
                # Check absolute price threshold
                if pref.get('price_below_threshold') and current_price < pref['price_below_threshold']:
                    alerts_to_send.append({
                        'user_email': user_email,
                        'product_id': product_id,
                        'alert_type': 'price_below_threshold',
                        'current_price': current_price,
                        'threshold': pref['price_below_threshold'],
                        'preference_id': str(pref['_id'])
                    })
            
            return alerts_to_send
            
        except Exception as e:
            logger.error(f"Error checking user alerts: {e}")
            return []
    
    def record_sent_alert(self, alert_data: Dict):
        """Record that an alert was sent"""
        try:
            alert_record = {
                'user_email': alert_data['user_email'],
                'product_id': alert_data['product_id'],
                'alert_type': alert_data['alert_type'],
                'sent_at': datetime.utcnow(),
                'alert_data': alert_data,
                'email_status': 'sent'
            }
            
            self.db.alert_history.insert_one(alert_record)
            
            # Update preference last triggered
            if 'preference_id' in alert_data:
                self.db.user_alert_preferences.update_one(
                    {'_id': ObjectId(alert_data['preference_id'])},
                    {
                        '$set': {'last_triggered': datetime.utcnow()},
                        '$inc': {'alert_count': 1}
                    }
                )
            
            logger.info(f"Alert recorded: {alert_data['alert_type']} for {alert_data['user_email']}")
            
        except Exception as e:
            logger.error(f"Error recording sent alert: {e}")
    
    # Analytics and Insights
    def save_anomaly(self, product_id: str, anomaly_score: float, 
                    anomaly_type: str, details: Dict):
        """Save detected anomaly"""
        anomaly_doc = {
            'product_id': product_id,
            'anomaly_score': anomaly_score,
            'anomaly_type': anomaly_type,
            'details': details,
            'detected_at': datetime.utcnow(),
            'is_resolved': False
        }
        
        self.db.anomalies.insert_one(anomaly_doc)
        logger.info(f"Anomaly saved: {anomaly_type} for {product_id} (score: {anomaly_score})")
    
    def save_prediction(self, product_id: str, predicted_price: float, 
                       confidence: float, model_version: str, 
                       prediction_horizon_days: int):
        """Save price prediction"""
        prediction_doc = {
            'product_id': product_id,
            'predicted_price': predicted_price,
            'confidence': confidence,
            'model_version': model_version,
            'prediction_horizon_days': prediction_horizon_days,
            'prediction_date': datetime.utcnow(),
            'target_date': datetime.utcnow() + timedelta(days=prediction_horizon_days),
            'is_validated': False
        }
        
        self.db.predictions.insert_one(prediction_doc)
        logger.info(f"Prediction saved: {product_id} -> {predicted_price} (confidence: {confidence})")
    
    def get_enhanced_statistics(self) -> Dict:
        """Get comprehensive system statistics"""
        try:
            stats = {
                'products': {
                    'total': self.db.products.count_documents({}),
                    'active': self.db.products.count_documents({'is_active': True}),
                    'by_source': {},
                    'by_category': {},
                    'quality_distribution': {}
                },
                'users': {
                    'total': self.db.users.count_documents({}),
                    'active': self.db.users.count_documents({'is_active': True}),
                    'with_alerts': self.db.users.count_documents({'alert_count': {'$gt': 0}})
                },
                'alerts': {
                    'total_preferences': self.db.user_alert_preferences.count_documents({'is_active': True}),
                    'total_sent': self.db.alert_history.count_documents({}),
                    'sent_today': self.db.alert_history.count_documents({
                        'sent_at': {'$gte': datetime.utcnow().replace(hour=0, minute=0, second=0)}
                    })
                },
                'price_data': {
                    'total_records': self.db.price_history.count_documents({}),
                    'total_changes': self.db.price_changes.count_documents({}),
                    'recent_changes': self.db.price_changes.count_documents({
                        'changed_at': {'$gte': datetime.utcnow() - timedelta(days=7)}
                    })
                },
                'analytics': {
                    'anomalies': self.db.anomalies.count_documents({}),
                    'predictions': self.db.predictions.count_documents({}),
                    'unresolved_anomalies': self.db.anomalies.count_documents({'is_resolved': False})
                },
                'last_updated': datetime.utcnow().isoformat()
            }
            
            # Get source distribution
            source_pipeline = [
                {'$group': {'_id': '$source', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            for result in self.db.products.aggregate(source_pipeline):
                stats['products']['by_source'][result['_id']] = result['count']
            
            # Get category distribution (top 10)
            category_pipeline = [
                {'$group': {'_id': '$category', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            for result in self.db.products.aggregate(category_pipeline):
                stats['products']['by_category'][result['_id']] = result['count']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting enhanced statistics: {e}")
            return {}
    
    def log_system_event(self, component: str, level: str, message: str, details: Dict = None):
        """Log system event"""
        log_doc = {
            'component': component,
            'level': level,
            'message': message,
            'details': details or {},
            'timestamp': datetime.utcnow()
        }
        
        self.db.system_logs.insert_one(log_doc)
    
    def _detect_enhanced_price_change(self, product_id: str, new_price_record: Dict) -> Optional[Dict]:
        """Enhanced price change detection"""
        # Get previous price record (excluding the current one if it has an _id)
        query = {"product_id": product_id}
        if '_id' in new_price_record:
            query["_id"] = {"$ne": new_price_record["_id"]}
        
        previous_record = self.db.price_history.find_one(
            query,
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
                    'data_quality': new_price_record.get('data_quality', 'unknown')
                }
            return None
        
        # Compare prices
        old_price = previous_record.get('price')
        new_price = new_price_record.get('price')
        
        if old_price is None or new_price is None:
            return None
        
        if abs(old_price - new_price) < 0.01:  # Consider prices equal if difference < 1 cent
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
            'significance': 'high' if abs(percentage_change) > 20 else 'medium' if abs(percentage_change) > 5 else 'low',
            'data_quality': new_price_record.get('data_quality', 'unknown')
        }
    
    def close(self):
        """Close database connection"""
        self.client.close()
        logger.info("Enhanced MongoDB connection closed")


# Compatibility wrapper for existing code
class DatabaseManager(EnhancedDatabaseManager):
    """Backward compatibility wrapper"""
    
    def save_products(self, products: List[Dict], detect_price_changes: bool = True) -> Dict[str, int]:
        """Backward compatible save_products method"""
        source = products[0].get('source', 'unknown') if products else 'unknown'
        return self.save_products_enhanced(products, source)
    
    def _detect_price_change(self, product_id: str, new_price_record: Dict) -> Optional[Dict]:
        """Backward compatible price change detection"""
        return self._detect_enhanced_price_change(product_id, new_price_record)