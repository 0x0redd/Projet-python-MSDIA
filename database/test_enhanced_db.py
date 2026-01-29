"""
Test script for enhanced database functionality
"""

import logging
from datetime import datetime, timedelta
from enhanced_db_manager import EnhancedDatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_enhanced_database():
    """Test all enhanced database features"""
    
    logger.info("Testing Enhanced Database Manager...")
    
    # Initialize database
    db = EnhancedDatabaseManager()
    
    try:
        # Test 1: User Management
        logger.info("\n=== Testing User Management ===")
        
        test_email = "test@example.com"
        user_id = db.create_user(test_email, "Test User", {"theme": "dark"})
        logger.info(f"Created user: {user_id}")
        
        user = db.get_user_by_email(test_email)
        logger.info(f"Retrieved user: {user['name']} ({user['email']})")
        
        db.update_user_login(test_email)
        logger.info("Updated user login time")
        
        # Test 2: Enhanced Product Saving
        logger.info("\n=== Testing Enhanced Product Saving ===")
        
        sample_products = [
            {
                'product_id': 'test_product_1',
                'name': 'Test Product 1',
                'brand': 'Test Brand',
                'category': 'Electronics',
                'price': 100.0,
                'url': 'https://example.com/product1',
                'scraped_at': datetime.now().isoformat()
            },
            {
                'product_id': 'test_product_2',
                'name': 'Test Product 2',
                'brand': 'Test Brand',
                'category': 'Electronics',
                'price': 200.0,
                'url': 'https://example.com/product2',
                'scraped_at': datetime.now().isoformat()
            }
        ]
        
        stats = db.save_products_enhanced(sample_products, "test_source")
        logger.info(f"Saved products: {stats}")
        
        # Test 3: User Alert Preferences
        logger.info("\n=== Testing User Alert Preferences ===")
        
        success = db.save_user_alert_preference(
            user_email=test_email,
            product_id='test_product_1',
            price_drop_threshold=15.0,
            price_below_threshold=90.0,
            anomaly_alerts=True
        )
        logger.info(f"Created alert preference: {success}")
        
        preferences = db.get_user_alert_preferences(test_email)
        logger.info(f"Retrieved {len(preferences)} preferences")
        
        # Test 4: Alert Checking
        logger.info("\n=== Testing Alert Checking ===")
        
        # Add another price point to trigger alert
        updated_product = {
            'product_id': 'test_product_1',
            'name': 'Test Product 1',
            'brand': 'Test Brand',
            'category': 'Electronics',
            'price': 80.0,  # Price drop to trigger alert
            'url': 'https://example.com/product1',
            'scraped_at': (datetime.now() + timedelta(hours=1)).isoformat()
        }
        
        db.save_products_enhanced([updated_product], "test_source")
        
        alerts = db.check_user_alerts()
        logger.info(f"Found {len(alerts)} alerts to send")
        
        for alert in alerts:
            logger.info(f"Alert: {alert['alert_type']} for {alert['product_id']}")
            db.record_sent_alert(alert)
        
        # Test 5: Analytics
        logger.info("\n=== Testing Analytics ===")
        
        # Save anomaly
        db.save_anomaly(
            product_id='test_product_1',
            anomaly_score=0.95,
            anomaly_type='price_spike',
            details={'reason': 'Unusual price increase'}
        )
        
        # Save prediction
        db.save_prediction(
            product_id='test_product_1',
            predicted_price=85.0,
            confidence=0.8,
            model_version='v1.0',
            prediction_horizon_days=7
        )
        
        # Test 6: Enhanced Statistics
        logger.info("\n=== Testing Enhanced Statistics ===")
        
        stats = db.get_enhanced_statistics()
        logger.info("Enhanced Statistics:")
        for category, data in stats.items():
            if isinstance(data, dict):
                logger.info(f"  {category}:")
                for key, value in data.items():
                    logger.info(f"    {key}: {value}")
            else:
                logger.info(f"  {category}: {data}")
        
        # Test 7: System Logging
        logger.info("\n=== Testing System Logging ===")
        
        db.log_system_event(
            component='test_script',
            level='INFO',
            message='Test completed successfully',
            details={'test_duration': '5 minutes'}
        )
        
        logger.info("All tests completed successfully!")
        
        # Cleanup
        logger.info("\n=== Cleaning Up Test Data ===")
        
        # Remove test alert preference
        db.remove_user_alert_preference(test_email, 'test_product_1')
        
        # Remove test products
        db.db.products.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        db.db.price_history.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        db.db.price_changes.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        db.db.anomalies.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        db.db.predictions.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        db.db.alert_history.delete_many({'product_id': {'$in': ['test_product_1', 'test_product_2']}})
        
        # Remove test user
        db.db.users.delete_one({'email': test_email})
        
        logger.info("Test data cleaned up")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    test_enhanced_database()