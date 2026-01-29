"""
Quick test to verify enhanced database functionality
"""

import logging
from datetime import datetime
from enhanced_db_manager import EnhancedDatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_basic_functionality():
    """Test basic enhanced database functionality"""
    
    logger.info("Testing basic enhanced database functionality...")
    
    # Initialize database
    db = EnhancedDatabaseManager()
    
    try:
        # Test 1: Simple product save
        logger.info("Testing simple product save...")
        
        sample_product = {
            'product_id': 'quick_test_1',
            'name': 'Quick Test Product',
            'brand': 'Test Brand',
            'category': 'Test Category',
            'price': 100.0,
            'scraped_at': datetime.now().isoformat()
        }
        
        stats = db.save_products_enhanced([sample_product], "test_source")
        logger.info(f"Product save stats: {stats}")
        
        # Test 2: User creation
        logger.info("Testing user creation...")
        
        test_email = "quicktest@example.com"
        user_id = db.create_user(test_email, "Quick Test User")
        logger.info(f"Created user: {user_id}")
        
        # Test 3: Alert preference (simplified)
        logger.info("Testing alert preference...")
        
        try:
            # Check if preference already exists
            existing_prefs = db.get_user_alert_preferences(test_email)
            logger.info(f"Existing preferences: {len(existing_prefs)}")
            
            # Try to create new preference
            success = db.save_user_alert_preference(
                user_email=test_email,
                product_id='quick_test_1',
                price_drop_threshold=10.0
            )
            logger.info(f"Alert preference created: {success}")
            
        except Exception as e:
            logger.error(f"Alert preference error: {e}")
        
        # Test 4: Statistics
        logger.info("Testing statistics...")
        
        stats = db.get_enhanced_statistics()
        logger.info(f"Total products: {stats.get('products', {}).get('total', 0)}")
        logger.info(f"Total users: {stats.get('users', {}).get('total', 0)}")
        
        logger.info("Basic functionality test completed!")
        
        # Cleanup
        logger.info("Cleaning up...")
        db.db.products.delete_one({'product_id': 'quick_test_1'})
        db.db.price_history.delete_many({'product_id': 'quick_test_1'})
        db.db.users.delete_one({'email': test_email})
        db.db.user_alert_preferences.delete_many({'user_email': test_email})
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    test_basic_functionality()