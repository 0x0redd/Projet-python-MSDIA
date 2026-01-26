"""
Test MongoDB connection and help configure it
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_connection():
    """Test MongoDB connection with different methods"""
    
    print("="*60)
    print("MongoDB Connection Test")
    print("="*60)
    
    # Method 0: Try loading from .env file first
    print("\nMethod 0: Trying to load from .env file...")
    try:
        db = DatabaseManager(use_env=True)
        print("✅ Connection successful using .env file!")
        stats = db.get_statistics()
        print(f"\nDatabase Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        db.close()
        return True
    except Exception as e:
        print(f"❌ Connection from .env failed: {e}")
        print("Will try other methods...\n")
    
    # Method 1: Try with full connection string
    print("="*60)
    print("Method 1: Using full connection string")
    print("Get your connection string from MongoDB Atlas:")
    print("1. Go to https://cloud.mongodb.com")
    print("2. Select your cluster")
    print("3. Click 'Connect' -> 'Connect your application'")
    print("4. Copy the connection string")
    print("\nExample format:")
    print("mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/")
    
    connection_string = input("\nEnter your full MongoDB connection string (or press Enter to skip): ").strip()
    
    if connection_string:
        # Check if password placeholder exists
        if '<db_password>' in connection_string:
            password = input("Enter your MongoDB password (to replace <db_password>): ").strip()
            if password:
                connection_string = connection_string.replace('<db_password>', password)
            else:
                print("⚠️  Password required to replace <db_password> placeholder")
                return False
        
        try:
            # Extract database name if provided
            if '/' in connection_string and connection_string.count('/') > 3:
                parts = connection_string.rsplit('/', 1)
                connection_string = parts[0] + '/'
                db_name = parts[1].split('?')[0] if len(parts) > 1 else "jumia_products"
            else:
                db_name = "jumia_products"
            
            db = DatabaseManager(
                connection_string=connection_string,
                database_name=db_name
            )
            print("✅ Connection successful!")
            stats = db.get_statistics()
            print(f"\nDatabase Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
            db.close()
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
    
    # Method 2: Try with credentials and cluster name
    print("\n" + "="*60)
    print("Method 2: Using credentials and cluster name")
    
    public_key = input("Enter MongoDB username (public key): ").strip() or '0x0redme'
    private_key = input("Enter MongoDB password (private key): ").strip()
    if not private_key:
        print("⚠️  Password is required")
        return False
    
    cluster_name = input("Enter cluster hostname (e.g., project10.iuhtcyt.mongodb.net or just project10.iuhtcyt): ").strip() or 'project10.iuhtcyt.mongodb.net'
    
    if not cluster_name:
        print("\n⚠️  You need to provide the cluster hostname.")
        print("Find it in your MongoDB Atlas connection string.")
        print("It looks like: project10.iuhtcyt.mongodb.net")
        return False
    
    try:
        db = DatabaseManager(
            database_name="jumia_products",
            public_key=public_key,
            private_key=private_key,
            cluster_name=cluster_name
        )
        print("✅ Connection successful!")
        stats = db.get_statistics()
        print(f"\nDatabase Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        db.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
