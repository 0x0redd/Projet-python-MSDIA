"""
Quick MongoDB connection test - uses local MongoDB (localhost:27017, database: project10)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager

if __name__ == "__main__":
    print("Testing MongoDB connection...")
    print("Using: localhost:27017, database: project10")

    try:
        db = DatabaseManager(
            connection_string="mongodb://localhost:27017/",
            database_name="project10",
            use_env=False  # Use explicit local connection
        )

        stats = db.get_statistics()
        print("✅ Connected successfully!")
        print("\nDatabase Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        db.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nMake sure MongoDB is running locally:")
        print("  - Check if MongoDB service is running")
        print("  - Verify MongoDB is listening on localhost:27017")
        import traceback
        traceback.print_exc()
