"""
Quick MongoDB connection test - uses .env file
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager

if __name__ == "__main__":
    print("Testing MongoDB connection...")

    db = DatabaseManager(
        use_env=True  # Loads from .env file
    )

    stats = db.get_statistics()
    print("Connected successfully!")
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    db.close()
