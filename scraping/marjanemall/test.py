"""
Quick example script to test the scraper
This will scrape 2 pages from one category
"""

from marjanemall_scraper import MarjanemallScraper
import json
from datetime import datetime

def main():
    print("🚀 Starting quick test...")
    print("This will scrape 2 pages from 'informatique-gaming' category")
    print("-" * 60)
    
    # Initialize scraper
    with MarjanemallScraper(headless=True) as scraper:
        # Scrape 2 pages from one category
        products = scraper.scrape_category('informatique-gaming', max_pages=2)
        
        if products:
            # Save to JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/quick_test_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            
            print(f"\n✅ Success! Scraped {len(products)} products")
            print(f"💾 Saved to: {filename}")
            print("\nSample products:")
            for i, product in enumerate(products[:3], 1):
                print(f"\n{i}. {product['name']}")
                print(f"   💰 {product['price']}")
                print(f"   🏪 {product['seller']}")
        else:
            print("⚠️  No products found")

if __name__ == "__main__":
    main()