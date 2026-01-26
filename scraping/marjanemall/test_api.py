"""
Test script to find and test the correct API endpoint for products
"""

import json
import time
import requests
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scraping.marjanemall.marjanemall_api_scraper import MarjanemallAPIScraper


def test_menu_api():
    """Test the menu API"""
    print("="*60)
    print("1. Testing Menu API")
    print("="*60)
    
    scraper = MarjanemallAPIScraper()
    menu_data = scraper.get_menu()
    
    if menu_data:
        print(f"Menu data type: {type(menu_data)}")
        if isinstance(menu_data, dict):
            print(f"Menu keys: {list(menu_data.keys())[:10]}...")
        elif isinstance(menu_data, list):
            print(f"Menu list length: {len(menu_data)}")
        
        # Save for inspection
        with open('menu_data.json', 'w', encoding='utf-8') as f:
            json.dump(menu_data, f, indent=2, ensure_ascii=False)
        print("Menu data saved to menu_data.json")
    else:
        print("Failed to get menu data")
    
    print()


def test_category_endpoints():
    """Test different category API endpoints"""
    print("="*60)
    print("2. Testing Category API Endpoints")
    print("="*60)
    
    scraper = MarjanemallAPIScraper()
    test_category = "telephone-objets-connectes"
    
    # Try different API endpoints
    endpoints_to_test = [
        f"{scraper.api_base_url}/api/appmobilemm/v19/data/{test_category}?page=1",
        f"{scraper.api_base_url}/api/appmobilemm/v16/category/path.json?category={test_category}&page=1",
        f"{scraper.api_base_url}/api/appmobilemm/v10/data/9999997?category={test_category}&page=1",
        f"{scraper.api_base_url}/api/appmobilemm/v19/data/{test_category}?page=1&pageSize=60",
    ]
    
    for i, url in enumerate(endpoints_to_test, 1):
        print(f"\n{i}. Testing: {url}")
        try:
            response = scraper.session.get(url, timeout=30)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"   Response type: {type(data)}")
                    
                    if isinstance(data, dict):
                        print(f"   Keys: {list(data.keys())[:10]}...")
                        
                        # Look for product data
                        if 'items' in data:
                            items = data['items']
                            print(f"   Items count: {len(items) if isinstance(items, list) else 'N/A'}")
                            if isinstance(items, list) and items:
                                print(f"   First item keys: {list(items[0].keys())[:10]}...")
                        
                        if 'products' in data:
                            products = data['products']
                            print(f"   Products count: {len(products) if isinstance(products, list) else 'N/A'}")
                            if isinstance(products, list) and products:
                                print(f"   First product keys: {list(products[0].keys())[:10]}...")
                    
                    elif isinstance(data, list):
                        print(f"   List length: {len(data)}")
                        if data and isinstance(data[0], dict):
                            print(f"   First item keys: {list(data[0].keys())[:10]}...")
                    
                    # Save sample for inspection
                    filename = f"api_response_{i}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"   Saved to {filename}")
                    
                    # Check if valid
                    if scraper.is_valid_product_data(data):
                        print(f"   ✓ Valid product data!")
                        return url, data
                    else:
                        print(f"   ✗ Doesn't look like product data")
                    
                except json.JSONDecodeError:
                    print(f"   ✗ Invalid JSON response")
                    print(f"   First 200 chars: {response.text[:200]}")
            else:
                print(f"   ✗ Failed with status {response.status_code}")
                
        except Exception as e:
            print(f"   ✗ Error: {e}")
        
        time.sleep(1)
    
    return None, None


def test_full_scrape():
    """Test full category scrape"""
    print("\n" + "="*60)
    print("3. Testing Full Category Scrape")
    print("="*60)
    
    scraper = MarjanemallAPIScraper()
    test_category = "telephone-objets-connectes"
    
    print(f"Scraping category: {test_category}")
    products = scraper.scrape_category(test_category, max_pages=2)
    
    print(f"\nGot {len(products)} products")
    
    if products:
        print("\nSample product:")
        print(json.dumps(products[0], indent=2, ensure_ascii=False, default=str))
        
        # Save all products
        with open('test_products.json', 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False, default=str)
        print(f"\nAll products saved to test_products.json")
    else:
        print("No products found")


def quick_debug():
    """Quick debug to see what the API returns"""
    print("="*60)
    print("Quick Debug: Direct API Call")
    print("="*60)
    
    url = "https://appli.marjanemall.ma/api/appmobilemm/v19/data/telephone-objets-connectes?page=1"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.marjanemall.ma/',
        'Origin': 'https://www.marjanemall.ma',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type')}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"\nResponse type: {type(data)}")
                
                if isinstance(data, dict):
                    print(f"Keys: {list(data.keys())}")
                    for key, value in list(data.items())[:5]:
                        if isinstance(value, list):
                            print(f"  {key}: list with {len(value)} items")
                            if value and isinstance(value[0], dict):
                                print(f"    First item keys: {list(value[0].keys())[:10]}...")
                        else:
                            print(f"  {key}: {type(value)}")
                
                elif isinstance(data, list):
                    print(f"List length: {len(data)}")
                    if data and isinstance(data[0], dict):
                        print(f"First item keys: {list(data[0].keys())[:10]}...")
                
                # Save for inspection
                with open('debug_api_response.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print("\nSaved full response to debug_api_response.json")
                
            except json.JSONDecodeError:
                print("Response is not JSON")
                print(f"First 500 chars: {response.text[:500]}")
        else:
            print(f"Error: {response.text[:200]}")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Marjanemall API endpoints')
    parser.add_argument('--quick', action='store_true', help='Quick debug test')
    parser.add_argument('--menu', action='store_true', help='Test menu API')
    parser.add_argument('--endpoints', action='store_true', help='Test category endpoints')
    parser.add_argument('--scrape', action='store_true', help='Test full scrape')
    args = parser.parse_args()
    
    if args.quick:
        quick_debug()
    elif args.menu:
        test_menu_api()
    elif args.endpoints:
        test_category_endpoints()
    elif args.scrape:
        test_full_scrape()
    else:
        # Run all tests
        quick_debug()
        print("\n")
        test_menu_api()
        print("\n")
        working_url, working_data = test_category_endpoints()
        if working_url:
            print(f"\n✓ Found working endpoint: {working_url}")
            print("\n")
            test_full_scrape()
        else:
            print("\n✗ No working endpoint found. Check the saved JSON files for structure.")
