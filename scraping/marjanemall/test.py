"""
Ultra-simple script to download HTML pages
"""

import requests
import time

def download_simple():
    """Simple download of 3 pages"""
    
    base_url = "https://www.marjanemall.ma/telephone-objets-connectes"
    
    for page in [1, 2, 3]:
        print(f"\n{'='*50}")
        print(f"Downloading page {page}")
        print('='*50)
        
        # Build URL
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?page={page}"
        
        print(f"URL: {url}")
        
        try:
            # Simple request
            response = requests.get(url, timeout=30)
            print(f"Status: {response.status_code}")
            print(f"Content length: {len(response.text):,} characters")
            
            # Save to file
            filename = f"page_{page}.html"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Saved to: {filename}")
            
            # Quick check for content
            if "animate-slideUp" in response.text:
                print("✓ Found 'animate-slideUp' (product containers)")
            
            if "/p/" in response.text:
                count = response.text.count('/p/')
                print(f"✓ Found {count} '/p/' (product URLs)")
            
            if "Aucun produit trouvé" in response.text:
                print("✗ Found 'Aucun produit trouvé' (no products)")
            
            # Wait before next request
            if page < 3:
                print(f"Waiting 2 seconds before next page...")
                time.sleep(2)
                
        except Exception as e:
            print(f"✗ Error: {e}")
            break

if __name__ == "__main__":
    download_simple()