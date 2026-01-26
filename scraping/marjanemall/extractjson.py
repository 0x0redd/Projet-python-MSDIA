"""
Playwright version - uses browser to get fully rendered page
"""

from playwright.sync_api import sync_playwright
import time
import json

def extract_with_playwright():
    url = "https://www.marjanemall.ma/telephone-objets-connectes"
    
    print("Launching browser...")
    
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=False)  # Set to True for headless
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            # Go to page
            print(f"Navigating to: {url}")
            page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            time.sleep(3)
            
            # Scroll to trigger loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            # Get the rendered HTML
            html = page.content()
            
            print(f"Got HTML: {len(html):,} characters")
            
            # Save HTML
            with open('playwright_rendered.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print("Saved to playwright_rendered.html")
            
            # Extract data using JavaScript
            print("\nExtracting data with JavaScript...")
            
            # Execute JavaScript to extract product data
            product_data = page.evaluate("""
                () => {
                    const products = [];
                    
                    // Look for product elements
                    const productElements = document.querySelectorAll('div.animate-slideUp');
                    
                    productElements.forEach(element => {
                        const product = {};
                        
                        // Get product name
                        const nameElem = element.querySelector('h3');
                        if (nameElem) product.name = nameElem.textContent.trim();
                        
                        // Get price
                        const priceElem = element.querySelector('span.text-lg.font-extrabold.text-primary');
                        if (priceElem) product.price = priceElem.textContent.trim();
                        
                        // Get product URL
                        const linkElem = element.querySelector('a[href^="/p/"]');
                        if (linkElem) {
                            product.url = linkElem.getAttribute('href');
                            // Extract SKU from URL
                            const match = product.url.match(/([a-z0-9]{8,})$/i);
                            if (match) product.sku = match[1].toUpperCase();
                        }
                        
                        // Get image
                        const imgElem = element.querySelector('img');
                        if (imgElem) product.image = imgElem.getAttribute('src');
                        
                        if (product.name || product.sku) {
                            products.push(product);
                        }
                    });
                    
                    return products;
                }
            """)
            
            print(f"Found {len(product_data)} products via JavaScript")
            
            if product_data:
                # Save the data
                with open('playwright_products.json', 'w', encoding='utf-8') as f:
                    json.dump(product_data, f, indent=2, ensure_ascii=False)
                print("Saved to playwright_products.json")
                
                # Show sample
                print("\nSample products:")
                for i, product in enumerate(product_data[:5]):
                    print(f"{i+1}. {product.get('sku', 'N/A')} - {product.get('name', 'N/A')[:40]}... - {product.get('price', 'N/A')}")
            
            return product_data
            
        except Exception as e:
            print(f"Error: {e}")
            return []
        finally:
            browser.close()

if __name__ == "__main__":
    print("="*60)
    print("PLAYWRIGHT EXTRACTOR")
    print("="*60)
    
    # First install: pip install playwright
    # Then run: playwright install
    
    products = extract_with_playwright()
    print(f"\nTotal products found: {len(products)}")