import requests
import os
from datetime import datetime
import warnings

# Suppress the SSL warning
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

def download_brvm_to_current_folder():
    """Download BRVM HTML to the current folder"""
    
    url = "https://www.brvm.org/en/indices"
    
    print("=" * 50)
    print("BRVM HTML DOWNLOADER")
    print("=" * 50)
    
    # Show current directory
    current_dir = os.getcwd()
    print(f"📂 Current folder: {current_dir}")
    print(f"🌐 Target URL: {url}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    try:
        print("🔄 Downloading page...")
        
        response = requests.get(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            verify=False,  # Bypass SSL certificate verification
            timeout=30
        )
        
        print(f"✅ HTTP Status: {response.status_code}")
        print(f"📄 File size: {len(response.text):,} characters")
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"brvm_indices_{timestamp}.html"
        
        # Save in current directory
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(response.text)
        
        # Get full path
        full_path = os.path.join(current_dir, filename)
        
        print("-" * 50)
        print(f"💾 SAVED TO CURRENT FOLDER:")
        print(f"   📁 File: {filename}")
        print(f"   📍 Full path: {full_path}")
        
        # Quick content check
        print("\n🔍 Content check:")
        if "<table" in response.text:
            print("   ✓ Table tags found")
        if "SONATEL" in response.text:
            print("   ✓ SONATEL data found")
        if "Closing price" in response.text:
            print("   ✓ Stock price data found")
        
        # Show first few lines to verify
        print("\n📋 First 3 lines of HTML:")
        print("-" * 40)
        lines = response.text.split('\n')[:3]
        for i, line in enumerate(lines, 1):
            print(f"{i}: {line[:80]}..." if len(line) > 80 else f"{i}: {line}")
        print("-" * 40)
        
        return filename
        
    except requests.exceptions.Timeout:
        print("❌ Timeout: The request took too long")
    except requests.exceptions.ConnectionError:
        print("❌ Connection error: Check your internet")
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
    
    return None

# Run the download
if __name__ == "__main__":
    downloaded_file = download_brvm_to_current_folder()
    
    if downloaded_file:
        print("\n" + "=" * 50)
        print("🎉 DOWNLOAD COMPLETE!")
        print("=" * 50)
        print("\nNext steps:")
        print("1. ✅ HTML file is in your current project folder")
        print("2. 🔍 You can open it in a browser to check")
        print("3. ➡️  Next we'll parse the stock data from it")
    else:
        print("\n❌ Download failed. Please try again.")
    
    # Keep window open
    input("\nPress Enter to exit...")