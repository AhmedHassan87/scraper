import requests
import pandas as pd
import random
import time
from bs4 import BeautifulSoup
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
from datetime import datetime
import sys
import os

# ==================== HARDCODED PRODUCTION SETTINGS ====================
PROXY = 'http://brd-customer-hl_b23a98a7-zone-datacenter_proxy1:ysk2p9uz4xxl@brd.superproxy.io:33335'
PROXIES = {'http': PROXY, 'https': PROXY}
MAX_WORKERS_FIRST_PASS = 15
MAX_WORKERS_RETRY = 15
MAX_RETRIES_PAGE_COUNT = 100
MAX_RETRIES_PER_PAGE = 5
REQUEST_TIMEOUT = 8
DELAY_BETWEEN_CATEGORIES = 1

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/115.0.0.0'
]

# ==================== PROPER LOGGING SETUP ====================
def setup_logging():
    """Setup proper logging to file and console"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'scraping_log_{timestamp}.log'

    # Create logger
    logger = logging.getLogger('AmazonScraper')
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File handler
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized. Log file: {os.path.abspath(log_filename)}")
    return logger

logger = setup_logging()

# ==================== GLOBAL STATE ====================
urls, categories, descriptions, prices, images, asins = [], [], [], [], [], []
failed_urls = {}
lock = threading.Lock()
overall_metrics = {
    'total_categories': 0,
    'total_urls': 0,
    'total_pages': 0,
    'total_products': 0,
    'total_time': 0,
    'categories_completed': 0,
    'start_time': None,
    'end_time': None
}

# ==================== SESSION SETUP ====================
session = requests.Session()
#session.proxies.update(PROXIES)
adapter = requests.adapters.HTTPAdapter(
    pool_connections=50,
    pool_maxsize=50,
    max_retries=3,
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

# ==================== CLEAN CORE FUNCTIONS ====================
def get_page_count(url):
    """Get page count with clean logging"""
    for attempt in range(MAX_RETRIES_PAGE_COUNT):
        try:
            headers = {
                'user-agent': random.choice(USER_AGENTS),
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
            }

            response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200 and response.text.strip()[-3:] == '-->':
                soup = BeautifulSoup(response.text, 'lxml')

                pagination_container = soup.find('div', {'class': 's-pagination-container'})
                if pagination_container:
                    page_spans = pagination_container.find_all('span', string=lambda text: text and text.strip().isdigit())
                    if page_spans:
                        page_numbers = [int(span.text.strip()) for span in page_spans]
                        page_count = max(page_numbers)
                        return page_count
                return 1

        except Exception:
            continue

    return 1

def extract_products_fast(html, category):
    """Product extraction without verbose logging"""
    products = []

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return products

    cards = (soup.find_all('div', {'data-component-type': 's-search-result'}) or
            soup.find_all('div', {'class': 's-result-item'}) or
            soup.find_all('div', {'role': 'listitem'}))

    for card in cards:
        try:
            asin = card.get('data-asin')
            if not asin or len(asin) != 10:
                continue

            img_tag = card.find('img', {'class': 's-image'})
            img = img_tag.get('src') if img_tag else None

            title_tag = card.find('h2') or card.find('span', {'class': 'a-size-base-plus'})
            description = None
            if title_tag:
                description = title_tag.get('aria-label') or title_tag.text.strip()

            price_whole = card.find('span', {'class': 'a-price-whole'})
            price_fraction = card.find('span', {'class': 'a-price-fraction'})
            price = None
            if price_whole and price_fraction:
                price = price_whole.text.strip() + price_fraction.text
            elif price_whole:
                price = price_whole.text.strip()

            if all([img, description, price, asin]):
                product_url = f"https://www.amazon.eg/dp/{asin}/"
                products.append({
                    'url': product_url,
                    'category': category,
                    'description': description,
                    'price': price,
                    'image': img,
                    'asin': asin
                })

        except Exception:
            continue

    return products

def process_page_fast_first_pass(args):
    """First pass - clean and efficient"""
    page_url, category, page_num = args

    try:
        headers = {
            'user-agent': random.choice(USER_AGENTS),
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
        }

        response = session.get(page_url, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code == 200 and response.text.strip()[-3:] == '-->':
            products = extract_products_fast(response.text, category)
            if products:
                return {'success': True, 'products': products, 'page_url': page_url, 'page_num': page_num}

        return {'success': False, 'page_url': page_url, 'category': category, 'page_num': page_num}

    except Exception:
        return {'success': False, 'page_url': page_url, 'category': category, 'page_num': page_num}

def process_page_with_retry(args):
    """Retry pass - clean implementation"""
    page_url, category, page_num, max_retries = args

    for attempt in range(max_retries):
        try:
            headers = {
                'user-agent': random.choice(USER_AGENTS),
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
            }

            response = session.get(page_url, headers=headers, timeout=REQUEST_TIMEOUT)
            text=response.text
            if response.status_code == 200 and response.text.strip()[-3:] == '-->':
                products = extract_products_fast(response.text, category)
                if products:
                    return True, products # Return success status and products

        except Exception:
            pass # Continue to next attempt on exception

    return False, [] # Return failure status and no products

def process_single_url_two_phase(category_name, base_url, url_index=1, total_urls=1):
    """Two-phase processing with clean logging"""
    if pd.isna(base_url) or not isinstance(base_url, str):
        return 0, 0

    url_start_time = time.time()

    # Get page count
    page_count = get_page_count(base_url)
    logger.info(f"   Pages found: {page_count}")

    # Generate all page URLs
    page_urls = []
    for page in range(1, page_count + 1):
        page_url = f"{base_url}&page={page}" if "?" in base_url else f"{base_url}?page={page}"
        page_urls.append((page_url, category_name, page))

    # PHASE 1: Fast First Pass
    all_products = []
    failed_first_pass_pages = [] # Pages that failed in phase 1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_FIRST_PASS) as executor:
        futures = [executor.submit(process_page_fast_first_pass, page_data) for page_data in page_urls]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result['success']:
                    all_products.extend(result['products'])
                else:
                    failed_first_pass_pages.append((result['page_url'], result['category'], result['page_num']))
            except Exception as e:
                # An unexpected error during first pass also means failure
                logger.error(f"Unexpected error in Phase 1 for {future}: {e}")
                # This is tricky without knowing which args were passed to the future.
                # For now, if a future raises an exception, we'll just acknowledge it.

    # PHASE 2: Retry Failed Pages
    recovered_in_retry_urls = set()
    truly_failed_after_retries_data = [] # To store (page_url, category, page_num) for pages that still fail

    if failed_first_pass_pages:
        retry_futures_map = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_RETRY) as executor:
            for page_data in failed_first_pass_pages:
                page_url, category, page_num = page_data
                future = executor.submit(process_page_with_retry, (page_url, category, page_num, MAX_RETRIES_PER_PAGE))
                retry_futures_map[future] = page_data # Map future back to original page_data

            for future in as_completed(retry_futures_map):
                original_page_data = retry_futures_map[future]
                original_page_url = original_page_data[0]
                try:
                    success, products = future.result()
                    if success:
                        all_products.extend(products)
                        recovered_in_retry_urls.add(original_page_url)
                    else:
                        # This page truly failed even after retries
                        truly_failed_after_retries_data.append(original_page_data)
                except Exception as e:
                    # An unexpected error during retry also means it failed
                    logger.error(f"Unexpected error in Phase 2 for {original_page_url}: {e}")
                    truly_failed_after_retries_data.append(original_page_data)

    # Track final results
    final_products = len(all_products)
    url_time = time.time() - url_start_time

    with lock:
        for product in all_products:
            urls.append(product['url'])
            categories.append(product['category'])
            descriptions.append(product['description'])
            prices.append(product['price'])
            images.append(product['image'])
            asins.append(product['asin'])

        # Track truly failed URLs (only those that failed all retries)
        if truly_failed_after_retries_data:
            if category_name not in failed_urls:
                failed_urls[category_name] = []
            for page_url, _, _ in truly_failed_after_retries_data:
                failed_urls[category_name].append(page_url)

    logger.info(f"   URL completed: {final_products} products in {url_time:.1f}s")
    logger.info(f"   Pages failed first pass: {len(failed_first_pass_pages)}")
    logger.info(f"   Pages recovered in retry: {len(recovered_in_retry_urls)}")
    logger.info(f"   Pages truly failed after all retries: {len(truly_failed_after_retries_data)}")

    return final_products, url_time

def process_category_with_multiple_urls(category_name, category_urls):
    """Process category with clean progress logging"""
    logger.info(f"🎯 Starting category: {category_name}")
    logger.info(f"   URLs to process: {len(category_urls)}")

    category_start_time = time.time()
    category_products = 0

    url_results = []

    for url_index, category_url in enumerate(category_urls, 1):
        products, url_time = process_single_url_two_phase(
            category_name, category_url, url_index, len(category_urls)
        )
        category_products += products
        url_results.append(products)

        # Small delay between URLs
        if url_index < len(category_urls):
            time.sleep(DELAY_BETWEEN_CATEGORIES / 2)

    total_category_time = time.time() - category_start_time

    with lock:
        overall_metrics['categories_completed'] += 1
        overall_metrics['total_urls'] += len(category_urls)
        overall_metrics['total_products'] += category_products

    return category_products, total_category_time, url_results

def log_category_progress(category_index, total_categories, category_name, category_products, category_time, url_results):
    """Clean progress logging after each category"""
    progress_percent = (category_index / total_categories) * 100
    elapsed_time = time.time() - overall_metrics.get('global_start_time', time.time())

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"CATEGORY {category_index}/{total_categories} COMPLETED ({progress_percent:.1f}%)")
    logger.info("=" * 60)
    logger.info(f"Category: {category_name}")
    logger.info(f"Products: {category_products}")
    logger.info(f"Time: {category_time:.1f}s")

    if category_time > 0:
        logger.info(f"Speed: {category_products/category_time:.1f} products/sec")

    logger.info(f"Progress: {category_index}/{total_categories} categories")
    logger.info(f"Elapsed: {elapsed_time/60:.1f}min")

    # ETA calculation
    if category_index > 0:
        avg_time_per_category = elapsed_time / category_index
        remaining_categories = total_categories - category_index
        eta_seconds = avg_time_per_category * remaining_categories
        logger.info(f"ETA: {eta_seconds/60:.1f}min")

    # URL breakdown for categories with multiple URLs
    if len(url_results) > 1:
        logger.info("URL Breakdown:")
        for i, products in enumerate(url_results, 1):
            logger.info(f"  URL {i}: {products} products")

    # Current overall statistics
    current_total_products = overall_metrics['total_products']
    current_total_urls = overall_metrics['total_urls']
    logger.info("OVERALL PROGRESS:")
    logger.info(f"  Total Products: {current_total_products}")
    logger.info(f"  Total URLs: {current_total_urls}")

    if elapsed_time > 0:
        logger.info(f"  Overall Speed: {current_total_products/elapsed_time:.1f} products/sec")

    logger.info("=" * 60)
    logger.info("")

def save_results():
    """Save all results with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if urls:
        data_dict = {
            'URL': urls,
            'Category': categories,
            'Description': descriptions,
            'Price': prices,
            'Image': images,
            'ASIN': asins
        }

        output_file = f'amazon_products_{timestamp}.xlsx'
        pd.DataFrame(data_dict).to_excel(output_file, index=False)
        logger.info(f"Saved {len(urls)} products to: {output_file}")

    # Save failed URLs
    if failed_urls:
        failed_file = f'failed_urls_{timestamp}.json'
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(failed_urls, f, indent=2, ensure_ascii=False)

        total_failed = sum(len(urls) for urls in failed_urls.values())
        logger.info(f"Saved {total_failed} failed URLs to: {failed_file}")

    # Save performance report
    report_data = {
        'overall_metrics': overall_metrics,
        'timestamp': timestamp,
        'duration_seconds': overall_metrics['total_time']
    }

    report_file = f'performance_report_{timestamp}.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Performance report saved to: {report_file}")
def retry_failed_urls_aggressive(failed_json_path, max_retries=50):
    """
    Retry failed URLs from the failed_urls JSON file with aggressive retry strategy.

    Args:
        failed_json_path: Path to the failed_urls_TIMESTAMP.json file
        max_retries: Maximum retry attempts per page (default: 50)
    """
    logger.info("=" * 60)
    logger.info("AGGRESSIVE RETRY MODE - FAILED URLS")
    logger.info("=" * 60)

    # Load failed URLs
    try:
        with open(failed_json_path, 'r', encoding='utf-8') as f:
            failed_data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading failed URLs file: {e}")
        return

    if not failed_data:
        logger.info("No failed URLs to retry")
        return

    # Count total failed URLs
    total_failed_urls = sum(len(urls) for urls in failed_data.values())
    logger.info(f"Total failed URLs to retry: {total_failed_urls}")
    logger.info(f"Max retries per URL: {max_retries}")
    logger.info(f"Categories with failures: {len(failed_data)}")
    logger.info("=" * 60)
    logger.info("")

    # Track retry results
    retry_results = {
        'recovered_products': 0,
        'still_failed': 0,
        'categories_processed': 0
    }

    recovered_products = []
    still_failed = {}

    # Process each category
    for category_index, (category_name, failed_urls_list) in enumerate(failed_data.items(), 1):
        logger.info(f"🔄 Retrying category: {category_name}")
        logger.info(f"   Failed URLs: {len(failed_urls_list)}")

        category_recovered = 0
        category_still_failed = []

        # Process each failed URL with aggressive retries
        for url_index, page_url in enumerate(failed_urls_list, 1):
            logger.info(f"   Retrying URL {url_index}/{len(failed_urls_list)}")

            success = False

            for attempt in range(max_retries):
                try:
                    headers = {
                        'user-agent': random.choice(USER_AGENTS),
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'accept-language': 'en-US,en;q=0.9',
                    }

                    # Add exponential backoff for retries
                    if attempt > 0:
                        delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                        time.sleep(random.uniform(delay * 0.5, delay))

                    response = session.get(page_url, headers=headers, timeout=REQUEST_TIMEOUT)

                    if response.status_code ==200  and response.text.strip()[-3:] == '-->':
                        products = extract_products_fast(response.text, category_name)

                        if products:
                            recovered_products.extend(products)
                            category_recovered += len(products)
                            logger.info(f"      ✓ Recovered {len(products)} products on attempt {attempt + 1}")
                            success = True
                            break

                    # Handle specific error codes
                    elif response.status_code == 429:
                        logger.warning(f"      Rate limited (429) on attempt {attempt + 1}, backing off...")
                        time.sleep(random.uniform(10, 20))
                    elif response.status_code == 503:
                        logger.warning(f"      Service unavailable (503) on attempt {attempt + 1}")
                        time.sleep(random.uniform(5, 10))

                except Exception:
                    continue

            if not success:
                category_still_failed.append(page_url)
                logger.warning(f"      ✗ Failed after {max_retries} attempts")

        # Update results
        retry_results['recovered_products'] += category_recovered
        retry_results['still_failed'] += len(category_still_failed)
        retry_results['categories_processed'] += 1

        if category_still_failed:
            still_failed[category_name] = category_still_failed

        logger.info(f"   Category results: {category_recovered} recovered, {len(category_still_failed)} still failed")
        logger.info("")

        # Small delay between categories
        if category_index < len(failed_data):
            time.sleep(DELAY_BETWEEN_CATEGORIES)

    # Final summary
    logger.info("=" * 60)
    logger.info("AGGRESSIVE RETRY COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Products recovered: {retry_results['recovered_products']}")
    logger.info(f"URLs still failed: {retry_results['still_failed']}")
    logger.info(f"Categories processed: {retry_results['categories_processed']}")
    logger.info(f"Success rate: {(retry_results['recovered_products']/(retry_results['recovered_products']+retry_results['still_failed'])*100):.1f}%")
    logger.info("=" * 60)
    logger.info("")

    # Save recovered products
    if recovered_products:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Prepare data
        recovered_data = {
            'URL': [p['url'] for p in recovered_products],
            'Category': [p['category'] for p in recovered_products],
            'Description': [p['description'] for p in recovered_products],
            'Price': [p['price'] for p in recovered_products],
            'Image': [p['image'] for p in recovered_products],
            'ASIN': [p['asin'] for p in recovered_products]
        }

        # Save recovered products
        recovered_file = f'recovered_products_{timestamp}.xlsx'
        pd.DataFrame(recovered_data).to_excel(recovered_file, index=False)
        logger.info(f"Saved {len(recovered_products)} recovered products to: {recovered_file}")

        # Save still-failed URLs
        if still_failed:
            still_failed_file = f'still_failed_urls_{timestamp}.json'
            with open(still_failed_file, 'w', encoding='utf-8') as f:
                json.dump(still_failed, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {retry_results['still_failed']} still-failed URLs to: {still_failed_file}")

        # Save retry report
        retry_report = {
            'retry_results': retry_results,
            'timestamp': timestamp,
            'max_retries_used': max_retries,
            'original_failed_file': failed_json_path
        }

        report_file = f'retry_report_{timestamp}.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(retry_report, f, indent=2, ensure_ascii=False)
        logger.info(f"Retry report saved to: {report_file}")

    return recovered_products, still_failed


# ==================== USAGE EXAMPLE ====================
# Add this to your main() function or run separately:

def run_aggressive_retry():
    """
    Run aggressive retry on the most recent failed_urls file
    """
    import glob

    # Find the most recent failed_urls JSON file
    failed_files = glob.glob('failed_urls_*.json')

    if not failed_files:
        logger.error("No failed_urls_*.json files found in current directory")
        return

    # Get the most recent file
    most_recent = max(failed_files, key=os.path.getmtime)
    logger.info(f"Using failed URLs file: {most_recent}")
    logger.info("")

    # Run aggressive retry with 50 attempts per URL
    recovered_products, still_failed = retry_failed_urls_aggressive(
        failed_json_path=most_recent,
        max_retries=10  # Increase this if you want even more retries
    )

    return recovered_products, still_failed


def main():
    """Main function - clean bulk test on all categories"""
    global overall_metrics

    try:
        df = pd.read_excel('category mapping.xlsx')
        df=df[:40]
        # Group by category to handle multiple URLs
        category_urls_map = {}
        for idx, row in df.iterrows():
            if pd.notna(row['amazon_url']) and pd.notna(row['subcategory']):
                category = row['subcategory']
                url = row['amazon_url']

                if category not in category_urls_map:
                    category_urls_map[category] = []
                category_urls_map[category].append(url)

        if not category_urls_map:
            logger.error("No valid categories found")
            return

        logger.info(f"Starting bulk test on {len(category_urls_map)} categories")

    except Exception as e:
        logger.error(f"Error loading Excel file: {e}")
        return

    # Initialize global state
    urls.clear(), categories.clear(), descriptions.clear(), prices.clear(), images.clear(), asins.clear()
    failed_urls.clear()
    overall_metrics = {
        'total_categories': len(category_urls_map),
        'total_urls': 0,
        'total_pages': 0,
        'total_products': 0,
        'total_time': 0,
        'categories_completed': 0,
        'start_time': datetime.now().isoformat(),
        'global_start_time': time.time()
    }

    global_start = time.time()

    # Show initial summary
    logger.info("=" * 60)
    logger.info("BULK SCRAPING INITIALIZED")
    logger.info("=" * 60)
    logger.info(f"Total Categories: {len(category_urls_map)}")
    logger.info(f"Total URLs: {sum(len(urls) for urls in category_urls_map.values())}")
    logger.info(f"Workers: {MAX_WORKERS_FIRST_PASS} (first pass), {MAX_WORKERS_RETRY} (retry)")
    logger.info("=" * 60)
    logger.info("")

    # Process all categories
    for i, (category_name, category_urls_list) in enumerate(category_urls_map.items(), 1):
        category_start_time = time.time()
        category_products, category_time, url_results = process_category_with_multiple_urls(category_name, category_urls_list)

        # Log detailed progress after each category
        log_category_progress(i, len(category_urls_map), category_name, category_products, category_time, url_results)

        # Delay between categories
        if i < len(category_urls_map):
            time.sleep(DELAY_BETWEEN_CATEGORIES)

    total_time = time.time() - global_start
    overall_metrics['total_time'] = total_time
    overall_metrics['end_time'] = datetime.now().isoformat()

    # Final summary
    logger.info("=" * 60)
    logger.info("BULK TEST COMPLETED!")
    logger.info("=" * 60)
    logger.info(f"Categories processed: {overall_metrics['categories_completed']}/{overall_metrics['total_categories']}")
    logger.info(f"Total URLs processed: {overall_metrics['total_urls']}")
    logger.info(f"Total products collected: {overall_metrics['total_products']}")
    logger.info(f"Total execution time: {total_time:.1f}s ({total_time/60:.1f} minutes)")

    if total_time > 0:
        logger.info(f"Overall speed: {overall_metrics['total_products']/total_time:.1f} products/sec")

    # Final failed URL count
    total_failed = sum(len(urls) for urls in failed_urls.values())
    if total_failed > 0:
        logger.info(f"{total_failed} URLs failed (check failed_urls.json)")
    else:
        logger.info("All URLs processed successfully!")

    # Save all results
    save_results()

if __name__ == "__main__":
    main()
    if failed_urls:
        logger.info("\n\nStarting aggressive retry on failed URLs...")
        time.sleep(5)  # Brief pause
        run_aggressive_retry()
