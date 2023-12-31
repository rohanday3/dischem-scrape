# Author: Rohan Dayaram
# Date: 1/11/2023
# Description: Web scraper for Dischem website
# https://github.com/rohanday3/dischem-scrape/
# Version: 3.0

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import aiohttp
import asyncio
import aiocells
import os
import hashlib
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm
import tqdm.asyncio as tqdm_asyncio

class DischemScraper:
    def __init__(self, prefered_categories=[], category_blacklist=[]):
        self.base_url = 'https://www.dischem.co.za'
        self.prefered_categories = prefered_categories
        self.categories = []
        self.cache_dir = 'product_links_cache'
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_links = True
        self.category_blacklist = category_blacklist

    async def get_category_links_async(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{self.base_url}/shop-by-department') as response:
                page = await response.text()
                soup = BeautifulSoup(page, 'html.parser')
                categories_div = soup.find('div', class_='sub-navigation sub-nav-desktop hidden-xs hidden-sm')
                categories_ul = categories_div.find('ul', class_='menu-items')
                categories_li = categories_ul.find_all('li', class_='menu-item')

                for category_li in categories_li:
                    category_link_a = category_li.find('a')
                    category_name = category_link_a.text
                    if category_name in self.category_blacklist:
                        continue
                    category_link = category_link_a['href']
                    self.categories.append({
                        'name': category_name,
                        'link': category_link,
                    })

    async def get_product_links_per_page_async(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()
        cache_file = os.path.join(self.cache_dir, f"{url_hash}.txt")

        if os.path.exists(cache_file):
            with open(cache_file, 'r') as file:
                products = [line.strip() for line in file.readlines()]
                products = [{
                    'name': product.split('/')[-1],
                    'link': product,
                } for product in products]
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    page = await response.text()
                    soup = BeautifulSoup(page, 'html.parser')
                    products_div = soup.find('div', class_='products wrapper grid products-grid')
                    products_ol = products_div.find('ol', class_='products list items product-items')
                    products_li = products_ol.find_all('li')
                    products = []

                    for product_li in products_li:
                        product_details_div = product_li.find('div', class_='product details product-item-details')
                        product_link_a = product_details_div.find('a', class_='product-item-link')
                        product_name = product_link_a.text
                        product_link = product_link_a['href']

                        products.append({
                            'name': product_name,
                            'link': product_link,
                        })

                    with open(cache_file, 'w') as file:
                        file.write('\n'.join([product['link'] for product in products]))
        return products

    async def get_all_product_links_threaded_async(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                page = await response.text()
                soup = BeautifulSoup(page, 'html.parser')
                index = soup.findAll('span', class_='toolbar-number')
                total_products = int(index[2].text)
                num_pages = total_products // 35 + 1
                products = []
                err_pages = []

                async def worker(page_number):
                    page_url = f'{url}?p={page_number}'
                    products_single_page = []
                    tries = 0
                    while not products_single_page and tries < 3:
                        products_single_page = await self.get_product_links_per_page_async(page_url)
                        tries += 1

                    if products_single_page:
                        products.extend(products_single_page)
                    else:
                        err_pages.append(page_url)

                tasks = [worker(page_number) for page_number in range(1, num_pages + 1)]
                await asyncio.gather(*tasks)

        return products, err_pages

    async def get_product_info_async(self, session, product, category_name):
        url = product['link']
        retry_count = 3
        for _ in range(retry_count):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        page = await response.text()
                        soup = BeautifulSoup(page, 'html.parser')
                        prod_info_div = soup.find('div', class_='product info detailed')
                        item_description_div_p = prod_info_div.find('div', class_='product attribute description')
                        item_description = item_description_div_p.find('div', class_='value').text if item_description_div_p else ''
                        item_info_table = prod_info_div.find('table', class_='data table additional-attributes')
                        item_schedule_p = item_info_table.find('th', string='Product Drugschedule')
                        item_nappi_p = item_info_table.find('th', string='Product Nappi Code')
                        item_barcode_p = item_info_table.find('th', string='Product Main Barcode')
                        item_schedule = item_schedule_p.find_next_sibling('td').text if item_schedule_p else ''
                        item_nappi = item_nappi_p.find_next_sibling('td').text if item_nappi_p else ''
                        item_barcode = item_barcode_p.find_next_sibling('td').text if item_barcode_p else ''
                        item_price_current_div = soup.find('div', {'data-price-type': 'finalPrice'})
                        item_price_current = item_price_current_div['data-price-amount']
                        item_price_old_div = soup.find('div', {'data-price-type': 'oldPrice'})
                        item_price_old = item_price_old_div['data-price-amount'] if item_price_old_div else ''
                        item_details = {
                            'description': item_description,
                            'schedule': item_schedule,
                            'nappi': item_nappi,
                            'barcode': item_barcode,
                            'current_price': item_price_current,
                            'normal_price': item_price_old,
                        }
                        return item_details
            except asyncio.TimeoutError:
                print(f"Timeout while fetching product info for {product['name']}, URL: {url}")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Error while fetching product info for {product['name']}, URL: {url}")
                print(e)
        return None

    async def get_all_product_info_async(self, url, category_name=''):
        products, err_pages = await self.get_all_product_links_threaded_async(url)

        async with aiohttp.ClientSession() as session:
            tasks = []
            progress_bar = tqdm_asyncio.tqdm(total=len(products), desc=f"Retrieving product info for {category_name}")

            async def track_progress():
                while len(product_info_list) < len(products):
                    progress_bar.n = len(product_info_list)
                    progress_bar.refresh()
                    await asyncio.sleep(1)

            progress_task = asyncio.ensure_future(track_progress())

            async def worker(product):
                product_info = await self.get_product_info_async(session, product, category_name)
                if product_info:
                    product.update(product_info)
                    product['category'] = category_name
                progress_bar.update(1)

            tasks = [worker(product) for product in products]
            await asyncio.gather(*tasks)
            progress_task.cancel()

        progress_bar.close()

        return products, err_pages


    async def scrape_categories_async(self):
        timeout = aiohttp.ClientTimeout(total=120)
        
        if self.prefered_categories:
            self.categories = [category for category in self.categories if category['name'] in self.prefered_categories]
        print('Total categories: ', len(self.categories))
        product_list = []
        err_info_list_all = []

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = []
            for category in self.categories:
                print('Category: ', category['name'], category['link'])
                task = asyncio.ensure_future(self.get_all_product_info_async(category['link'], category['name']))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

        for products, err_info_list in results:
            err_info_list_all.extend(err_info_list)
            if products:
                product_list.extend(products)

        return product_list, err_info_list_all

    def save_to_csv(self, products):
        products_df = pd.DataFrame(products)
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H-%M-%S")
        filename = 'dischem_products_' + dt_string + '.csv'
        products_df.to_csv(filename, index=False)

    def plot_runtimes(self, runtime_all_links, runtime_product_info):
        plt.plot(runtime_all_links)
        plt.plot(runtime_product_info)
        plt.title('Runtimes')
        plt.ylabel('Runtime (s)')
        plt.xlabel('Product')
        plt.legend(['All links', 'Product info'], loc='upper left')
        plt.show()

if __name__ == "__main__":
    async def main():
        scraper = DischemScraper(prefered_categories=['Health'], category_blacklist=['Brands A-Z','Beauty','Healthy Living','Hair Strategy','Gift Cards'])
        await scraper.get_category_links_async()
        product_list, err_prod_info_all = await scraper.scrape_categories_async()
        print('Total products downloaded: ', len(product_list))
        print('Total error products: ', len(err_prod_info_all))
        print('Error products: ')
        for err_prod_info in err_prod_info_all:
            print(err_prod_info['name'], err_prod_info['link'])
        scraper.save_to_csv(product_list)

    asyncio.run(main())
