import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm

class DischemScraper:
    def __init__(self, categories=[]):
        self.base_url = 'https://www.dischem.co.za'
        self.categories = []
        self.get_category_links()
        print('Total categories: ', len(self.categories))
        for category in self.categories:
            print(category['name'])
        if categories:
            self.categories = [category for category in self.categories if category['name'] in categories]

    def get_category_links(self):
        page = requests.get(f'{self.base_url}/shop-by-department')
        soup = BeautifulSoup(page.content, 'html.parser')
        categories_div = soup.find('div', class_='sub-navigation sub-nav-desktop hidden-xs hidden-sm')
        categories_ul = categories_div.find('ul', class_='menu-items')
        categories_li = categories_ul.find_all('li', class_='menu-item')

        for category_li in categories_li:
            category_link_a = category_li.find('a')
            category_name = category_link_a.text
            if category_name == 'Brands A-Z':
                continue
            category_link = category_link_a['href']
            self.categories.append({
                'name': category_name,
                'link': category_link,
            })

    def get_product_links_per_page(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
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

        return products

    def get_all_product_links_threaded(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        index = soup.findAll('span', class_='toolbar-number')
        total_products = int(index[2].text)
        num_pages = total_products // 35 + 1
        products = []
        err_pages = []

        def worker(page_number):
            page_url = f'{url}?p={page_number}'
            products_single_page = []
            tries = 0
            while not products_single_page and tries < 3:
                products_single_page = self.get_product_links_per_page(page_url)
                tries += 1

            if products_single_page:
                products.extend(products_single_page)
            else:
                err_pages.append(page_url)

        with ThreadPoolExecutor(max_workers=16) as executor:
            list(tqdm(executor.map(worker, range(1, num_pages + 1)), total=num_pages))

        return products, err_pages

    def get_product_info(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
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

    def get_all_product_info_threaded(self, url, category_name=''):
        products, err_pages = self.get_all_product_links_threaded(url)
        # print('Total products downloaded: ', len(products))
        # print('Total error pages: ', len(err_pages))
        product_info_list = []
        err_info_list = []

        def worker(product):
            try:
                product_info = self.get_product_info(product['link'])
            except:
                product_info = None
            tries = 0
            while not product_info and tries < 3:
                try:
                    product_info = self.get_product_info(product['link'])
                except:
                    product_info = None
                tries += 1
            if product_info:
                product.update(product_info)
                product['category'] = category_name
                product_info_list.append(product)
            else:
                err_info_list.append(product)

        with ThreadPoolExecutor(max_workers=16) as executor:
            list(tqdm(executor.map(worker, products), total=len(products)))

        return product_info_list, err_info_list

    def scrape_categories_threaded(self):
        print('Total categories: ', len(self.categories))
        product_list = []
        err_info_list_all = []

        def worker(category):
            print('Category: ', category['name'])
            products, err_info_list = self.get_all_product_info_threaded(category['link'], category['name'])
            err_info_list_all.extend(err_info_list)
            if products:
                product_list.extend(products)

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(tqdm(executor.map(worker, self.categories), total=len(self.categories)))

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
    scraper = DischemScraper(['Health'])
    product_list, err_prod_info_all = scraper.scrape_categories_threaded()
    print('Total products downloaded: ', len(product_list))
    print('Total error products: ', len(err_prod_info_all))
    print('Error products: ')
    for err_prod_info in err_prod_info_all:
        print(err_prod_info['name'], err_prod_info['link'])
    scraper.save_to_csv(product_list)
    # scraper.plot_runtimes(runtime_all_links, runtime_product_info)
