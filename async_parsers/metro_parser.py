import asyncio
import csv
import json
import time
from random import choice

import aiohttp
import requests
from aiohttp_retry import ExponentialRetry, RetryClient
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\nВремя выполнения программы: {execution_time} секунд.")
        return result

    return wrapper


class AsyncMetroScraper:
    BASE_URL = 'https://online.metro-cc.ru'

    def __init__(self, url):
        self.category_url = url
        self.products_data = []
        self.count_of_category_pages = None

    def write_to_file(self, output_format: str = 'csv'):
        """
        Writes product data to a json or csv file
        """
        output_format = output_format if output_format in ['csv', 'json'] else 'csv'

        if output_format == 'csv':
            with open('metro_products.csv', 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['id', 'name', 'regular_price', 'promo_price', 'brand', 'link']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in self.products_data:
                    writer.writerow(row)

        elif output_format == 'json':
            with open('metro_products.json', 'w', encoding='utf-8') as jsonfile:
                json.dump(self.products_data, jsonfile, ensure_ascii=False, indent=4)
        print(f"Запись данных в файл формата {output_format} произведена. "
              f"Количество товаров: {len(self.products_data)} шт.")

    def get_count_of_category_pages(self, url: str) -> int:
        """
        Processes the page, finds the paginated block and retrieves the last page number
        """
        response = requests.get(url=f"{self.BASE_URL}{url}")
        soup = BeautifulSoup(response.text, 'html.parser')
        return int(soup.find('ul', class_='catalog-paginate v-pagination').find_all('a')[-2].text)

    @staticmethod
    def get_product_price(product: BeautifulSoup) -> tuple:
        """
        Retrieves and returns the current price of a product and the old price if available
        """
        price_classes = ('product-unit-prices__actual-wrapper', 'product-unit-prices__old-wrapper')
        actual_price = old_price = None
        for indx, price_cls in enumerate(price_classes, start=1):
            product_price_block = product.find('div', class_=price_cls)
            price_rubles = product_price_block.find('span', class_='product-price__sum-rubles')
            price_pennies = product_price_block.find('span', class_='product-price__sum-penny')
            if indx == 1:
                actual_price = price_rubles.text + price_pennies.text if price_pennies else price_rubles.text
                actual_price = actual_price.replace('\xa0', ',')
            if indx == 2:
                old_price = f"{price_rubles.text}{price_pennies.text if price_pennies else ''}" if price_rubles else None
                old_price = old_price.replace('\xa0', ',') if old_price else None

        prices = (old_price, actual_price) if old_price else (actual_price, old_price)
        return prices

    def save_product_data(self, product_soup: BeautifulSoup, link: str, regular_price: str, promo_price: str):
        """
        Saves product data to the product_data list
        """
        product_id = product_soup.find('p', class_='product-page-content__article').text.strip().split()[-1]
        name = product_soup.find('h1', class_='product-page-content__product-name catalog-heading heading__h2').find(
            'span').text.strip()
        brand = product_soup.find('a',
                                  class_='product-attributes__list-item-link reset-link active-blue-text').text.strip()

        self.products_data.append({
            'id': product_id,
            'name': name,
            'regular_price': f'{regular_price} руб.',
            'promo_price': f'{promo_price} руб.' if promo_price else None,
            'brand': brand,
            'link': link,
        })

    async def get_page_data(self, session: aiohttp.ClientSession, link: str, number_of_page: int):
        """
        Retrieves all products from a page, then extracts data from each product
        """
        retry_options = ExponentialRetry(attempts=5)
        retry_client = RetryClient(raise_for_status=False, retry_options=retry_options, client_session=session,
                                   start_timeout=0.5)
        async with retry_client.get(link) as response:

            print(f'Извлечение данных со страницы №{number_of_page}')
            if response.ok:
                resp = await response.text()
                page_soup = BeautifulSoup(resp, 'lxml')

                products = page_soup.find_all(
                    'div',
                    class_='catalog-2-level-product-card product-card subcategory-or-type__products-item with-rating with-prices-drop'
                )

                for product in products:
                    product_href = product.find('a', class_='product-card-photo__link reset-link')['href']
                    product_url = self.BASE_URL + product_href

                    regular_price, promo_price = self.get_product_price(product)
                    async with session.get(url=product_url) as product_response:
                        product_response = await product_response.text()
                        product_soup = BeautifulSoup(product_response, 'lxml')
                        self.save_product_data(product_soup, product_url, regular_price, promo_price)
                print(f'Данные товаров со страницы №{number_of_page} извлечены')

    async def main(self):
        """
        In this function, a session is created using the aiohttp library, tasks are also created,
        a coroutine is wrapped in each task and the previously created session,
        a link to the category page and the page number are transferred to it.
        After tasks are created, the event loop starts.
        """
        ua = UserAgent()
        fake_ua = {'user-agent': ua.random}
        async with aiohttp.ClientSession(headers=fake_ua) as session:
            tasks = []
            for number_of_page in range(1, self.count_of_category_pages + 1):
                link = f'{self.BASE_URL}{category_url}&page={number_of_page}'
                task = asyncio.create_task(self.get_page_data(session, link, number_of_page))
                tasks.append(task)
            await asyncio.gather(*tasks)

    @timing_decorator
    def run_scraper(self):
        """
        Launches scraper
        """
        print('Определение количества страниц в категории...')
        self.count_of_category_pages = self.get_count_of_category_pages(self.category_url)
        print(f"Определено количество страниц ({self.count_of_category_pages})", end='\n\n')

        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # comment if you're using Unix or MacOS
        asyncio.run(self.main())

        print()
        self.write_to_file('csv')
        self.write_to_file('json')


if __name__ == "__main__":
    category_urls = {
        "Питьевая вода": '/category/bezalkogolnye-napitki/pityevaya-voda-kulery?in_stock=1',
        "Овощи": '/category/ovoshchi-i-frukty/ovoshchi?in_stock=1',
        "Молоко": '/category/molochnye-prodkuty-syry-i-yayca/moloko?in_stock=1',
        "Кофе": '/category/chaj-kofe-kakao/kofe?in_stock=1',
        "Колбасы": '/category/myasnye/kolbasy-vetchina?in_stock=1'
    }

    category_url = choice(list(category_urls.values()))  # select a random category
    metro_scraper = AsyncMetroScraper(category_url)
    metro_scraper.run_scraper()
