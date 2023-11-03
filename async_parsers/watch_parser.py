import json

import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
from aiohttp_retry import RetryClient, ExponentialRetry
from fake_useragent import UserAgent

domain = 'https://parsinger.ru/html/'


def get_soup(url):
    resp = requests.get(url=url)
    return BeautifulSoup(resp.text, 'lxml')


def get_page_urls(soup):
    return [domain + pagen['href'] for pagen in soup.find('div', class_='pagen').find_all('a')]


def save_product_data(soup, link):
    item_descr = soup.find('div', class_='description')

    title = item_descr.find(id='p_header').text
    article = item_descr.find('p', class_='article').text.split()[1]
    brand, model, tp, display, material_frame, material_bracer, size, site = tuple(
        map(lambda tag: tag.text.split(': ')[1], item_descr.find_all('li'))
    )
    in_stock = item_descr.find(id='in_stock').text.split(': ')[1]
    price = item_descr.find(id='price').text
    old_price = item_descr.find(id='old_price').text
    result_json.append({
        'title': title, "article": int(article), 'brand': brand, 'model': model, 'tp': tp,
        'display': display, 'material_frame': material_frame, 'material_bracer': material_bracer,
        'size': size, 'site': site, 'in_stock': int(in_stock), 'price': price, 'old_price': old_price,
        'url': link
    })


async def get_data(session, link):
    retry_options = ExponentialRetry(attempts=5)
    retry_client = RetryClient(raise_for_status=False, retry_options=retry_options, client_session=session,
                               start_timeout=0.5)
    async with retry_client.get(link) as response:
        if response.ok:
            resp = await response.text()
            page_soup = BeautifulSoup(resp, 'lxml')
            item_cards = [item_card['href'] for item_card in page_soup.find_all('a', class_='name_item')]
            for item_card_href in item_cards:
                item_url = domain + item_card_href
                async with session.get(url=item_url) as item_response:
                    item_resp = await item_response.text()
                    item_soup = BeautifulSoup(item_resp, 'lxml')
                    save_product_data(item_soup, link)


async def main():
    ua = UserAgent()
    fake_ua = {'user-agent': ua.random}
    async with aiohttp.ClientSession(headers=fake_ua) as session:
        tasks = []
        for link in page_urls:
            task = asyncio.create_task(get_data(session, link))
            tasks.append(task)
        await asyncio.gather(*tasks)


start_url = 'https://parsinger.ru/html/index1_page_1.html'
result_json = list()

soup = get_soup(start_url)
page_urls = get_page_urls(soup)

# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # uncomment if you are using Windows OS
asyncio.run(main())

with open('watches.json', 'w', encoding='utf-8') as file:
    json.dump(result_json, file, indent=4, ensure_ascii=False)
print('Файл watches.json создан')
