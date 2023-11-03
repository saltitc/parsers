"""
This script asynchronously downloads and saves 2616 images and displays their size.
The speed of the script depends on your Internet connection, but on average it takes 5 minutes to complete.
For correct operation, create a folder in the project root called 'image_folder_2'.
!!! If you are using Windows OS uncomment line 55 !!!
"""

import time
import aiofiles
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os


async def save_jpg(session, url, semaphore, counter=[0]):
    async with semaphore:
        async with aiofiles.open(f'../image_folder_2/{url.split("/")[-1]}', mode='wb') as f:
            async with session.get(url) as response:
                async for x in response.content.iter_chunked(1024):
                    await f.write(x)
    counter[0] += 1
    print(f'Сохранено {counter[0]} изображений...') if counter[0] % 100 == 0 else None


async def main():
    schema = 'https://parsinger.ru/asyncio/aiofile/3/'
    async with aiohttp.ClientSession() as session:
        async with session.get(schema) as response:
            print('Сбор ссылок на изображения...\n')
            soup = BeautifulSoup(await response.text(), 'lxml')
            all_pages = [f'{schema}{x["href"]}' for x in soup.find_all('a')]

            image_links = set()
            pages_with_images = []

            for category_link in all_pages:
                async with session.get(category_link) as category_response:
                    category_soup = BeautifulSoup(await category_response.text(), 'lxml')
                    pages_with_images.extend([f'{schema}depth2/{x["href"]}' for x in
                                              category_soup.find_all('a')])

            for page_with_images in pages_with_images:
                async with session.get(page_with_images) as images_response:
                    images_soup = BeautifulSoup(await images_response.text(), 'lxml')
                    image_links.update([img["src"] for img in images_soup.find_all('img')])

            print('Ссылки на изображения получены. Началась загрузка...')
            semaphore = asyncio.Semaphore(100)
            tasks = [save_jpg(session, img_link, semaphore) for img_link in image_links]
            await asyncio.gather(*tasks)


start = time.perf_counter()
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # uncomment if you are using Windows OS
asyncio.run(main())
print(f'\nCохранено {len(os.listdir("../image_folder_2/"))} изображений за {round(time.perf_counter() - start, 2)} сек.')


def get_folder_size(filepath="../image_folder_2/", size=0):
    for root, dirs, files in os.walk(filepath):
        for f in files:
            size += os.path.getsize(os.path.join(root, f))
    return size


print('\nРасчет общего размера изображений...')
print(f'Общий размер изображений: {get_folder_size()} bytes')
