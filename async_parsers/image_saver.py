"""
This script asynchronously downloads and saves 449 images and displays their size.
For correct operation, create a folder in the project root called 'images'.
!!! If you are using Windows OS uncomment line 42 !!!
"""

import time
import aiofiles
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os


async def save_jpg(session, url, name_img):
    async with aiofiles.open(f'../images/{name_img}', mode='wb') as f:
        async with session.get(url) as response:
            async for x in response.content.iter_chunked(1024):
                await f.write(x)
        print(f'Изображение {name_img} сохранено')


async def main():
    url = 'https://parsinger.ru/asyncio/aiofile/2/index.html'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
            links = [f'https://parsinger.ru/asyncio/aiofile/2/{x["href"]}' for x in soup.find_all('a')]
            tasks = []
            for link in links:
                async with session.get(link) as response2:
                    soup2 = BeautifulSoup(await response2.text(), 'lxml')
                    img_links = [img["src"] for img in soup2.find_all('img')]
                    for img_link in img_links:
                        name_img = img_link.split('/')[-1]
                        task = asyncio.create_task(save_jpg(session, img_link, name_img))
                        tasks.append(task)
            await asyncio.gather(*tasks)


start = time.perf_counter()
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # uncomment if you are using Windows OS
asyncio.run(main())
print(f'\nCохранено {len(os.listdir("../images/"))} изображений за {round(time.perf_counter() - start, 3)} сек')


def get_folder_size(filepath="../images/", size=0):
    for root, dirs, files in os.walk(filepath):
        for f in files:
            size += os.path.getsize(os.path.join(root, f))
    return size


print(f'Общий размер изображений: {get_folder_size()} bytes')
