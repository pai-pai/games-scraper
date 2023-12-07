"""Get list of games with valuable data from www.metacritic.com.

"""

import asyncio
import csv
import logging
import random
import re
import time

import aiofiles
import aiohttp
import requests
import hrequests
import pandas as pd

from urllib.parse import urljoin
from typing import TextIO, Union

from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientHttpProxyError,
    ClientPayloadError,
    ServerDisconnectedError,
)
from aiohttp.web import HTTPForbidden
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError
from selectolax.parser import HTMLParser

from constants import (
    CHUNK_SIZE,
    CONCURRENT_REQUESTS,
    HEADERS,
    REQUEST_DELAY,
)
from helpers import to_csv_string


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log',
    filemode='w',
)


class GamesScraper():
    """Scrapes list of details page links of every game
    and a data from every details page.
    """

    LINKS_FILE = 'games_links.csv'
    DATA_FILE = 'games.csv'
    DATA_FIELDS = [
        'link', 'name', 'release_date', 'developer', 'platform', 'additional_platforms',
        'summary', 'genres', 'rating', 'metascore',
        'number_of_critic_reviews', 'user_score', 'number_of_user_ratings',
    ]
    BASE_URL = 'https://www.metacritic.com/'

    def __init__(self) -> None:
        self.queue = None
        self.max_requests = asyncio.Semaphore(CONCURRENT_REQUESTS)
        self.unprocessed_links = set()

    def _get_headers(self):
        """Returns Chrome/Safari/Brave request headers dict.
        
        """
        return random.choice(HEADERS)

    def parse_list_page(self, page_number: int=1) -> tuple[list, Union[int, None]]:
        """ Gets links from list page.

        Parameters
        ----------
        page_number : int
            Page number.

        Returns
        -------
        tuple[list, Union[int, None]]
            Links to write into file; Number of the next page (if any).
        """

        links, next_page = [], None
        list_page_url = 'https://www.metacritic.com/browse/game/'\
            f'?releaseYearMin=1910&releaseYearMax=2023&page={page_number}'
        try:
            logging.info("Go to page number %s: %s", page_number, list_page_url)
            response = requests.get(list_page_url, headers=self._get_headers(), timeout=180)
            page_tree = HTMLParser(response.content)
            links = [[node.attributes.get('href'),] for node in
                     page_tree.css('a.c-finderProductCard_container')]
            if page_tree.css_first('span.c-navigationPagination_item--next.enabled') is not None:
                next_page = page_number + 1
        except (
            ConnectionError,
            RemoteDisconnected,
        ) as http_error:
            logging.error("Connection error occurred: %s", http_error)
            return links, next_page
        except Exception:
            logging.exception("An error occurred due to getting links from page %s", list_page_url)
        return links, next_page

    def get_links(self, page_number: int=1) -> None:
        """Gets all links from 'game' category.

        Parameters
        ----------
        url : str
            Starting url.

        Returns
        -------
        None
        """

        logging.info("Getting links.")
        open(self.LINKS_FILE, 'w', encoding='utf-8').close()
        while page_number:
            links, page_number = self.parse_list_page(page_number)
            with open(self.LINKS_FILE, 'a', encoding='utf-8') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerows(links)
        logging.info("Links were collected.")

    def extract_text(self, node):
        return node.text(strip=True) if node else None

    def parse_details_page(self, content: str) -> list[str]:
        """Gets data about game from details page.

        Parameters
        ----------
        content : str
            Page content.

        Returns
        -------
        list[str]
            Game details as a list.
        """

        page_tree = HTMLParser(content)
        name = page_tree.css_first('meta[property="og:title"]')
        if name is not None:
            name = name.attributes.get('content')
        else:
            name = self.extract_text(page_tree.css_first('.c-productHero_title'))
        release_date = self.extract_text(
            page_tree.css_first('.c-productHero_score-container .g-text-xsmall .u-text-uppercase')
        )
        developer = self.extract_text(
            page_tree.css_first('.c-gameDetails .c-gameDetails_Developer ul li')
        )
        platform = self.extract_text(
            page_tree.css_first('.c-ProductHeroGamePlatformInfo .c-gamePlatformLogo')
        )
        additional_platforms = page_tree.css(
            '.c-gameDetails_sectionContainer .c-gameDetails_Platforms ul li'
        ) or []
        additional_platforms = ', '.join((node.text(strip=True) for node in additional_platforms))
        summary = page_tree.css_first('meta[name="description"]').attributes.get('content')
        summary = summary.replace('\n', ' ')
        genres = page_tree.css('.c-gameDetails_sectionContainer ul.c-genreList li') or []
        genres = ', '.join((node.text(strip=True) for node in genres))
        rating = self.extract_text(
            page_tree.css_first('.c-productionDetailsGame_esrb_title span.u-block')
        )
        rating = rating.replace('Rated ', '') if rating is not None else None
        metascore = self.extract_text(
            page_tree.css_first(
                '.c-reviewsSection_criticReviews .c-reviewsOverview '
                '.c-ScoreCard_scoreContent > a > div > div > span'
            )
        )
        number_of_critic_reviews = self.extract_text(
            page_tree.css_first(
                '.c-reviewsSection_criticReviews .c-reviewsOverview '
                'span.c-ScoreCard_reviewsTotal'
            )
        )
        number_of_critic_reviews = re.sub(r'\D', '', number_of_critic_reviews) \
            if number_of_critic_reviews else None
        user_score = self.extract_text(
            page_tree.css_first(
                '.c-reviewsSection_userReviews .c-reviewsOverview '
                '.c-ScoreCard_scoreContent > a > div > div > span'
            )
        )
        number_of_user_ratings = self.extract_text(
            page_tree.css_first(
                '.c-reviewsSection_userReviews .c-reviewsOverview '
                'span.c-ScoreCard_reviewsTotal'
            )
        )
        number_of_user_ratings = re.sub(r'\D', '', number_of_user_ratings) \
            if number_of_user_ratings else None

        result = [
            name, release_date, developer, platform, additional_platforms,
            summary, genres, rating, metascore, number_of_critic_reviews,
            user_score, number_of_user_ratings,
        ]
        return result

    async def _get_details(self, url: str, session: aiohttp.ClientSession,
                           output_file: TextIO) -> None:
        """Collects game data from details page and writes it to file.

        Parameters
        ----------
        url : str
            Page content.
        session : aiohttp.ClientSession
            Opened session.
        output_file : TextIO
            File to write game comprehensive data.

        Returns
        -------
        None
        """

        try:
            async with self.max_requests:
                if self.max_requests.locked():
                    logging.debug("Max concurrent requests number have been reached. Waiting...")
                await asyncio.sleep(random.randint(0, REQUEST_DELAY))
                async with session.get(
                    url, headers=self._get_headers(), timeout=180
                ) as response:
                    logging.debug(response)
                    if response.status == 403:
                        raise HTTPForbidden
                    if response.status == 404:
                        logging.warning("Link %s is not valid.", url)
                        game_data = [None] * (len(self.DATA_FIELDS) - 1)
                    else:
                        game_data = self.parse_details_page(await response.content.read())
                    game_row = [url] + game_data
                    logging.debug(game_row)
                    await output_file.write(to_csv_string(game_row))
        except (
            ClientConnectorError,
            ClientHttpProxyError,
            ClientPayloadError,
            HTTPForbidden,
            ServerDisconnectedError,
        ) as http_error:
            logging.error("Connection error occurred: %s", http_error)
            logging.debug("Put link %s back into queue.", url)
            await self.queue.put(url)
        except Exception:
            logging.exception("An error occurred due to getting data for %s:", url)

    async def worker(self, session: aiohttp.ClientSession, output_file: TextIO) -> None:
        """Worker which is process an url.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Opened session.
        output_file : TextIO
            File to write game data.

        Returns
        -------
        None
        """

        while True:
            url = await self.queue.get()
            logging.debug("Got link %s" % url)
            await self._get_details(url, session, output_file)
            self.queue.task_done()

    async def get_data(self):
        """Collects data for every game page which link is presented
        in previously populated file.
        """

        logging.info("Getting data process has started.")
        with open(self.DATA_FILE, 'w', encoding='utf-8') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(self.DATA_FIELDS)
        self.queue = asyncio.Queue(CHUNK_SIZE)
        async with aiohttp.ClientSession() as session:
            async with (
                aiofiles.open(self.LINKS_FILE, mode='r',
                              encoding='utf-8', newline='') as links_file,
                aiofiles.open(self.DATA_FILE, mode='a',
                              encoding='utf-8') as data_file
            ):
                tasks = [asyncio.create_task(self.worker(session, data_file))
                         for _ in range(CHUNK_SIZE)]
                async for url in links_file:
                    url = urljoin(self.BASE_URL, url.rstrip())
                    logging.debug("Put link %s into queue.", url)
                    await self.queue.put(url)
                await self.queue.join()
        for task in tasks:
            task.cancel()
        logging.info("Data extraction is finished.")

    def get_chunk(self):
        for chunk in pd.read_csv(self.LINKS_FILE, chunksize=CONCURRENT_REQUESTS,
                                 header=None, names=['link']):
            chunk['link'] = chunk['link'].apply(lambda x: urljoin(self.BASE_URL, x))
            yield list(chunk['link'])

    def exception_handler(self, request, exception):
        url = request.url
        logging.error("An error occurred due to getting data from %s for %s time",
                      url, 2 if url in self.unprocessed_links else 1)
        logging.error(exception)
        self.unprocessed_links.add(url)

    def process_links(self, links):
        processed_rows = []
        reqs = [hrequests.async_get(link, timeout=60) for link in links]
        for resp in hrequests.imap(
            reqs, size=len(links), exception_handler=self.exception_handler
        ):
            logging.info('Collect data from %s', resp.url)
            game_data = self.parse_details_page(resp.content)
            processed_rows.append([resp.url] + game_data)
        return processed_rows

    def new_get_data(self):
        logging.info("Getting data process has started.")
        with open(self.DATA_FILE, 'w', encoding='utf-8') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(self.DATA_FIELDS)
            for links in self.get_chunk():
                csv_writer.writerows(self.process_links(links))
                time.sleep(random.randint(1,3))

        # Process failed links
        while self.unprocessed_links:
            unprocessed_links = list(self.unprocessed_links)
            links = unprocessed_links[:CONCURRENT_REQUESTS]
            csv_writer.writerows(self.process_links(links))
            self.unprocessed_links = set(unprocessed_links[CONCURRENT_REQUESTS:])
        logging.info("Data extraction is finished.")


async def runner():
    """
    Runs scraper:
    1. fill in file with game details page links;
    2. get data about every game.
    """

    crawler = GamesScraper()
    #crawler.get_links()
    await crawler.get_data()

def new_runner():
    crawler = GamesScraper()
    #crawler.get_links()
    crawler.new_get_data()


if __name__ == "__main__":
    #asyncio.run(runner())
    new_runner()
