"""Get list of games with valuable data from www.metacritic.com.

"""

import asyncio
import csv
import logging
import random

import aiofiles
import aiohttp
import requests

from urllib.parse import urljoin
from typing import TextIO, Union

from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientHttpProxyError,
    ClientPayloadError,
    ServerDisconnectedError,
)
from aiohttp.web import HTTPForbidden
from bs4 import BeautifulSoup
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError

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
        'summary', 'genres', 'number_of_players', 'rating', 'metascore',
        'number_of_critic_reviews', 'user_score', 'number_of_user_ratings',
        'awards_and_ranking',
    ]
    BASE_URL = 'https://www.metacritic.com/'

    def __init__(self) -> None:
        self.queue = None
        self.max_requests = asyncio.Semaphore(CONCURRENT_REQUESTS)

    def _get_headers(self):
        """Returns Chrome/Safari/Brave request headers dict.
        
        """
        return random.choice(HEADERS)

    def parse_list_page(self, url: str) -> tuple[list, Union[str, None]]:
        """ Gets links from list page.

        Parameters
        ----------
        url : str
            An url of the page.

        Returns
        -------
        tuple[list, Union[str, None]]
            Links to write into file; url of the next page (if any).
        """

        links, next_page_url = [], None
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=180)
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.select(
                'div#main_content div.browse_list_wrapper table.clamp-list '
                'tr.expand_collapse td.details a.title')
            links = [[a['href'],] for a in links]
            next_page_el = soup.select_one('div.page_nav div.page_flipper span.flipper.next a')
            next_page_url = urljoin(self.BASE_URL, next_page_el['href']) if next_page_el else None
        except (
            ConnectionError,
            RemoteDisconnected,
        ) as http_error:
            logging.error("Connection error occurred: %s", http_error)
            return links, url
        except Exception:
            logging.exception("An error occurred due to getting links from page %s", url)
        return links, next_page_url

    def get_links(self, url: str) -> None:
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
        while url:
            links, url = self.parse_list_page(url)
            with open(self.LINKS_FILE, 'a', encoding='utf-8') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerows(links)
        logging.info("Links were collected.")

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

        soup = BeautifulSoup(content, 'html.parser')
        name = soup.select_one('div.product_title a h1').text
        release_date = soup.select_one('div.product_data ul.summary_details '
                                       'li.release_data span.data').text
        developer = soup.select_one('div.side_details ul.summary_details '
                                    'li.developer span.data')
        developer = developer.get_text(strip=True) if developer else None
        platform = soup.select_one('div.product_title span.platform').get_text(strip=True)
        additional_platforms = soup.select_one(
            'div.product_data ul.summary_details '
            'li.product_platforms span.data')
        additional_platforms = additional_platforms.get_text(strip=True)\
            if additional_platforms else None
        summary = soup.select_one('div.product_details ul.summary_details '
                                  'li.product_summary span.data span.blurb_expanded')
        if summary is None:
            summary = soup.select_one('div.product_details ul.summary_details '
                                      'li.product_summary span.data span')
        summary = summary.get_text(strip=True) if summary else None
        genres = ','.join((
            span.text.strip() for span
            in soup.select('div.side_details ul.summary_details '
                           'li.product_genre span.data')
        ))
        number_of_players = soup.select_one('div.side_details ul.summary_details '
                                            'li.product_players span.data')
        number_of_players = number_of_players.text.strip() if number_of_players else None
        rating = soup.select_one('div.side_details ul.summary_details '
                                 'li.product_rating span.data')
        rating = rating.text.strip() if rating else None
        metascore = soup.select_one('div.metascore_summary.metascore_summary div.metascore_w span')
        metascore = metascore.text.strip() if metascore else None
        number_of_critic_reviews = soup.select_one('div.metascore_summary div.metascore_wrap '
                                                   'div.summary span.count a span').text.strip()
        user_score = soup.select_one('div.score_summary div.userscore_wrap  div.metascore_w')
        user_score = user_score.text.strip() if user_score else None
        number_of_user_ratings = soup.select_one('div.score_summary div.userscore_wrap '
                                                 'div.summary span.count a')
        number_of_user_ratings = number_of_user_ratings.text.strip()\
            if number_of_user_ratings else None
        awards_and_ranking = ','.join((
            a.text for a in soup.select('table.rankings td.ranking_wrap div.ranking_title a')
        ))
        result = [
            name, release_date, developer, platform, additional_platforms,
            summary, genres, number_of_players, rating, metascore,
            number_of_critic_reviews, user_score, number_of_user_ratings,
            awards_and_ranking,
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


async def runner():
    """
    Runs scraper:
    1. fill in file with game details page links;
    2. get data about every game.
    """

    crawler = GamesScraper()
    crawler.get_links(
        'https://www.metacritic.com/browse/games/score/'
        'metascore/all/all/filtered?view=condensed')
    await crawler.get_data()


if __name__ == "__main__":
    asyncio.run(runner())
