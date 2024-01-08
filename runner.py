"""
Get list of games with its data from www.metacritic.com.
"""
import csv
import json
import logging
import queue
import re
import threading

from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs

import _csv
import chompjs
import hrequests

from hrequests.exceptions import ClientException
from selectolax.parser import HTMLParser, Node

from constants import (
    CHUNK_SIZE,
    CONCURRENT_REQUESTS,
    NUMBER_OF_CONSUMERS,
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log',
    filemode='w',
)


BASE_URL = 'https://www.metacritic.com/'
LIST_URL = 'https://www.metacritic.com/browse/game/all/all/all-time/new/?'\
    f'releaseYearMin=1970&releaseYearMax={datetime.now().year}&page='
LIST_URL_BY_YEAR = 'https://www.metacritic.com/browse/game/all/all/{}/new/?page='
DATA_FILE = 'games/games.csv'
DATA_FILE_BY_YEAR = 'games/games_{}.csv'
DATA_FIELDS = [
    'link', 'name', 'developer', 'publisher', 'summary', 'genres', 'rating',
    'platform', 'release_date',
    'metascore', 'critic_reviews_count',
    'positive_critic_reviews_count', 'mixed_critic_reviews_count', 'negative_critic_reviews_count',
    'user_score', 'user_reviews_count',
    'positive_user_reviews_count', 'mixed_user_reviews_count', 'negative_user_reviews_count',
]


class DataExtractor():
    """
    A class to collect games data from metacritic.com.

    Parameters
    ----------
    year : Union[int, None]
        Year to collect games data.
        If 'None' data will be collected from 1970 to current year.
    starting_page : int
        Number of the first page to extract data.

    Attributes
    ----------
    requests_limiter : threading.Semaphore
        Semaphore for controlling concurrent requests.
    producer_stopped : threading.Event
        Event signaling the stopping of the links producer.
    writer_lock : threading.Lock
        Lock for ensuring thread-safe writing to CSV.
    can_get_link : threading.Condition
        Condition variable for coordinating link retrieval.
    queues : dict
        Dictionary of queues for different types of pages.
    data_file : str
        File path for storing extracted data.
    list_page_url : str
        URL template for list pages.

    Methods
    -------
    __init__(year=None, starting_page=1):
        Constructs all the necessary attributes for the extractor.

    run(continue_collecting=False):
        Initiates the data extraction process.

    links_producer():
        Collects game links from list pages and adds them to the 'list_pages' queue
        for processing by the 'links_consumer'.

    links_consumer(writer):
        Consumes game links from the list_pages queue, extracts game data,
        and writes it to a CSV file.

    collect_links_from_page():
        Collects game links from a list page.

    get_game_data(url):
        Retrieves and processes game data from a game page.

    get_reviews_data(response):
        Extracts and returns user reviews data from a response.

    extract_text(node):
        Extracts text content from an HTML node.

    cut_excess(value):
        Removes excess characters from a given value.

    extract_json(response):
        Extracts game data from script tag in format of JSON list.
    """

    def __init__(self, year: int | None = None, starting_page: int = 1) -> None:
        """
        Constructs all the necessary attributes for the extractor.

        Parameters
        ----------
        year : int | None
            Year to collect games data.
            If 'None', data will be collected from 1970 to the current year.
        starting_page : int
            Number of the first page to extract data.
        """
        self.requests_limiter = threading.Semaphore(CONCURRENT_REQUESTS)
        self.producer_stopped = threading.Event()
        self.writer_lock = threading.Lock()
        self.can_get_link = threading.Condition()
        self.queues = {
            'list_pages': queue.Queue(),
            'fail_pages': queue.Queue(),
            'game_pages': queue.Queue(maxsize=CHUNK_SIZE),
        }

        self.data_file = DATA_FILE_BY_YEAR.format(year) if year else DATA_FILE
        self.list_page_url = LIST_URL_BY_YEAR.format(year) if year else LIST_URL
        self.queues['list_pages'].put(f'{self.list_page_url}{starting_page}')

    def run(self, continue_collecting: bool = False) -> None:
        """
        Initiates the data extraction process.

        Parameters
        ----------
        continue_collecting : bool, optional
            Whether to continue adding data to the data file (default is False).
        """
        file_mode = 'a' if continue_collecting else 'w'
        with open(self.data_file, file_mode, encoding='utf-8') as data_file:
            csv_writer = csv.writer(data_file)
            if not continue_collecting:
                csv_writer.writerow(DATA_FIELDS)
            threads = [
                threading.Thread(target=self.links_producer)
            ]
            for i in range(NUMBER_OF_CONSUMERS):
                threads.append(
                    threading.Thread(
                        target=self.links_consumer,
                        name=f'LINKS_CONSUMER_{i}',
                        args=(csv_writer,)
                    )
                )
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        with open('unprocessed_games.csv', file_mode, encoding='utf-8') as unprocessed_links_file:
            csv_writer = csv.writer(unprocessed_links_file)
            while not self.queues['fail_pages'].empty():
                csv_writer.writerow(self.queues['fail_pages'].get())

    def links_producer(self) -> None:
        """
        Collects game links from list pages and adds them to the 'list_pages' queue
        for processing by the 'links_consumer'.
        """
        logging.info('Getting links.')
        while not self.queues['list_pages'].empty():
            links = self.collect_links_from_page()
            for link in links:
                with self.can_get_link:
                    self.queues['game_pages'].put(urljoin(BASE_URL, link))
                    self.can_get_link.notify_all()
        logging.info('All links were collected and added into queue.')
        self.producer_stopped.set()

    def links_consumer(self, writer: '_csv._writer') -> None:
        """
        Consumes game links from the list_pages queue, extracts game data,
        and writes it to a CSV file.

        Parameters
        ----------
        writer : csv.writer
            CSV writer object for writing game data.
        """
        while True:
            while self.queues['game_pages'].empty() and not self.producer_stopped.is_set():
                with self.can_get_link:
                    logging.debug('%s: waiting for link...', threading.current_thread().name)
                    self.can_get_link.wait(timeout=30)
            if self.queues['game_pages'].empty() and self.producer_stopped.is_set():
                break
            url = self.queues['game_pages'].get()
            game_data = self.get_game_data(url)
            if game_data:
                with self.writer_lock:
                    writer.writerows(game_data)
        logging.info('%s: queue has been exhausted.', threading.current_thread().name)

    def collect_links_from_page(self) -> list[str]:
        """
        Collects game links from a list page.

        Returns
        -------
        list[str]
            list of game links.
        """
        links: list[str] = []
        page_url = self.queues['list_pages'].get()
        page_number = parse_qs(urlparse(page_url).query).get('page')
        if page_number is None:
            return links
        page_number = page_number[0]
        logging.debug('Go to page number %s: %s', page_number, page_url)
        response = self._send_request(page_url, is_list_page=True)
        if response is not None:
            page_tree = HTMLParser(response.content)
            links = (node.attributes.get('href') for node in
                     page_tree.css('a.c-finderProductCard_container'))
            if page_tree.css_first('span.c-navigationPagination_item--next.enabled') is not None:
                self.queues['list_pages'].put(f'{self.list_page_url}{int(page_number) + 1}')
        return links

    def get_game_data(self, url: str) -> list[list[str | int | float | None]] | None:
        """
        Retrieves and processes game data from a game page.

        Parameters
        ----------
        url : str
            URL of the game page.

        Returns
        -------
        list[list[str | int | float | None]] | None
            list of csv-ready rows of game data (a row per platform).
        """
        logging.debug('Go to game page: %s', url)
        response = self._send_request(url)
        if response is None:
            return None
        results = []
        json_data = self.extract_json(response)
        if json_data is None:
            return None
        item_data = json_data[0].get('item')
        if item_data is None:
            return None
        page_tree = HTMLParser(response.content)
        title = self.cut_excess(item_data.get('title'))
        if title is None:
            title_meta_node = page_tree.css_first('meta[property="og:title"]')
            title_node = page_tree.css_first('.c-productHero_title')
            title = title_meta_node.attributes.get('content') if title_meta_node is not None else \
                self.extract_text(title_node)
        description = self.cut_excess(item_data.get('description'))
        companies = item_data.get('production', {}).get('companies', [])
        developer, publisher = None, None
        for company in companies:
            if company.get('typeName') == 'Developer':
                developer = self.cut_excess(company.get('name'))
            if company.get('typeName') == 'Publisher':
                publisher = self.cut_excess(company.get('name'))
        main_data = {
            'link': url,
            'name': title,
            'developer': developer,
            'publisher': publisher,
            'summary': description.replace('\n', ' ').replace('\r', '') \
                if isinstance(description, str) else None,
            'genres': ', '.join((genre.get('name') for genre in item_data.get('genres', []))),
            'rating': self.cut_excess(item_data.get('rating')),
        }
        main_release_date = self.cut_excess(item_data.get('releaseDate'))
        user_reviews_section = page_tree.css_first(
            '.c-reviewsSection_userReviews .c-ScoreCard_scoreContent_number'
        )
        for item_platform_data in item_data.get('platforms', []):
            game_data = dict(main_data)
            critic_reviews_data = item_platform_data.get('criticScoreSummary', {})
            game_data.update({
                'platform': item_platform_data.get('name'),
                'release_date': self.cut_excess(item_platform_data.get('releaseDate')) or
                    main_release_date,
                'metascore': self.cut_excess(critic_reviews_data.get('score')),
                'critic_reviews_count': self.cut_excess(
                    critic_reviews_data.get('reviewCount')),
                'positive_critic_reviews_count': self.cut_excess(
                    critic_reviews_data.get('positiveCount')),
                'mixed_critic_reviews_count': self.cut_excess(
                    critic_reviews_data.get('neutralCount')),
                'negative_critic_reviews_count': self.cut_excess(
                    critic_reviews_data.get('negativeCount')),
            })
            critic_reviews_url = critic_reviews_data.get('url')
            if critic_reviews_url is not None and user_reviews_section is not None:
                user_reviews_url = urljoin(
                    BASE_URL,
                    critic_reviews_url.replace('/critic-reviews/', '/user-reviews/')
                )
                logging.debug('Go to user reviews page (platform %s): %s',
                              game_data['platform'], user_reviews_url)
                user_reviews_response = self._send_request(user_reviews_url, base_url=url)
                if user_reviews_response:
                    user_reviews_data = self.get_reviews_data(user_reviews_response)
                    if user_reviews_data:
                        game_data.update({
                            'user_score': user_reviews_data['score'],
                            'user_reviews_count': user_reviews_data['reviews'],
                            'positive_user_reviews_count': user_reviews_data['positive'],
                            'mixed_user_reviews_count': user_reviews_data['mixed'],
                            'negative_user_reviews_count': user_reviews_data['negative'],
                        })
            results.append([game_data.get(field_name) for field_name in DATA_FIELDS])
        return results

    def get_reviews_data(
        self, response: hrequests.Response
    ) -> dict[str , str | int | float | None] | None:
        """
        Extracts and returns user reviews data from a response.

        Parameters
        ----------
        response : hrequests.Response
            HTTP response object.

        Returns
        -------
        dict[str, str | int | float | None] | None
            Dictionary containing review data or None if extraction fails.
        """
        if response is None:
            return None
        json_data = self.extract_json(response)
        if json_data is None:
            return None
        item_data = json_data[1].get('item')
        if item_data is None:
            return None
        return {
            'score': self.cut_excess(item_data.get('score')),
            'reviews': self.cut_excess(item_data.get('reviewCount')),
            'positive': self.cut_excess(item_data.get('positiveCount')),
            'mixed': self.cut_excess(item_data.get('neutralCount')),
            'negative': self.cut_excess(item_data.get('negativeCount')),
        }

    def _send_request(
        self, url: str, base_url: str | None = None, is_list_page: bool = False
    ) -> hrequests.Response | None:
        """
        Sends an HTTP request and handles exceptions.

        Parameters
        ----------
        url : str
            URL to send the request to.
        base_url : str, optional
            Base game URL to put into fail_pages queue
            if game reviews page request failed (default is None).
        is_list_page : bool, optional
            Whether the request is for a list page (default is False).
            To determine the queue in which the failed request's URL should be placed.

        Returns
        -------
        hrequests.Response | None
            HTTP response object or None if the request fails.
        """
        response = None
        fail_queue = self.queues['list_pages'] if is_list_page else self.queues['game_pages']
        try:
            self.requests_limiter.acquire(timeout=65)
            response = hrequests.get(url, timeout=60)
        except ClientException:
            logging.exception(
                'An error occurred due to getting game '
                '%s from url %s', 'links' if is_list_page else 'data', url
            )
            fail_queue.put(base_url or url)
        except Exception:
            logging.exception('Unexpected exception with getting %s', url)
        finally:
            self.requests_limiter.release()
        if response is None:
            return None
        if not response.content:
            logging.warning('There is no content (url: %s status code: %s)',
                            url, response.status_code)
            fail_queue.put(base_url or url)
            return None
        return response

    def extract_text(self, node: Node) -> str | None:
        """
        Extracts text content from an HTML node.

        Parameters
        ----------
        node : selectolax.parser.Node
            Selectolax node object.

        Returns
        -------
        str | None
            Text content or None if the node is None.
        """
        return node.text(strip=True) if node else None

    def cut_excess(self, value: str | int | float | None) -> str | int | float | None:
        """
        Removes excess characters from a given value.

        Parameters
        ----------
        value : str | int | float | None
            Value to process.

        Returns
        -------
        str | int | float | None
            Processed value or None if the input is None.
        """
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return value
        return re.sub('^[a-z]$', '', value)

    def extract_json(self, response: hrequests.Response) -> list[Any] | None:
        """
        Extracts game data from script tag in format of JSON list.

        Parameters
        ----------
        response : hrequests.Response
            HTTP response object.

        Returns
        -------
        list[Any] | None
            Parsed JSON data or None if extraction fails.
        """
        if not response:
            return None
        url = response.url
        page_tree = HTMLParser(response.content)
        data_container = None
        for node in page_tree.css('body > script'):
            if 'window.__NUXT__' in node.text():
                data_container = node
                break
        if not data_container:
            return None
        raw_data = re.findall(
            r'[a-z]\.components=(\[.*\]);[a-z]\.footer',
            data_container.text()
        )
        try:
            json_data = chompjs.parse_js_object(raw_data[0])
        except (IndexError, json.decoder.JSONDecodeError) as err:
            self.queues['fail_pages'].put((url, err))
            logging.exception('An error occurred due to processing url %s', url)
            return None
        return json_data


def main():
    """
    Runs games data extraction.
    """
    DataExtractor(year=1984).run()


if __name__ == "__main__":
    main()
