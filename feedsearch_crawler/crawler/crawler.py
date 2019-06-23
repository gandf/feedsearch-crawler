import asyncio
import copy
import inspect
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from statistics import harmonic_mean
from types import AsyncGeneratorType
from typing import List, Any
from typing import Union

import aiohttp
from aiohttp import ClientTimeout
from yarl import URL

from feedsearch_crawler.crawler.duplicatefilter import DuplicateFilter
from feedsearch_crawler.crawler.item import Item
from feedsearch_crawler.crawler.lib import coerce_url, ignore_aiohttp_ssl_eror
from feedsearch_crawler.crawler.request import Request
from feedsearch_crawler.crawler.response import Response

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    uvloop = None
    pass


@dataclass
class CallbackResult:
    """Dataclass for holding callback results and recording recursion"""

    result: Any
    callback_recursion: int


class Crawler(ABC):

    # Class Name of the Duplicate Filter.
    # May be overridden to use different Duplicate Filter.
    # Not an instantiation of the class.
    duplicate_filter_class = DuplicateFilter

    # Callback to be run after all workers are finished.
    post_crawl_callback = None

    # Max number of concurrent http requests.
    concurrency: int = 10
    # Max size of incoming http response content.
    max_content_length = 1024 * 1024 * 10
    # Max crawl depth. i.e. The max length of the response history.
    max_depth: int = 0
    # Max callback recursion depth, to prevent accidental infinite recursion from AsyncGenerators.
    max_callback_recursion: int = 10

    # List of worker tasks.
    _workers = []

    # ClientSession for requests. Created on Crawl start.
    _session: aiohttp.ClientSession
    # Task queue for Requests. Created on Crawl start.
    _request_queue: asyncio.Queue
    # Semaphore for controlling HTTP Request concurrency.
    _semaphore: asyncio.Semaphore

    def __init__(
        self,
        start_urls: List[str] = None,
        concurrency: int = 10,
        total_timeout: Union[float, ClientTimeout] = 10,
        request_timeout: Union[float, ClientTimeout] = 3,
        user_agent: str = "",
        max_content_length: int = 1024 * 1024 * 10,
        max_depth: int = 10,
        headers: dict = None,
        allowed_schemes: List[str] = None,
        *args,
        **kwargs,
    ):
        """
        Base class for a WebCrawler implementation.

        :param allowed_schemes: List of strings of allowed Request URI schemes. e.g. ["http", "https"]
        :param start_urls: List of initial URLs to crawl.
        :param concurrency: Max number of workers and of concurrent HTTP requests.
        :param total_timeout: Total aiohttp ClientSession timeout. Crawl will end if this timeout is triggered.
        :param request_timeout: Total timeout for each individual HTTP request.
        :param user_agent: Default User-Agent for HTTP requests.
        :param max_content_length: Max size in bytes of incoming http response content.
        :param max_depth: Max crawl depth. i.e. The max length of the response history.
        :param headers: Default HTTP headers to be included in each request.
        :param args: Additional positional arguments for subclasses.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        self.start_urls = start_urls or []
        self.concurrency = concurrency

        if not isinstance(total_timeout, ClientTimeout):
            total_timeout = aiohttp.ClientTimeout(total=total_timeout)
        if not isinstance(request_timeout, ClientTimeout):
            request_timeout = aiohttp.ClientTimeout(total=request_timeout)

        self.total_timeout: ClientTimeout = total_timeout
        self.request_timeout: ClientTimeout = request_timeout

        self.max_content_length = max_content_length
        self.max_depth = max_depth

        self.user_agent = user_agent or (
            "Mozilla/5.0 (compatible; Feedsearch-Crawler; +https://pypi.org/project/feedsearch-crawler)"
        )

        self.headers = {"User-Agent": self.user_agent, "Upgrade-Insecure-Requests": "1"}

        if headers:
            self.headers = {**self.headers, **headers}

        self.allowed_schemes = allowed_schemes

        self.logger = logging.getLogger("feedsearch_crawler")

        # Default set for parsed items.
        self.items: set = set()

        # URL Duplicate Filter instance.
        self._duplicate_filter = self.duplicate_filter_class()

        self.request_durations = []
        self.request_content_length = []

        # Crawl statistics.
        self.stats: dict = {
            # Number of HTTP Requests added and sent.
            "requests_added": 0,
            # Number of HTTP Requests that were successful (HTTP Status code 200-299).
            "requests_successful": 0,
            # Number of HTTP Requests that were unsuccessful (HTTP Status code not in 200s).
            "requests_failed": 0,
            # Total size in bytes of all HTTP Requests.
            "content_length_total": 0,
            # Harmonic mean of total HTTP Request content length in bytes.
            "content_length_avg": 0,
            # Highest HTTP Request content length in bytes.
            "content_length_max": 0,
            # Lowest HTTP Request content length in bytes.
            "content_length_min": 0,
            # Number of Items processed.
            "items_processed": 0,
            # Number of URls seen and added to duplicate filter.
            "urls_seen": 0,
            # Harmonic mean of HTTP Request duration in Milliseconds.
            "requests_duration_avg": 0,
            # Highest HTTP request duration in Milliseconds.
            "requests_duration_max": 0,
            # Lowest HTTp request duration in Milliseconds.
            "requests_duration_min": 0,
            # Total HTTP request duration in Milliseconds.
            "requests_duration_total": 0,
            # Total duration of crawl in Milliseconds.
            "total_duration": 0,
        }

    async def _handle_request(self, request: Request) -> None:
        """
        Handle fetching of Requests and processing of Request callbacks.

        :param request: Request
        :return: None
        """
        try:
            if request.has_run:
                self.logger.warning("%s has already run", request)
                return

            start = time.perf_counter()

            # Fetch the request and run its callback
            results, response = await request.fetch_callback(self._semaphore)

            dur = int((time.perf_counter() - start) * 1000)
            self.request_durations.append(dur)
            self.logger.debug(
                "Fetched: url=%s dur=%dms status=%s prev=%s",
                response.url,
                dur,
                response.status_code,
                response.originator_url,
            )

            if response.ok:
                self.stats["requests_successful"] += 1
            else:
                self.stats["requests_failed"] += 1

            self.request_content_length.append(response.content_length)

            # Mark the Response URL as seen in the duplicate filter, as it may be different from the Request URL
            # due to redirects.
            await self._duplicate_filter.url_seen(response.url, response.method)

            # Add callback results to the queue for processing.
            if results:
                self._request_queue.put_nowait(CallbackResult(results, 0))

        except asyncio.CancelledError:
            self.logger.debug("Cancelled: %s", request)
        except Exception as e:
            self.logger.exception("Exception during %s: %s", request, e)
        finally:
            return

    async def _process_request_callback_result(
        self, result: Any, callback_recursion: int = 0
    ) -> None:
        """
        Process the Request callback result depending on the result type.
        Request callbacks may contain nested iterators.

        :param result: Callback Result. May be an CallbackResult class, AsyncGenerator, Coroutine, Request, or Item.
        :param callback_recursion: Incremented counter to limit this method's recursion.
        :return: None
        """
        if callback_recursion >= self.max_callback_recursion:
            self.logger.warning(
                "Max callback recursion of %d reached", self.max_callback_recursion
            )
            return

        try:
            # If a CallbackResult class is passed, process the result values from within the class.
            if isinstance(result, CallbackResult):
                await self._process_request_callback_result(
                    result.result, result.callback_recursion
                )
            # For async generators, put each value back on the queue for processing.
            # This will happen recursively until the end of the recursion chain or max_callback_recursion is reached.
            elif inspect.isasyncgen(result):
                async for value in result:
                    self._request_queue.put_nowait(
                        CallbackResult(value, callback_recursion + 1)
                    )
            # For coroutines, await the result then put the value back on the queue for further processing.
            elif inspect.iscoroutine(result):
                value = await result
                self._request_queue.put_nowait(
                    CallbackResult(value, callback_recursion + 1)
                )
            # Requests are checked for uniqueness and put onto the queue.
            elif isinstance(result, Request):
                await self._process_request(result)

            # Items are handled by the implementing Class
            elif isinstance(result, Item):
                await self.process_item(result)
                self.stats["items_processed"] += 1
        except Exception as e:
            self.logger.exception(e)

    async def _process_request(self, request: Request) -> None:
        """
        Process a Request onto the Request Queue.

        Before a Request is added to the Queue, first check that the Request URL has not already been seen,
        that the max URL depth has not been reached, and that the URI scheme is allowed.

        :param request: HTTP Request
        :return: None
        """
        # If URL is not already seen, and add it to the duplicate filter seen list.
        if await self._duplicate_filter.url_seen(request.url, request.method):
            return

        # The URL scheme must be in the list of allowed schemes.
        if self.allowed_schemes and request.url.scheme not in self.allowed_schemes:
            self.logger.debug(
                "URI Scheme '%s' not allowed: %s", request.url.scheme, request
            )
            return

        # Restrict the depth of the Request chain to the maximum depth.
        if self.max_depth and len(request.history) >= self.max_depth:
            self.logger.debug("Max Depth of '%d' reached: %s", self.max_depth, request)
            return

        self.stats["requests_added"] += 1
        self.logger.debug("Queue Add: %s", request)
        # Add the Request to the queue for processing.
        self._request_queue.put_nowait(request)

    def follow(
        self, url: Union[str, URL], callback=None, response: Response = None, **kwargs
    ) -> Request:
        """
        Follow a URL by creating an HTTP Request.

        If the URL is not absolute then it is joined with the previous Response URL.
        The previous Response history is copied to the Request.

        :param url: URL to follow.
        :param callback: Callback method to run if the Request is successful.
        :param response: Previous Response that contained the Request URL.
        :param kwargs: Optional Request keyword arguments. See Request for details.
        :return: Request
        """
        if isinstance(url, str):
            url = URL(url)

        history = []
        if response:
            # Join the URL to the Response URL if it doesn't contain a domain.
            if not url.is_absolute():
                url = response.url.origin().join(url)
            # Copy the Response history so that it isn't a pointer.
            history = copy.deepcopy(response.history)

        request = Request(
            url=url,
            request_session=self._session,
            history=history,
            callback=callback,
            xml_parser=self.parse_xml,
            max_content_length=self.max_content_length,
            timeout=self.request_timeout,
            **kwargs,
        )

        return request

    @abstractmethod
    async def process_item(self, item: Item) -> None:
        """
        Processed a parsed Item in some way. e.g. Add it to the Item set, or database, or send a signal.

        :param item: A parsed Item.
        """
        self.items.add(item)

    @abstractmethod
    async def parse_xml(self, response_text: str) -> Any:
        """
        Parse Response text as XML.
        Used to allow implementations to provide their own XML parser.

        :param response_text: Response text as string.
        """
        raise NotImplementedError("Not Implemented")

    @abstractmethod
    async def parse(self, request: Request, response: Response) -> AsyncGeneratorType:
        """
        Parse an HTTP Response. Must yield Items, Requests, AsyncGenerators, or Coroutines.

        :param request: HTTP Request that created the Response.
        :param response: HTTP Response.
        """
        raise NotImplementedError("Not Implemented")

    async def _work(self):
        """
        Worker function for handling request queue items.
        """
        while True:
            item = await self._request_queue.get()

            try:
                # Fetch Request and handle callbacks
                if isinstance(item, Request):
                    try:
                        await self._handle_request(item)
                    except asyncio.CancelledError:
                        self.logger.debug("Cancelled Request: %s", item)
                # Process Callback results
                elif isinstance(item, CallbackResult):
                    await self._process_request_callback_result(
                        item.result, item.callback_recursion
                    )
            finally:
                self._request_queue.task_done()

    async def _run_callback(self, callback, *args, **kwargs) -> None:
        """
        Runs a callback function.

        :param callback: Function to run. May be async.
        :param args: Positional arguments to pass to the function.
        :param kwargs: Keyword arguments to pass to the function.
        :return: None
        """
        if not callback:
            return
        if inspect.iscoroutinefunction(callback):
            await callback(*args, **kwargs)
        elif inspect.isfunction(callback):
            callback(*args, **kwargs)
        else:
            self.logger.warning("Callback %s must be a coroutine or function", callback)

    def create_start_urls(self, url: Union[str, URL]) -> List[URL]:
        """
        Create the start URLs for the crawl from an initial URL. May be overridden.

        :param url: Initial URL
        """
        if isinstance(url, str):
            url = URL(url)

        if url.scheme not in ["http", "https"]:
            url = url.with_scheme("http")

        return [url]

    async def crawl(self, url: Union[URL, str] = ""):
        """
        Start the web crawler.

        :param url: An optional URL to start the crawl. If not provided then start_urls are used.
        """

        # Fix for ssl errors
        ignore_aiohttp_ssl_eror(asyncio.get_running_loop())

        if url:
            self.start_urls = self.create_start_urls(url)

        if not self.start_urls:
            raise ValueError("crawler.start_urls are required")

        start = time.perf_counter()
        # Create the Request Queue and ClientSession within the asyncio loop.
        self._request_queue = asyncio.Queue()

        # Create the Semaphore for controlling HTTP Request concurrency within the asyncio loop.
        self._semaphore = asyncio.Semaphore(self.concurrency)

        self._session = aiohttp.ClientSession(
            timeout=self.total_timeout, headers=self.headers
        )

        # Create a Request for each start URL and add it to the Request Queue.
        for url in self.start_urls:
            await self._process_request(self.follow(coerce_url(url), self.parse))

        # Create workers to process the Request Queue.
        # Create twice as many workers as potential concurrent requests, to handle request callbacks without delay.
        self._workers = [
            asyncio.create_task(self._work()) for _ in range(self.concurrency * 2)
        ]

        try:
            # Run workers within the ClientSession.
            async with self._session:
                await asyncio.wait_for(
                    self._request_queue.join(), timeout=self.total_timeout.total
                )
        except asyncio.TimeoutError:
            self.logger.debug("Timed out after %s seconds", self.total_timeout)
        finally:
            # Make sure all workers are cancelled.
            for w in self._workers:
                w.cancel()

        # Run the post crawl callback if it exists.
        await self._run_callback(self.post_crawl_callback)

        # The ClientSession is closed only after all work is completed.
        await self._session.close()

        duration = int((time.perf_counter() - start) * 1000)

        # Record statistics
        self.stats["total_duration"] = duration
        self.stats["requests_duration_total"] = int(sum(self.request_durations))
        self.stats["requests_duration_avg"] = int(harmonic_mean(self.request_durations))
        self.stats["requests_duration_max"] = int(max(self.request_durations))
        self.stats["requests_duration_min"] = int(min(self.request_durations))
        self.stats["content_length_total"] = int(sum(self.request_content_length))
        self.stats["content_length_avg"] = int(
            harmonic_mean(self.request_content_length)
        )
        self.stats["content_length_max"] = int(max(self.request_content_length))
        self.stats["content_length_min"] = int(min(self.request_content_length))
        self.stats["urls_seen"] = len(self._duplicate_filter.fingerprints)

        self.logger.info(
            "Crawl finished: requests=%s time=%dms",
            (self.stats["requests_failed"] + self.stats["requests_successful"]),
            duration,
        )
        self.logger.debug("Stats: %s", self.stats)
