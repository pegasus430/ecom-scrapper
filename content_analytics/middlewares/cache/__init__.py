import logging
import traceback

from six import with_metaclass, string_types
from abc import ABCMeta, abstractmethod

from scrapy import Request
from scrapy.utils.misc import load_object
from scrapy.exceptions import NotConfigured
from datetime import datetime
from hashlib import sha1

from ... import signals

CACHE_ATTRIBUTE_TTL = '_cache_ttl'
CACHE_ATTRIBUTE_ENABLED = '_cache_enabled'
CACHE_ATTRIBUTE_FINGERPRINT = '_cache_fingerprint'
CACHE_ATTRIBUTE_CACHED_RESPONSE = '_cache_cached_response'
CACHE_ATTRIBUTE_DATE = '_cache_date'

TTL_NEVER_EXPIRE = '_ttl_never_expire'

CRAWL_DATE_FORMAT = '%Y-%m-%d'

logger = logging.getLogger(__name__)


class ExpiredCrawlDateError(ValueError):
    pass


class BaseCache(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def __init__(self, crawler, *args, **kwargs):
        pass

    @abstractmethod
    def get(self, request, *args, **kwargs):
        return

    @abstractmethod
    def put(self, request, response, *args, **kwargs):
        return

    @abstractmethod
    def open(self, *args, **kwargs):
        return

    @abstractmethod
    def close(self, *args, **kwargs):
        return


class CacheContext(object):
    request = None

    def __init__(
            self,
            request,
            ttl=None,
            fingerprint=None,
            date=None,
            ignore_headers=None,
            ignore_cookies=None
    ):
        assert isinstance(request, Request)
        request.meta[CACHE_ATTRIBUTE_ENABLED] = True

        if ttl:
            assert isinstance(ttl, int)
            request.meta[CACHE_ATTRIBUTE_TTL] = ttl

        if fingerprint:
            request.meta[CACHE_ATTRIBUTE_FINGERPRINT] = fingerprint

        if date:
            assert isinstance(date, (string_types, datetime))
            request.meta[CACHE_ATTRIBUTE_DATE] = date
            item = request.meta.get('item')
            if item and not item.get('crawl_date'):
                if isinstance(date, string_types):
                    item.update({'crawl_date': date})
                else:
                    item.update({'crawl_date': date.strftime(CRAWL_DATE_FORMAT)})

        self.request = request

    def __enter__(self):
        return self.request

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logger.exception('Error in cache context {}'.format(traceback.format_exc()))


class CacheMiddleware(object):
    stats = None
    client = None
    CACHE_STATS_ENABLED = 'cache/enabled'
    CACHE_STATS_PUT = 'cache/put/count'
    CACHE_STATS_PUT_BYTES = 'cache/put/bytes'
    CACHE_STATS_GET = 'cache/get/count'
    CACHE_STATS_GET_BYTES = 'cache/get/bytes'
    CACHE_STATS_CRAWL_DATE = 'cache/crawl_date'

    def __init__(self, crawler):
        try:
            self.client = load_object(crawler.settings.get('CACHE_MODULE'))(crawler)
            self.stats = crawler.stats
        except Exception as e:
            raise NotConfigured(e)

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.get('CACHE_ENABLED') \
                or crawler.spider.name not in crawler.settings.get('CACHE_SPIDERS') \
                or crawler.spider.summary:
            crawler.stats.set_value(cls.CACHE_STATS_ENABLED, False)
            return
        extension = cls(crawler)
        crawler.stats.set_value(cls.CACHE_STATS_ENABLED, True)
        if crawler.spider.crawl_date:
            crawler.stats.set_value(cls.CACHE_STATS_CRAWL_DATE,
                                    crawler.spider.crawl_date.strftime(CRAWL_DATE_FORMAT))
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    def process_request(self, request, *args, **kwargs):
        if request.meta.get(CACHE_ATTRIBUTE_ENABLED, False):
            response = self.client.get(request, *args, **kwargs)
            if response:
                self.stats.inc_value(self.CACHE_STATS_GET)
                logger.debug('Got response from cache for url {}'.format(request.url))
                request.meta[CACHE_ATTRIBUTE_CACHED_RESPONSE] = True
                return response

    def process_response(self, request, response, *args, **kwargs):
        if request.meta.get(CACHE_ATTRIBUTE_ENABLED, False) \
                and not request.meta.get(CACHE_ATTRIBUTE_CACHED_RESPONSE, False) \
                and response.status == 200:
            logger.debug('Saved response to cache for url {}'.format(request.url))
            if self.client.put(request, response, *args, **kwargs):
                self.stats.inc_value(self.CACHE_STATS_PUT)
        return response

    def spider_opened(self, *args, **kwargs):
        return self.client.open(*args, **kwargs)

    def spider_closed(self, *args, **kwargs):
        return self.client.close(*args, **kwargs)


def request_fingerprint(request, date=None, date_format=CRAWL_DATE_FORMAT):
    def _sort(o):
        return ''.join(sorted(str(o)))

    if isinstance(date, datetime):
        date = date.strftime(date_format)
    elif not isinstance(date, string_types):
        date = ""

    fp = sha1()
    fp.update(request.url)
    fp.update(request.method)
    fp.update(getattr(request, '_encoding'))
    fp.update(_sort(getattr(request, '_body')))
    fp.update(date)
    return fp.hexdigest()
