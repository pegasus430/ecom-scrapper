import traceback
from datetime import datetime

import os
import six
import logging
import aerospike
import gzip

from cStringIO import StringIO
from aerospike import exception, TTL_NEVER_EXPIRE  # pylint: disable=E0611
from scrapy.utils.misc import load_object

from . import (
    BaseCache,
    ExpiredCrawlDateError,
    request_fingerprint,
    CACHE_ATTRIBUTE_FINGERPRINT,
    CACHE_ATTRIBUTE_TTL,
    CACHE_ATTRIBUTE_DATE,
    TTL_NEVER_EXPIRE as BASE_TTL_NEVER_EXPIRE,
    CRAWL_DATE_FORMAT,
    CacheMiddleware
)

logger = logging.getLogger(__name__)

# don't depend on aerospike specific variables in other parts of the code
BASE_TO_AERO = {
    BASE_TTL_NEVER_EXPIRE: TTL_NEVER_EXPIRE
}


class AerospikeCacheEntry(object):
    def __init__(self, cache):
        self.cache = cache

    def get(self, request):
        crawl_date = request.meta.get(CACHE_ATTRIBUTE_DATE, self.cache._today)
        ttl = request.meta.get(CACHE_ATTRIBUTE_TTL, None)
        fingerprint = request.meta.get(CACHE_ATTRIBUTE_FINGERPRINT,
                                       request_fingerprint_with_ttl(request, crawl_date, ttl))
        request.meta[CACHE_ATTRIBUTE_FINGERPRINT] = fingerprint  # preserve fingerprint in case of redirects
        raw_response = None
        try:
            _, _, raw_response = self.cache.client.get(
                key=(self.cache.namespace, self.cache.set_, fingerprint,),
            )
        except exception.RecordNotFound:
            logger.debug("Cache miss: date {} for {}"
                         .format(crawl_date.strftime('%Y-%m-%d'), request.url))

        if not raw_response:
            today = self.cache._today  # cache has no response for passed date, raise exception if past date
            if crawl_date.date() != today.date() \
                    and not request.meta.get(CACHE_ATTRIBUTE_FINGERPRINT, None) \
                    and ttl != BASE_TTL_NEVER_EXPIRE:
                raise ExpiredCrawlDateError()
            else:
                return

        logger.debug('Cache hit: date {} for {}'.format(request.meta.get(
            CACHE_ATTRIBUTE_DATE,
            crawl_date.strftime(CRAWL_DATE_FORMAT)),
            request.url))

        self.cache.stats.inc_value(CacheMiddleware.CACHE_STATS_GET_BYTES,
                                   len(raw_response.get('cls')))
        data = {
            'url': raw_response.get('url'),
            'headers': raw_response.get('headers'),
            'body': self.__decompress(raw_response.get('body'))
        }
        for value in data.values():
            if isinstance(value, int):
                self.cache.stats.inc_value(CacheMiddleware.CACHE_STATS_GET_BYTES, 8)  # int is 8 bytes in aerospike
            else:
                self.cache.stats.inc_value(CacheMiddleware.CACHE_STATS_GET_BYTES, len(value))
        return load_object(raw_response.get('cls'))(
            request=request,
            status=200,
            **data
        )

    def put(self, request, response):
        crawl_date = request.meta.get(CACHE_ATTRIBUTE_DATE, self.cache._today)
        ttl = request.meta.get(CACHE_ATTRIBUTE_TTL, None)
        fingerprint = request.meta.get(CACHE_ATTRIBUTE_FINGERPRINT,
                                       request_fingerprint_with_ttl(request, crawl_date, ttl))
        data = {
            'cls': '.'.join([
                response.__class__.__module__,
                response.__class__.__name__
            ]),
            'url': response.url,
            'headers': response.headers,
            'body': bytearray(self.__compress(response.body.decode('utf-8')))
        }
        self.cache.client.put(
            key=(self.cache.namespace, self.cache.set_, fingerprint),
            meta={'ttl': BASE_TO_AERO.get(ttl, self.cache.ttl)},
            bins=data
        )
        for value in data.values():
            if isinstance(value, int):
                self.cache.stats.inc_value(CacheMiddleware.CACHE_STATS_PUT_BYTES, 8)  # int is 8 bytes in aerospike
            else:
                self.cache.stats.inc_value(CacheMiddleware.CACHE_STATS_PUT_BYTES, len(value))

    def __compress(self, text):
        if not isinstance(text, unicode):
            raise ValueError('Text for compression must be unicode')
        compressed = StringIO()
        with gzip.GzipFile(fileobj=compressed, mode='w') as gzipf:
            gzipf.write(text.encode('utf-8'))
        return compressed.getvalue()

    def __decompress(self, text):
        if isinstance(text, unicode):
            raise ValueError('Text for decompression can not be unicode')
        with gzip.GzipFile(fileobj=StringIO(text), mode='r') as gzipf:
            return gzipf.read()


class AerospikeCache(BaseCache):
    def __init__(self, crawler, *args, **kwargs):
        super(AerospikeCache, self).__init__(crawler, *args, **kwargs)
        settings = crawler.settings
        self.stats = crawler.stats
        if os.environ.get('CACHE_HOSTS'):
            hosts = os.environ.get('CACHE_HOSTS')
        else:
            hosts = settings.get('CACHE_HOSTS')

        self.username = settings.get('CACHE_USERNAME')
        self.password = settings.get('CACHE_PASSWORD')
        self.namespace = settings.get('CACHE_NAMESPACE')
        try:
            self.set_ = crawler.spider.name[:crawler.spider.name.rindex('_products')]
        except ValueError:
            self.set_ = crawler.spider.name
        self.ttl = settings.get('CACHE_DEFAULT_TTL')
        self.policies = settings.get('CACHE_DEFAULT_POLICIES')

        assert isinstance(hosts, six.string_types)
        assert isinstance(self.namespace, six.string_types)
        assert isinstance(self.set_, six.string_types)
        assert isinstance(self.ttl, int)
        assert isinstance(self.policies, dict)

        try:
            self.hosts = [
                (host.split(':')[0], int(host.split(':')[1]))
                for host in hosts.split(',')
            ]
        except:
            raise AssertionError('CACHE_URI should be in format "host1:3001,host2:3002"')

        if self.username:
            assert isinstance(self.username, six.string_types)

        if self.password:
            assert isinstance(self.password, six.string_types)

        self._today = datetime.utcnow()

        self.client = aerospike.client({
            'hosts': self.hosts,
            'policies': self.policies,
            'use_shared_connection': True  # Used to prevent TCP overhead in runner script
        })

    def open(self, *args, **kwargs):
        if not self.client.is_connected():
            self.client.connect(self.username, self.password)

    def close(self, *args, **kwargs):
        if self.client.is_connected():
            self.client.close()

    def get(self, request, *args, **kwargs):
        try:
            return AerospikeCacheEntry(self).get(request)
        except exception.RecordNotFound:
            return
        except ExpiredCrawlDateError:
            raise
        except:
            logger.warning('Error while retrieving cache: {}'.format(traceback.format_exc()))

    def put(self, request, response, *args, **kwargs):
        try:
            AerospikeCacheEntry(self).put(request, response)
            return True
        except:
            logger.warning('Error while storing cache: {}'.format(traceback.format_exc()))
            return False


def request_fingerprint_with_ttl(request, date=None, ttl=None, date_format=CRAWL_DATE_FORMAT):
    if ttl == BASE_TTL_NEVER_EXPIRE or ttl == BASE_TO_AERO[BASE_TTL_NEVER_EXPIRE]:
        return request_fingerprint(request, "", date_format)
    return request_fingerprint(request, date, date_format)
