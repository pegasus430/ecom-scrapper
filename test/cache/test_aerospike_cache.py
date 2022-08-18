import mock
import pytest
import datetime
from scrapy import Spider
from scrapy.http import Request, Response
from content_analytics.middlewares.cache.aero import AerospikeCache, AerospikeCacheEntry
from content_analytics.middlewares.cache import CACHE_ATTRIBUTE_DATE, CACHE_ATTRIBUTE_FINGERPRINT, \
    ExpiredCrawlDateError
from aerospike import Client  # pylint: disable=E0611
from aerospike.exception import RecordNotFound  # pylint: disable=E0611,E0401

# pylint:disable=redefined-outer-name

SPIDER_NAME = 'test_products'

VALID_SETTINGS = {
    'CACHE_HOSTS': '127.0.0.1:3000',
    'CACHE_USERNAME': 'qwe',
    'CACHE_PASSWORD': 'rty',
    'CACHE_NAMESPACE': 'test',
    'CACHE_DEFAULT_TTL': 100,
    'CACHE_DEFAULT_POLICIES': {}
}

TODAY = datetime.datetime.strptime('2018-05-05', '%Y-%m-%d')
PAST = datetime.datetime.strptime('2018-05-04', '%Y-%m-%d')


@pytest.fixture()
def settings_mock():
    settings = mock.MagicMock(wraps={})
    settings.update(VALID_SETTINGS)
    return settings


@pytest.fixture()
def crawler_mock(settings_mock):
    spider = mock.MagicMock(spec=Spider)
    spider.name = SPIDER_NAME
    return mock.MagicMock(spider=spider, settings=settings_mock)


@pytest.fixture()
def cache(crawler_mock):
    cache = AerospikeCache(crawler_mock)
    cache.client = mock.MagicMock(spec=Client)
    return cache


@pytest.fixture(params=[
    'CACHE_HOSTS', 'CACHE_NAMESPACE',
    'CACHE_DEFAULT_TTL', 'CACHE_DEFAULT_POLICIES',
], ids=lambda val: val)
def invalid_settings_key(request):
    return request.param


def test_init_fails_when_invalid_settings(invalid_settings_key, settings_mock):
    settings_mock.update({invalid_settings_key: None})
    spider = mock.MagicMock(spec=Spider)
    spider.name = SPIDER_NAME
    crawler = mock.MagicMock(spider=spider, settings=settings_mock)
    with pytest.raises(Exception):
        AerospikeCache(crawler)


def test_when_open_client_connects(cache):
    cache.client.is_connected.return_value = False
    cache.open()
    cache.client.connect.assert_called_once()


def test_when_close_client_closes(cache):
    cache.client.is_connected.return_value = True
    cache.close()
    cache.client.close.assert_called_once()


@mock.patch('scrapy.http.Request')
@mock.patch('content_analytics.middlewares.cache.aero.request_fingerprint', return_value='11111')
def test_entry_get_no_response_past_date(request_fingerprint, request, cache):
    entry = AerospikeCacheEntry(cache)
    cache._today = TODAY
    cache.client.get.side_effect = RecordNotFound()
    request.meta = {
        CACHE_ATTRIBUTE_DATE: PAST,
        CACHE_ATTRIBUTE_FINGERPRINT: None
    }
    with pytest.raises(ExpiredCrawlDateError):
        entry.get(request)
    cache.client.get.assert_called_once()


@mock.patch('scrapy.http.Request')
@mock.patch('content_analytics.middlewares.cache.aero.request_fingerprint',
            return_value='test_fingerprint')
def test_entry_get_no_response_present_date(request_fingerprint, request, cache):
    entry = AerospikeCacheEntry(cache)
    cache._today = TODAY
    cache.client.get.side_effect = RecordNotFound()
    setattr(request, CACHE_ATTRIBUTE_DATE, TODAY)
    setattr(request, CACHE_ATTRIBUTE_FINGERPRINT, None)
    assert entry.get(request) is None
    cache.client.get.assert_called_once()


@mock.patch('content_analytics.middlewares.cache.aero.AerospikeCacheEntry._AerospikeCacheEntry__decompress')
@mock.patch('scrapy.http.Request')
@mock.patch('scrapy.http.Response')
@mock.patch('content_analytics.middlewares.cache.aero.request_fingerprint',
            return_value='test_fingerprint')
def test_entry_get_response(request_fingerprint, response, request, decompress, cache):
    mock_data = {
        'cls': 'mock.MagicMock',
        'body': 'body',
        'url': 'url',
        'headers': 'headers',
        'status': 200
    }
    request.body = 'body'
    entry = AerospikeCacheEntry(cache)
    cache._today = TODAY
    cache.client.get.return_value = (None, None, mock_data)
    assert isinstance(entry.get(request), mock.MagicMock)
    cache.client.get.assert_called_once()


@mock.patch('scrapy.http.Request')
@mock.patch('scrapy.http.Response')
@mock.patch('content_analytics.middlewares.cache.aero.request_fingerprint',
            return_value='test_fingerprint')
def test_entry_put_response(request_fingerprint, response, request, cache):
    response.body = 'body'
    entry = AerospikeCacheEntry(cache)
    entry.put(request, response)
    cache.client.put.assert_called_once()
