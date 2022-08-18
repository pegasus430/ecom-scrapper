import mock
import pytest
from content_analytics.middlewares.cache import CacheMiddleware, CACHE_ATTRIBUTE_ENABLED, \
    CACHE_ATTRIBUTE_CACHED_RESPONSE
from scrapy.http import Request, Response
from scrapy.signalmanager import SignalManager
from scrapy import signals, Spider

# pylint:disable=redefined-outer-name

SPIDER_NAME = 'spider_name'


@pytest.fixture()
def settings_mock():
    settings = mock.MagicMock(wraps={})
    settings.update({'CACHE_ENABLED': True,
                     'CACHE_MODULE': 'content_analytics.middlewares.cache.BaseCache',
                     'CACHE_SPIDERS': [SPIDER_NAME]})
    return settings


@pytest.fixture()
def crawler_mock(settings_mock):
    spider = mock.MagicMock(spec=Spider, summary=False, crawl_date=None)
    spider.name = SPIDER_NAME
    crawler = mock.MagicMock(spider=spider, settings=settings_mock,
                             signals=mock.MagicMock(spec=SignalManager))
    return crawler


@pytest.fixture()
@mock.patch('content_analytics.middlewares.cache.BaseCache', autospec=True)
def cache_middleware_mock(cache, crawler_mock):
    return CacheMiddleware.from_crawler(crawler_mock)


@pytest.fixture()
def request_mock():
    return mock.MagicMock(spec=Request, meta={})


def test_from_crawler_none_when_cache_is_disabled():
    settings = mock.MagicMock(wraps={})
    crawler = mock.MagicMock(settings=settings)
    assert CacheMiddleware.from_crawler(crawler) is None


def test_from_crawler_none_when_spider_not_in_list(crawler_mock):
    crawler_mock.settings.update({'CACHE_SPIDERS': []})
    assert CacheMiddleware.from_crawler(crawler_mock) is None


def test_from_crawler_none_when_summary_true(crawler_mock):
    crawler_mock.spider.summary = True
    assert CacheMiddleware.from_crawler(crawler_mock) is None


def test_reject_request_when_no_cache_attribute_set(request_mock, cache_middleware_mock):
    assert cache_middleware_mock.process_request(request_mock) is None


def test_no_response_in_cache(cache_middleware_mock):
    base_cache = cache_middleware_mock.client
    base_cache.get = mock.Mock(return_value=None)
    request = mock.MagicMock(spec=Request)
    setattr(request, CACHE_ATTRIBUTE_ENABLED, True)
    assert cache_middleware_mock.process_request(request) is None
    base_cache.get.assert_called_once()


def test_response_in_cache(cache_middleware_mock):
    base_cache = cache_middleware_mock.client
    response = mock.MagicMock(spec=Response)
    request = mock.MagicMock(spec=Request)

    def cache_get(req, *args, **kwargs):
        if req == request:
            return response

    base_cache.get = mock.Mock(side_effect=cache_get)
    setattr(request, CACHE_ATTRIBUTE_ENABLED, True)
    response_from_cache = cache_middleware_mock.process_request(request)
    assert response_from_cache == response
    assert response_from_cache.meta.get(CACHE_ATTRIBUTE_CACHED_RESPONSE, False)
    base_cache.get.assert_called_once()


@mock.patch('scrapy.http.Response')
def test_response_not_saved_to_cache_if_no_cache_attribute_set(response, request_mock, cache_middleware_mock):
    response.status = 200
    assert response == cache_middleware_mock.process_response(request_mock, response)
    assert not cache_middleware_mock.client.put.called


@mock.patch('scrapy.http.Response')
def test_response_from_cache_are_not_saved_again(response, request_mock, cache_middleware_mock):
    response.status = 200
    assert response == cache_middleware_mock.process_response(request_mock, response)
    assert not cache_middleware_mock.client.put.called


def test_response_with_non_200_status_are_not_saved(cache_middleware_mock):
    statuses = [100, 101, 201, 250, 400, 404, 500, 504]
    for status in statuses:
        response = mock.MagicMock(spec=Response, status=status)
        request = mock.MagicMock(spec=Request)
        assert response == cache_middleware_mock.process_response(request, response)
        assert not cache_middleware_mock.client.put.called


@mock.patch('scrapy.http.Response')
def test_response_put_to_cache_correctly(response, request_mock, cache_middleware_mock):
    response.status = 200
    request_mock.meta[CACHE_ATTRIBUTE_ENABLED] = True
    request_mock.meta[CACHE_ATTRIBUTE_CACHED_RESPONSE] = False
    assert response == cache_middleware_mock.process_response(request_mock, response)
    cache_middleware_mock.client.put.assert_called_once()
    cache_middleware_mock.client.put.assert_called_with(request_mock, response)


@mock.patch('content_analytics.middlewares.cache.BaseCache', autospec=True)
def test_signals_connected_to_client(cache, crawler_mock):
    middleware = CacheMiddleware.from_crawler(crawler_mock)
    calls = [mock.call(middleware.spider_opened, signal=signals.spider_opened),
             mock.call(middleware.spider_closed, signal=signals.spider_closed)]
    crawler_mock.signals.connect.assert_has_calls(calls, any_order=False)
