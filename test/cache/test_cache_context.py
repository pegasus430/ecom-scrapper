import mock
import datetime
from scrapy.http import Request
from content_analytics.middlewares.cache import CacheContext

def test_crawl_date_as_string_set_in_item():
    date = '2018-05-07'
    item = mock.MagicMock(wraps=dict())
    request = mock.MagicMock(spec=Request, meta={'item': item})
    with CacheContext(request, date=date) as cached_req:
        assert cached_req.meta.get('item', {}).get('crawl_date') == date

def test_crawl_date_as_datetime_set_in_item():
    date = '2018-05-07'
    date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
    item = mock.MagicMock(wraps={})
    request = mock.MagicMock(spec=Request, meta={'item': item})
    with CacheContext(request, date=date_obj) as cached_req:
        assert cached_req.meta.get('item', {}).get('crawl_date') == date
