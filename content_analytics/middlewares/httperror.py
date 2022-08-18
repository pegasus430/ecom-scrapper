# pylint: skip-file
from scrapy.spidermiddlewares.httperror import HttpErrorMiddleware as _HttpErrorMiddleware


class HttpErrorMiddleware(_HttpErrorMiddleware):
    def process_spider_exception(self, *args, **kwargs):
        super(HttpErrorMiddleware, self).process_spider_exception(*args, **kwargs)
