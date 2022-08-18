import logging

from uuid import uuid4

from scrapy.item import Item
from scrapy.http import Request, Response
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.request import request_fingerprint as scrapy_request_fingerprint

from content_analytics import signals

logger = logging.getLogger(__name__)


def request_fingerprint_for_merge(request, include_headers=None):
    # Merge middleware requires special fingerprint calculation for 2 cases
    # 1) Because splash middleware alters requests' url, method, and body, the fingerprint changes.
    #    SplashContext saves the initial fingerprint to meta of request for merge middleware to use.
    # 2) If a request is redirected, its fingerprint will change. To track
    #    the initial request, we use the first url before redirects to calculate fingerprint.
    if 'initial_fingerprint' in request.meta and not include_headers:
        return request.meta['initial_fingerprint']
    urls = request.meta.get('redirect_urls')
    if urls and not include_headers:
        return scrapy_request_fingerprint(request.replace(url=urls[0]))
    return scrapy_request_fingerprint(request, include_headers)


class MergeRequest(Request):
    """Special request class for `MergeItemMiddleware` to use merge item feature to prevent long
     and related callback chains.

    This request type can be used such as default Scrapy `Request` by calling it constructor with additional
    response and item parameters. `MergeRequest` works properly with enabled `MergeItemMiddleware` only.

    """

    def __init__(self, url, item, *args, **kwargs):
        assert isinstance(item, Item)

        meta = kwargs.pop('meta', {})
        if not hasattr(item, MergeItemMiddleware.GUID_ATTRIBUTE):
            guid = uuid4().get_hex()  # GUID generation

            logger.debug('Adding GUID {} to item with id 0x{:02x}'.format(guid, id(item)))
            setattr(item, MergeItemMiddleware.GUID_ATTRIBUTE, guid)

            logger.debug('Mark request as initial')
            meta.update({MergeItemMiddleware.INITIAL_ATTRIBUTE: True})

        meta.update({MergeItemMiddleware.ITEM_ATTRIBUTE: item})
        super(MergeRequest, self).__init__(url, meta=meta, *args, **kwargs)

    def replace(self, *args, **kwargs):
        item = self.meta.get(MergeItemMiddleware.ITEM_ATTRIBUTE, None)
        if item is None:
            logger.error('Item is not presented in `MergeRequest` while replacing {}!')
            return
        return super(MergeRequest, self).replace(item=item, *args, **kwargs)


class MergeFailResponse(Response):
    HTTP_STATUS = 0

    def __init__(self, request, *args, **kwargs):
        super(MergeFailResponse, self).__init__(
            url=request.url,
            status=self.HTTP_STATUS,
            *args, **kwargs
        )


class MergeItemMiddleware(object):
    """Middleware class implements merge item feature to prevent long and related callback chains.

    You just yield in a spider all requests you need and middleware will return fully-filled item.
    To use this feature you have to import `MergeRequest` from `spiders` and additionally pass response and item
    instances into the `MergeRequest` constructor with other default parameters (url, callback).

    Also, you can use default `Request`s as normal, but in this case items and requests will be ignored
    by the middleware.

    Author of the original code is Nicolas Ramirez.
    Great thanks him for idea and code example.
    """
    GUID_ATTRIBUTE = '_guid'
    ITEM_ATTRIBUTE = 'item'
    INITIAL_ATTRIBUTE = '_initial'

    def __init__(self):
        logger.debug('{} initialized'.format(self.__class__.__name__))
        self.memorized = {}

    @classmethod
    def from_crawler(cls, crawler):
        logger.debug('{} called from crawler'.format(cls.__class__.__name__))

        middleware = cls()
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def spider_closed(self, spider, reason):
        if reason == 'finished':
            if self.memorized:
                spider.logger.warning('Some data left in memory {}'.format(repr(self.memorized)))
        else:
            spider.logger.warning(
                'Spider stopped before finish! Some data left in memory {}: '.format(repr(self.memorized))
            )

    def process_spider_output(self, response, result, spider):
        logger.debug('Processing output response {}'.format(response))
        if isinstance(response.request, MergeRequest):
            logger.debug('Output response of is instance of `MergeRequest` and continue merging')

            item = response.meta.get(self.ITEM_ATTRIBUTE, None)
            if item is None:
                logger.error('Item is not presented in output response {}!'.format(response))

            guid = getattr(item, self.GUID_ATTRIBUTE, None)
            if not guid:
                logger.error('Item {} GUID is not presented in output response {}!'.format(item, response))

            self.__process_response(response)
            for r in arg_to_iter(result):
                if isinstance(r, MergeRequest):
                    yield self.__process_output_request(r)
                elif isinstance(r, Request):
                    logger.warning('You should not yield simple `Request` in context of `MergeRequest`!')
                    yield r
                elif isinstance(r, Item):
                    if getattr(r, self.GUID_ATTRIBUTE, None) != guid:
                        logger.warning(
                            'You should yield items only from response {} meta with GUID {}'.format(response, guid)
                        )
                        yield r
                    logger.debug('Store output item with id 0x{:02x} to memory'.format(id(item)))
                    self.memorized[guid]['item'] = r

            if not self.memorized[guid]['requests']:
                logger.debug('No requests left for GUID {}. Returning item with id 0x{:02x}'.format(guid, id(item)))
                yield self.memorized[guid]['item']
                del self.memorized[guid]
        else:
            for r in result:
                if isinstance(r, MergeRequest):
                    yield self.__process_output_request(r)
                else:
                    yield r

    def process_spider_exception(self, response, exception, spider):
        logger.warning('Processing exception {}'.format(exception))
        if isinstance(response.request, MergeRequest) and isinstance(exception, HttpError):
            if not response.request.meta.get(self.INITIAL_ATTRIBUTE, None):
                return [response]

    def __process_response(self, response):
        logger.debug('Processing response {}'.format(response))

        item = response.meta.get(self.ITEM_ATTRIBUTE, None)
        if item is None:
            logger.error('Item is not presented in output response {}!'.format(response))
            return

        guid = getattr(item, self.GUID_ATTRIBUTE, None)
        if not guid:
            logger.error(
                'GUID is not presented in output response {} item with id 0x{:02x}!'.format(response, id(item))
            )

        fingerprint = request_fingerprint_for_merge(response.request)
        if fingerprint in self.memorized[guid]['requests']:
            logger.debug('Removing output request fingerprint {}'.format(fingerprint))
            self.memorized[guid]['requests'].remove(fingerprint)

    def __process_output_request(self, request):
        logger.debug('Processing output request {}'.format(request))

        item = request.meta.get(self.ITEM_ATTRIBUTE, None)
        if item is None:
            logger.error('Item is not presented in output request {}!'.format(request))
            return

        guid = getattr(item, self.GUID_ATTRIBUTE, None)
        if not guid:
            logger.error(
                'GUID is not presented in output request {} item with id 0x{:02x}!'.format(request, id(item))
            )

        memo = self.memorized.get(guid, None)
        if memo is None:
            logger.debug('Initialize memory for GUID {}'.format(guid))
            self.memorized.setdefault(guid, {
                'requests': [],
                'item': item,
            })

        fingerprint = request_fingerprint_for_merge(request)
        logger.debug('Appending output request fingerprint {}'.format(fingerprint))
        self.memorized[guid]['requests'].append(fingerprint)

        return request

    def process_start_requests(self, start_requests, spider):
        for request in start_requests:
            if isinstance(request, MergeRequest):
                item = request.meta.get(self.ITEM_ATTRIBUTE, None)
                if item is None:
                    logger.error('Item is not presented in output request {}!'.format(request))
                    return

                guid = getattr(item, self.GUID_ATTRIBUTE, None)
                if not guid:
                    logger.error('Item {} GUID is not presented in output request {}!'.format(item, request))

                memo = self.memorized.get(guid, None)
                if memo is None:
                    logger.debug('Initialize memory for GUID {}'.format(guid))
                    self.memorized.setdefault(guid, {
                        'requests': [],
                        'item': item,
                    })
                yield self.__process_output_request(request)
            else:
                yield request


class MergeItemDownloaderMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_exception(self, request, exception, spider):
        logger.debug('Processing exception type {} with value {} for request {}'.format(type(exception), exception, request))
        if isinstance(request, MergeRequest):
            if not request.meta.get(MergeItemMiddleware.INITIAL_ATTRIBUTE, None):
                response = MergeFailResponse(request)
                logger.warning(exception)
                return response

