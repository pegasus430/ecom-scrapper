# pylint: disable=W0101, E0202
import base64
import os.path

from six import with_metaclass, string_types
from abc import ABCMeta, abstractmethod, abstractproperty

from scrapy import Spider, Request, Item
from dateutil import parser as date_parser

from content_analytics.utils import cond_set_value, fetch_product_from_req_or_item
from content_analytics.items import SiteProductItem
from content_analytics.middlewares.mergeitem import MergeRequest
from content_analytics.middlewares.splash import SplashContext, get_splash_args, crop_image


class Component(with_metaclass(ABCMeta, object)):
    _message = {}

    def __init__(self, *args, **kwargs):
        super(Component, self).__init__()

    @property
    def message(self):
        return self._message

    @abstractmethod
    def make_requests(self, *args, **kwargs):
        return
        yield

    @abstractmethod
    def get_default_item(self, *args, **kwargs):
        pass

    @abstractmethod
    def process_default_response_status(self, response, callback_prefix='parse', *args, **kwargs):
        pass


class RankingComponent(Component):
    @abstractmethod
    def get_single_product_item(self, *args, **kwargs):
        pass

    @abstractmethod
    def make_single_product_requests(self, url, *args, **kwargs):
        return
        yield


class SingleProductComponent(Component):
    _product_url = None

    @property
    def product_url(self):
        return self._product_url

    @abstractmethod
    def parse_product(self, response):
        return
        yield

    def __init__(self, *args, **kwargs):
        self._product_url = kwargs.get('product_url')

        if self._message:
            self._product_url = self._message.get('url')

        super(SingleProductComponent, self).__init__(*args, **kwargs)

    def get_single_product_item(self, *args, **kwargs):
        return self.get_default_item(*args, **kwargs)

    def make_requests(self, *args, **kwargs):
        if self._product_url:
            for request in self.make_single_product_requests(url=self._product_url):
                yield request

        for request in super(SingleProductComponent, self).make_requests(*args, **kwargs):
            yield request

    def make_single_product_requests(self, url, *args, **kwargs):
        yield MergeRequest(
            url=url,
            item=kwargs.get('item') or self.get_single_product_item(),
            callback=kwargs.get('callback') or self.process_single_page_response
        )

    def process_single_page_response(self, response):
        response_status_results = self.process_single_page_response_status(response)
        if response_status_results:
            return response_status_results
        return self._process_single_page_response(response)

    def _process_single_page_response(self, response):
        for result in self.parse_product(response):
            if isinstance(result, Request):
                yield result
                continue

            if isinstance(result, Item):
                item = cond_set_value(result, 'is_single_result', True)
                yield item
                continue

    def process_single_page_response_status(self, response, *args, **kwargs):
        return self.process_default_response_status(response, *args, **kwargs)


class SearchTermComponent(RankingComponent):
    DEFAULT_SEARCH_TERM_QUANTITY = 100

    _search_term = None
    _quantity = None

    @property
    def search_term(self):
        return self._search_term

    @property
    def quantity(self):
        if self._quantity > self.DEFAULT_SEARCH_TERM_QUANTITY:
            self._quantity = self.DEFAULT_SEARCH_TERM_QUANTITY
        return self._quantity

    @abstractmethod
    def get_search_term_url(self, search_term):
        pass

    @abstractmethod
    def get_search_term_next_page(self, response):
        pass

    @abstractmethod
    def parse_search_term_items(self, response):
        return
        yield

    @abstractmethod
    def parse_search_term_total_matches(self, response):
        pass

    @abstractmethod
    def parse_search_term_results_per_page(self, response):
        pass

    def __init__(self, *args, **kwargs):
        self._search_term = kwargs.get('search_term')
        self._quantity = int(kwargs.get('quantity')) if kwargs.get('quantity') else None

        if self._message:
            self._search_term = self._message.get('search_term').encode('utf-8') if self._message.get('search_term') else None
            self._quantity = int(self._message.get('quantity')) if self._message.get('quantity') else None

        super(SearchTermComponent, self).__init__(*args, **kwargs)

    def get_search_term_item(self, *args, **kwargs):
        return self.get_single_product_item(*args, **kwargs)

    def make_requests(self, *args, **kwargs):
        if self._search_term:
            for request in self.make_search_term_requests(
                search_term=self._search_term,
                quantity=self._quantity
            ):
                yield request

        for request in super(SearchTermComponent, self).make_requests(*args, **kwargs):
            yield request

    def make_search_term_requests(self, search_term, *args, **kwargs):
        yield Request(
                url=kwargs.get('url') or self.get_search_term_url(search_term),
                callback=kwargs.get('callback') or self.process_search_terms_response,
                meta={
                    'search_term': search_term,
                    'remaining': kwargs.get('remaining'),
                    'quantity': kwargs.get('quantity'),
                }
            )

    def process_search_terms_response(self, response):
        response_status_results = self.process_search_term_response_status(response)
        if response_status_results:
            return response_status_results
        return self._process_search_term_response(response)

    def _process_search_term_response(self, response):
        search_term = response.meta.get('search_term')
        quantity = response.meta.get('quantity') or self.DEFAULT_SEARCH_TERM_QUANTITY
        remaining = response.meta.get('remaining') or quantity

        scraped_items_or_urls = []
        total_matches = self.parse_search_term_total_matches(response)
        # Number of results extracted from page
        scraped_result_per_page = self.parse_search_term_results_per_page(response)

        for result in self.parse_search_term_items(response):
            if isinstance(result, Request):
                yield result
                continue

            if isinstance(result, (string_types, Item, tuple)):
                scraped_items_or_urls.append(result)
                continue

        # Number of results actually scraped by scraper
        results_per_page = len(scraped_items_or_urls)
        if results_per_page:
            for rank, item_or_url in enumerate(scraped_items_or_urls[:remaining], quantity - remaining + 1):
                if isinstance(item_or_url, tuple):
                    item = item_or_url[-1]
                elif isinstance(item_or_url, Item):
                    item = item_or_url
                else:
                    item = self.get_search_term_item()
                item = cond_set_value(item, 'ranking', rank)
                item = cond_set_value(item, 'search_term', search_term)
                item = cond_set_value(item, 'total_matches', total_matches)
                item = cond_set_value(item, 'results_per_page', results_per_page)
                item = cond_set_value(item, 'scraped_results_per_page', scraped_result_per_page)
                item = cond_set_value(item, 'is_single_result', False)

                # parse_search_term_items can return:
                # Item - rare case when all product data is available right on the search page
                # url - common case when there are links on search page - other product data parsed on product pages
                # tuple - (url, Item) some fields are filled on search page, other fields - om individual pages

                if isinstance(item_or_url, Item):
                    yield item
                    continue

                if isinstance(item_or_url, (string_types, tuple)):
                    url = item_or_url[0] if isinstance(item_or_url, tuple) else item_or_url
                    for request in self.make_single_product_requests(url=url, item=item):
                        yield request
                    continue

            remaining -= results_per_page
            if remaining > 0:
                next_page = self.get_search_term_next_page(response)
                if next_page:
                    if isinstance(next_page, Request):
                        next_page.meta.update({
                            'quantity': quantity,
                            'remaining': remaining,
                            'search_term': search_term,
                        })
                        yield next_page

                    if isinstance(next_page, string_types):
                        for request in self.make_search_term_requests(
                            search_term=search_term,
                            remaining=remaining,
                            quantity=quantity,
                            url=next_page
                        ):
                            yield request

    def process_search_term_response_status(self, response, *args, **kwargs):
        return self.process_default_response_status(response, *args, **kwargs)


class ShelfPageComponent(RankingComponent):
    DEFAULT_SHELF_PAGE_QUANTITY = 20
    DEFAULT_SHELF_PAGE_COUNT = 1

    _shelf_url = None
    _quantity = None
    _pages_count = None

    @property
    def shelf_url(self):
        return self._shelf_url

    @property
    def quantity(self):
        return self._quantity

    @property
    def pages_count(self):
        return self._pages_count

    @abstractmethod
    def get_shelf_page_next_page(self, response):
        pass

    @abstractmethod
    def parse_shelf_page_items(self, response):
        return
        yield

    @abstractmethod
    def parse_shelf_page_total_matches(self, response):
        pass

    @abstractmethod
    def parse_shelf_page_results_per_page(self, response):
        pass

    def __init__(self, *args, **kwargs):
        self._shelf_url = kwargs.get('shelf_url')
        self._quantity = int(kwargs.get('quantity')) if kwargs.get('quantity') else None
        self._pages_count = int(kwargs.get('pages_count')) if kwargs.get('pages_count') else None

        if self._message:
            self._shelf_url = self._message.get('shelf_url')
            self._quantity = int(self._message.get('quantity')) if self._message.get('quantity') else None
            self._pages_count = int(self._message.get('pages_count')) if self._message.get('pages_count') else None

        super(ShelfPageComponent, self).__init__(*args, **kwargs)

    def get_shelf_page_item(self, *args, **kwargs):
        return self.get_single_product_item(*args, **kwargs)

    def make_requests(self, *args, **kwargs):
        if self._shelf_url:
            for request in self.make_shelf_page_requests(
                url=self._shelf_url,
                quantity=self._quantity,
                pages_count=self._pages_count,
            ):
                yield request

        for request in super(ShelfPageComponent, self).make_requests(*args, **kwargs):
            yield request

    def make_shelf_page_requests(self, url, *args, **kwargs):
        yield Request(
            url=url,
            callback=kwargs.get('callback') or self.process_shelf_page_response,
            meta={
                'quantity': kwargs.get('quantity'),
                'remaining': kwargs.get('remaining'),
                'pages_count': kwargs.get('pages_count'),
            }
        )

    def process_shelf_page_response(self, response):
        response_status_results = self.process_shelf_page_response_status(response)
        if response_status_results:
            return response_status_results
        return self._process_shelf_page_response(response)

    def _process_shelf_page_response(self, response):
        quantity = response.meta.get('quantity')
        remaining = response.meta.get('remaining')
        pages_count = response.meta.get('pages_count')

        scraped_items_or_urls = []
        total_matches = self.parse_shelf_page_total_matches(response)
        scraped_result_per_page = self.parse_shelf_page_results_per_page(response)
        for result in self.parse_shelf_page_items(response):
            if isinstance(result, Request):
                yield result
                continue

            if isinstance(result, (string_types, Item)):
                scraped_items_or_urls.append(result)
                continue

        results_per_page = len(scraped_items_or_urls)
        if results_per_page:
            if not quantity:
                # TODO: need to discuss priority of `pages_count` and `quantity`
                # TODO: ability to parse tuples (url, Item) to partially fill items on shelf page (see searchterms)
                # quantity = results_per_page * pages_count if pages_count else self.DEFAULT_SHELF_PAGE_QUANTITY
                quantity = results_per_page * (pages_count if pages_count else self.DEFAULT_SHELF_PAGE_COUNT)

            if not remaining:
                remaining = quantity

            for rank, item_or_url in enumerate(scraped_items_or_urls[:remaining], quantity - remaining + 1):
                item = item_or_url if isinstance(item_or_url, Item) else self.get_shelf_page_item()
                item = cond_set_value(item, 'ranking', rank)
                item = cond_set_value(item, 'total_matches', total_matches)
                item = cond_set_value(item, 'results_per_page', results_per_page)
                item = cond_set_value(item, 'scraped_results_per_page', scraped_result_per_page)
                item = cond_set_value(item, 'is_single_result', False)

                if isinstance(item_or_url, Item):
                    yield item
                    continue

                if isinstance(item_or_url, string_types):
                    for request in self.make_single_product_requests(url=item_or_url, item=item):
                        yield request
                    continue

            remaining -= results_per_page
            if remaining > 0:
                next_page = self.get_shelf_page_next_page(response)
                if next_page:
                    if isinstance(next_page, Request):
                        next_page.meta.update({
                            'quantity': quantity,
                            'remaining': remaining,
                            'pages_count': pages_count,
                        })
                        yield next_page

                    if isinstance(next_page, string_types):
                        for request in self.make_shelf_page_requests(
                                url=next_page,
                                quantity=quantity,
                                remaining=remaining,
                                pages_count=pages_count
                        ):
                            yield request

    def process_shelf_page_response_status(self, response, *args, **kwargs):
        return self.process_default_response_status(response, *args, **kwargs)


class ScreenshotComponent(SingleProductComponent, SearchTermComponent,
                          ShelfPageComponent):

    def __init__(self, make_screenshot=False, *args, **kwargs):
        # TODO turn back on
        # self.make_screenshot = make_screenshot in ('1', 1, True, 'True', 'true')
        self.make_screenshot = False
        self.width = kwargs.get('height', '1696')
        self.height = kwargs.get('width', '1280')
        self.timeout = kwargs.get('timeout', 180)
        self.wait = kwargs.get("wait", 5)  # splash wait time
        self.crop_left = kwargs.get('crop_left', 0)
        self.crop_top = kwargs.get('crop_top', 0)
        self.crop_width = kwargs.get('crop_width', 0)
        self.crop_height = kwargs.get('crop_height', 0)
        self.render_all = kwargs.get('render_all', True) # whether to render the whole page
        self.debug_screenshot_path = kwargs.get("debug_path")  # where to save image locally for debugging
        if self.debug_screenshot_path:
            if os.path.isdir(self.debug_screenshot_path):
                self.debug_screenshot_path = os.path.join(self.debug_screenshot_path, 'screenshot.png')
            else:
                if not self.debug_screenshot_path.endswith('.png'):
                    self.debug_screenshot_path += '.png'
        super(ScreenshotComponent, self).__init__(*args, **kwargs)

    def make_requests(self, *args, **kwargs):
        for request in super(ScreenshotComponent, self).make_requests(args, kwargs):
            yield request
            if self.make_screenshot and isinstance(request, MergeRequest):
                for splash_request in self.make_splash_single_request(request):
                    yield splash_request

    def make_splash_single_request(self, request, *args, **kwargs):
        with SplashContext(request) as splash_request:
            self._fill_splash_args(splash_request)
            yield splash_request.replace(url=self.product_url, callback=self.parse_image)

    def _make_splash_shelf_request(self, shelf_url, item, *args, **kwargs):
        req = MergeRequest(shelf_url, item=item, callback=self.parse_image)
        with SplashContext(req) as splash_request:
            self._fill_splash_args(splash_request)
            return splash_request

    def parse_image(self, response):
        item = response.meta.get('item')
        cropped_image = crop_image(response.body, self.crop_height, self.crop_width,
                                   self.crop_left, self.crop_top)
        if self.debug_screenshot_path:
            self._save_locally(cropped_image, self.debug_screenshot_path)
        cond_set_value(item, 'screenshot', base64.b64encode(cropped_image))
        return item

    def _process_shelf_page_response(self, response):
        # yield merge req with screenshot of shelf page for item with ranking = 1
        yielded_screenshot_request = False
        for req_or_item in super(ScreenshotComponent, self)._process_shelf_page_response(response):
            if self.make_screenshot and not yielded_screenshot_request:
                product = fetch_product_from_req_or_item(req_or_item)
                if product and product.get('ranking') == 1:
                    splash_request = self._make_splash_shelf_request(shelf_url=self.shelf_url,
                                                                     item=product)
                    yield splash_request
                    yielded_screenshot_request = True
            yield req_or_item

    def _process_search_term_response(self, response):
        # yield merge req with screenshot of searchterm page for item with ranking = 1
        yielded_screenshot_request = False
        for req_or_item in super(ScreenshotComponent, self)._process_search_term_response(response):
            if self.make_screenshot and not yielded_screenshot_request:
                product = fetch_product_from_req_or_item(req_or_item)
                if product and product.get('ranking') == 1:
                    splash_request = self._make_splash_shelf_request(shelf_url=self.SEARCH_TERM_SCREENSHOT_URL
                                                                     .format(search_term=self.search_term),
                                                                     item=product)
                    yielded_screenshot_request = True
                    yield splash_request
            yield req_or_item

    def _save_locally(self, image, path):
        try:
            with open(path, 'wb') as handle:
                handle.write(image)
                self.logger.debug("Saved screenshot to {}".format(self.debug_screenshot_path))
        except (OSError, IOError) as e:
            self.logger.warning("Couldn't save screenshot locally: {}".format(e.message))

    def _fill_splash_args(self, request):
        request.meta.setdefault('splash', {})['endpoint'] = 'render.png'
        args = get_splash_args(request)
        args['render_all'] = self.render_all
        args['viewport'] = 'x'.join((self.width, self.height))
        args['timeout'] = self.timeout
        args['wait'] = self.wait
        return request


class BaseProductsSpider(with_metaclass(
    ABCMeta,
    ScreenshotComponent,
    SingleProductComponent,
    SearchTermComponent,
    ShelfPageComponent,
    Spider
)):

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def allowed_domains(self):
        pass

    def __init__(self, *args, **kwargs):
        if kwargs.get('message'):
            self._message = kwargs.get('message')
        # sqs_tools_jobs bot
        if kwargs.get('slack_username'):
            self.slack_username = kwargs['slack_username']

        self.crawl_date = kwargs.get('crawl_date')
        self.summary = kwargs.get('summary') in ('True', 'true', '1', True)

        if self.crawl_date:
            try:
                self.crawl_date = date_parser.parse(self.crawl_date, dayfirst=False)  # yyyy.mm.dd
            except ValueError:
                self.crawl_date = None

        super(BaseProductsSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        return super(BaseProductsSpider, self).make_requests()

    def get_default_item(self, *args, **kwargs):
        return SiteProductItem()

    # pylint: disable=E1102
    def process_default_response_status(self, response, callback_prefix='parse', *args, **kwargs):
        meta = response.meta
        status = response.status

        handle_all_statuses = meta.get('handle_httpstatus_all') or getattr(self, 'handle_httpstatus_all', None)
        allowed_statuses = getattr(self, 'handle_httpstatus_list', meta.get('handle_httpstatus_list', []))

        if not handle_all_statuses and status in allowed_statuses:
            callback_name = '_'.join([callback_prefix, str(status)])
            callback = getattr(self, callback_name, None)
            if callable(callback):
                return callback(response)

