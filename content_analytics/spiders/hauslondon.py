import re
import json
import urlparse

from . import BaseProductsSpider
from ..utils import catch_dictionary_exception, catch_json_exceptions


class HauslondonProductsSpider(BaseProductsSpider):
    name = "hauslondon_products"
    allowed_domains = ['hauslondon.com']

    # Urls
    SEARCH_URL = 'https://hauslondon.com/search?q={search_term}&type=product'

    # Constants
    PRICE_CURRENCY = 'GBP'

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        # Embedded json
        data = self._parse_main_inline_json(response)

        sku = self._parse_sku(data)

        product.update(
            {
                # General fields
                'title': self._parse_title(response, data),
                'short_description': self._parse_description(response, data),
                'image_urls': self._parse_image_urls(data),

                # CH fields
                'features': self._parse_features(response),

                # Price
                'price_amount': self._parse_price_amount(data),
                'price_currency': self._parse_price_currency(),

                # Categorization
                'brand': self._parse_brand(response),
                'departments': self._parse_departments(response),

                # Identification
                'sku': sku,
                'reseller_id': sku,

                # Availability
                'in_stores': True,
                'site_online': True,
                'no_longer_available': self._parse_no_longer_available(data)
            }
        )

        yield product

    # Parsing inline product json blocs
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        return json.loads(re.search(r'HAUS.product =(.*\});Object', response.body).group(1))

    # Parsing json data
    def _parse_title(self, response, data):
        if isinstance(data, dict):
            return data.get('title')

        return response.xpath(
            '//h1[contains(@class, "product-title")]/text()'
        ).extract_first()

    def _parse_brand(self, response):
        return response.xpath(
            '//div[@class="product--header"]//h2[@class="h3"]//a/text()'
        ).extract_first()

    def _parse_departments(self, response):
        return response.xpath(
            '//nav[contains(@class, "breadcrumb")]//li//a//span/text()'
        ).extract()

    @catch_dictionary_exception
    def _parse_description(self, response, data):
        if data:
            return data['description']

        description = response.xpath(
            '//div[contains(@class, "product--description")]//p/text()'
        ).extract()

        return ''.join(description) if description else None

    def _parse_features(self, response):
        features = []

        for f in response.xpath('//div[contains(@class, "product--description")]//p'):
            title = f.xpath('./strong/text()').extract()
            value = f.xpath('./text()').extract()
            if title and value:
                features.append(title[0] + ': ' + value[0])

        return features

    @catch_dictionary_exception
    def _parse_image_urls(self, data):
        return ['https:' + image for image in data['images']]

    @catch_dictionary_exception
    def _parse_no_longer_available(self, data):
        return not data['available']

    # Codes parsing
    @catch_dictionary_exception
    def _parse_sku(self, data):
        if isinstance(data['variants'], list):
            return data['variants'][0]['sku']

    # Price parsing
    @catch_dictionary_exception
    def _parse_price_amount(self, data):
        return float(data['price']) / 100

    @catch_dictionary_exception
    def _parse_price_currency(self):
        return self.PRICE_CURRENCY

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    @catch_json_exceptions
    def parse_search_term_items(self, response):
        items = response.xpath(
            '//div[contains(@class, "product-li")]'
            '//a[@class="product-grid-image"]/@href'
        ).extract()

        for item in items:
            yield urlparse.urljoin(response.url, item)

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//ul[contains(@class, "pagination-custom")]'
            '//li//a[contains(@title, "Next")]/@href'
        ).extract_first()

        if next_page:
            return urlparse.urljoin(response.url, next_page)

    @catch_json_exceptions
    def parse_search_term_total_matches(self, response):
        # total matches does not exist on search page
        return

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        pass

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
