import re
import json
from urlparse import urlparse
from scrapy import Request
import traceback

from . import BaseProductsSpider
from ..utils import catch_dictionary_exception, catch_json_exceptions


class RealCanadianSuperStoreProductsSpider(BaseProductsSpider):

    name = "realcanadiansuperstore_products"
    allowed_domains = ['realcanadiansuperstore.ca']

    # Urls
    SEARCH_URL = 'https://www.realcanadiansuperstore.ca/search/?search-bar={search_term}'
    NEXT_URL = 'https://www.realcanadiansuperstore.ca{next_url}&itemsLoadedonPage={offset}'

    HOME_URL = 'https://www.realcanadiansuperstore.ca'

    STORE_URL = 'https://www.realcanadiansuperstore.ca/store-locator/locations/all?showNonShoppable=true'

    SEL_STORE_URL = 'https://www.realcanadiansuperstore.ca/booking/save?CSRFToken={token}'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/65.0.3325.181 Safari/537.36'
    }

    def __init__(self, *args, **kwargs):
        super(RealCanadianSuperStoreProductsSpider, self).__init__(*args, **kwargs)
        self.store_id = kwargs.get('store', '1009')
        self.zip_code = kwargs.get('zip_code', 'K1A 0G9')
        self.total_matches = 0
        self._original_product_url = self._product_url

    def start_requests(self):
        yield Request(
            url=self.HOME_URL,
            callback=self._parse_token
        )

    def _parse_token(self, response):
        token = response.xpath(
            '//input[@name="CSRFToken"]/@value'
        ).extract_first()
        cart_id = response.xpath(
            '//body[@data-cart-id]/@data-cart-id'
        ).extract_first()
        if not all([cart_id, token]):
            self.logger.warning('Can not extract the token and cart_id: {}'.format(response.url))
            return
        response.meta.update({
            'token': token,
            'cart_id': cart_id
        })
        if not self.store_id:
            return Request(
                url=self.STORE_URL,
                callback=self._parse_stores,
                meta=response.meta,
                dont_filter=True
            )
        return self._select_store(response, self.store_id)

    def _select_store(self, response, store_id):
        return Request(
            url=self.SEL_STORE_URL.format(token=response.meta.get('token')),
            method='POST',
            body=json.dumps({
                "cartId": response.meta.get('cart_id'),
                "pickupLocationId": store_id,
                "startTime": "",
                "endTime": "",
                "storeName": ""
            }),
            meta=response.meta,
            callback=self._start_requests,
            headers={
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.8',
                'ADRUM': 'isAjax:true',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            }
        )

    @catch_json_exceptions
    def _parse_stores(self, response):
        res = json.loads(response.body)
        first_store = None
        zip_code = None
        # find store based on zip_code
        for idx, store in enumerate(res.get('searchResult', [])):
            if not store.get('details', {}).get('isStore'):
                continue
            if idx == 0:
                first_store = store.get('details', {}).get('storeID')
                zip_code = store.get('details', {}).get('postalCode', '')
            if store.get('details', {}).get('postalCode', '').replace(' ', '').lower() \
                    == self.zip_code.replace(' ', '').lower():
                return self._select_store(response, store.get('details', {}).get('storeID'))

        if first_store:
            self.store_id = first_store
            self.zip_code = zip_code
            self.logger.warning(
                'There are no stores matched with zip_code: {}, so select first store: {}'.format(
                    self.zip_code,
                    first_store
                )
            )
            return self._select_store(response, first_store)

        self.logger.warning('There are no available stores')

    def _start_requests(self, response):
        for req in super(RealCanadianSuperStoreProductsSpider, self).start_requests():
            yield req

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        product.update(
            {
                # General fields
                'title': self._parse_title(response),
                'short_description': self._parse_description(response),
                'image_urls': self._parse_image_urls(response),
                'store': self.store_id,
                'zip_code': self.zip_code,

                # CH fields
                'nutrition_fact_count': self._parse_nutrition_fact_count(response),
                'ingredients': self._parse_ingredients(response),

                # Price
                'price_amount': self._parse_price_amount(response),
                'price_currency': self._parse_price_currency(response),
                'was_price': self._parse_was_price(response),
                'now_price': self._parse_price_amount(response),
                'temp_price_cut': self._parse_temp_price_cut(response),

                # Categorization
                'brand': self._parse_brand(response),
                'departments': self._parse_departments(response),

                # Identification
                'site_product_id': self._parse_site_product_id(response.url),

                # Availability
                'is_out_of_stock': False,
                'in_stores': True,
                'site_online': True,
            }
        )
        return product

    @staticmethod
    def _parse_nutrition_fact_count(response):
        return len(
            response.xpath(
                '//div[contains(@class, "nutrition-fact-attr") and contains(@class, "hidden-sm")]'
                '//div[contains(@class, "main-nutrition-attr") or contains(@class, "sub-nutrition-attr")]'
            )
        )

    @staticmethod
    def _parse_ingredients(response):
        ingredients = response.xpath(
            '//div[@class="ingredients-list"]/text()[normalize-space(.)]'
        ).extract_first()
        return [
            i.strip()
            for i in ingredients.split(',')
        ] if ingredients else None

    @staticmethod
    def _parse_title(response):
        title = response.xpath('//h1[@class="product-name"]/text()[normalize-space(.)]').extract_first()
        return title.strip() if title else None

    @staticmethod
    def _parse_brand(response):
        brand = response.xpath('//span[@class="product-sub-title"]/text()[normalize-space(.)]').extract_first()
        return brand.strip() if brand else None

    @staticmethod
    def _parse_departments(response):
        return response.xpath(
            '//ul[contains(@class, "bread-crumb")]/li[contains(@class, "item")]/a/text()'
        ).extract()

    @catch_dictionary_exception
    def _parse_description(self, response):
        description = response.xpath(
            '//div[contains(@class, "row-product-description")]/p/text()[normalize-space(.)]'
        ).extract()
        return ''.join(description)

    @catch_dictionary_exception
    def _parse_image_urls(self, response):
        image_urls = response.xpath(
            '//span[@data-image-renderer="pdpZoom"]//img/@srcset'
        ).extract()
        return image_urls

    @staticmethod
    def _parse_sku(response):
        sku = response.xpath(
            '//span[@class="product-number"]//span[@class="number"]/text()'
        ).extract_first()
        return sku.strip() if sku else None

    @staticmethod
    def _parse_site_product_id(url):
        path = urlparse(url).path.split('/')[-1]
        product_id = re.search(r'(\d+)', path)
        return product_id.group(1) if product_id else None

    # Price parsing
    def _parse_price_amount(self, response):
        price = response.xpath(
            '//div[@class="pricing-module"]//span[contains(@class, "sale-price-text")]/text() | '
            '//div[@class="pricing-module"]//span[contains(@class, "reg-price-text")]/text()'
        ).re_first(r'\d{1,3}[,\d{3}]*\.?\d*')
        try:
            return float(price.replace(',', ''))
        except Exception:
            self.logger.warning('Error parsing the price: {}'.format(traceback.format_exc()))

    def _parse_temp_price_cut(self, response):
        if self._parse_was_price(response) and self._parse_price_amount(response):
            return False
        return True

    @staticmethod
    def _parse_was_price(response):
        return response.xpath(
            '//div[@class="pricing-module"]//span[contains(@class, "sale-price-text")]/text()'
        ).re_first(r'\d{1,3}[,\d{3}]*\.?\d*')

    @staticmethod
    def _parse_price_currency(response):
        price_currency = response.xpath(
            '//div[@class="pricing-module"]//sup[@class="sale-price-unit"]/text() | '
            '//div[@class="pricing-module"]//sup[@class="reg-price-unit"]/text()'
        ).extract_first()
        return price_currency if price_currency else 'CAD'

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        links = response.xpath(
            '//a[@class="product-name"]/@href'
        ).extract()
        for link in links:
            yield response.urljoin(link)

    def get_search_term_next_page(self, response):
        next_url = response.meta.get('next_url')
        current_page = response.meta.get('current_page', 1)
        if current_page * 48 >= self.total_matches:
            return
        if not next_url:
            next_url = response.xpath(
                '//button[contains(@class, "btn-show-more")]/@data-ajax-url'
            ).extract_first()
        if not next_url:
            self.logger.warning('Can not extract next_url from {}'.format(response.url))
            return
        current_page += 1
        url = response.urljoin(self.NEXT_URL.format(next_url=next_url, offset=current_page*48))
        response.meta.update({
            'current_page': current_page,
            'next_url': next_url
        })
        return url

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//span[@class="result-total"]/text()').re_first(r'\d+')
        return int(total_matches) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        total_matches = response.xpath(
            '//div[@class="wrapper-see-filter-results"]//span[@class="count"]/text()'
        ).re_first(r'\d+')
        return int(total_matches) if total_matches else None

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
