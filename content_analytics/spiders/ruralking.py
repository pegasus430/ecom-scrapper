import re

import json

from content_analytics.middlewares.mergeitem import MergeRequest
from content_analytics.middlewares.proxy import ProxyContext
from content_analytics.data_parsing.buyer_reviews.bazaarvoice_reviews import BazaarvoiceReviews
from content_analytics.spiders import BaseProductsSpider
from content_analytics.utils import replace_http_with_https, cond_set_value, catch_json_exceptions


class RuralkingProductsSpider(BaseProductsSpider):
    name = 'ruralking_products'
    allowed_domains = ['www.ruralking.com', BazaarvoiceReviews.DOMAIN, 'search.unbxd.io']

    custom_settings = {
        'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/65.0.3325.162 Safari/537.36"
    }

    SEARCH_URL = "https://search.unbxd.io/6fe978d3ea6f06bef1dcac48daa881bf/dev-ruralking-com701651515397423" \
                 "/search?&q={search_term}&rows={n_products_per_page}&start={start_rank_no}&fields=productUrl" \
                 "&api-key=6fe978d3ea6f06bef1dcac48daa881bf"
    N_PRODUCTS_PER_PAGE = 24

    # ############################
    # Requests generator methods #
    # ############################
    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(RuralkingProductsSpider, self).make_single_product_requests(url, *args, **kwargs))
        request = request.replace(url=replace_http_with_https(request.url))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(RuralkingProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_shelf_page_requests(self, url, *args, **kwargs):
        request = next(super(RuralkingProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    ################################
    # Search term abstract methods #
    ################################
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term, start_rank_no=0,
                                      n_products_per_page=self.N_PRODUCTS_PER_PAGE)

    def get_search_term_next_page(self, response):
        start_rank_no = response.meta.get('search_results_obj', {}).get('response', {}).get('start', 0) + self.N_PRODUCTS_PER_PAGE

        return self.SEARCH_URL.format(search_term=response.meta.get('search_term'), start_rank_no=start_rank_no,
                                      n_products_per_page=self.N_PRODUCTS_PER_PAGE)

    def parse_search_term_items(self, response):
        search_results_obj = response.meta['search_results_obj']
        return [product.get('productUrl') for product in search_results_obj.get('response', {}).get('products', [])]

    def parse_search_term_total_matches(self, response):
        search_results_obj = self._get_json_from_string(response.body)
        # share to all search methods
        response.meta['search_results_obj'] = search_results_obj

        total_matches = search_results_obj.get('response', {}).get('numberOfProducts', 0)
        return total_matches

    def parse_search_term_results_per_page(self, response):
        return self.N_PRODUCTS_PER_PAGE

    ################################
    # Shelf pages abstract methods #
    ################################
    def get_shelf_page_next_page(self, response):
        next_page_url = response.xpath('//*[contains(@class, "next")]//a/@href').extract_first()
        return next_page_url

    def parse_shelf_page_items(self, response):
        links = response.xpath('//*[contains(@class, "product-item-link")]/@href').extract()
        return [response.urljoin(link) for link in links]

    def parse_shelf_page_total_matches(self, response):
        total_matches = response.xpath('//*[contains(@class, "toolbar-number")]//text()').extract()
        return total_matches[-1] if total_matches else 0

    def parse_shelf_page_results_per_page(self, response):
        results_per_page_container = response.xpath('//*[contains(@selected, "selected")]//@value').extract_first()
        results_per_page = re.findall(r'\d+$', str(results_per_page_container))
        return int(results_per_page[0]) if results_per_page else None

    ###################
    # Parsing methods #
    ###################
    def parse_product(self, response):
        product = response.meta.get('item')

        product.update({
            'sku': response.xpath('//*[contains(@itemprop, "sku")]//text()').extract_first(),
            'title': self._get_meta_field(response, 'og:title'),
            'short_description': response.xpath('//*[contains(@class, "description")]//p/text()').extract_first(),
            'price_amount': self._parse_price_amount(response),
            'price_currency': self._get_meta_field(response, 'product:price:currency'),

            'image_urls': self._parse_image_urls(response),
            'image_url': self._get_meta_field(response, 'og:image'),

            'features': response.xpath('//*[contains(@class, "description")]//ul/li/text()').extract(),
            'specs': self._parse_specs(response),
            'brand': self._get_meta_field(response, 'og:brand'),
            'department': self._get_meta_field(response, 'og:category'),
            'departments': response.xpath('//*[contains(@class, "breadcrumbs")]'
                                          '//li[not(@class="home")]/a/span/text()').extract(),

            'is_out_of_stock': response.xpath('//*[contains(@title, "Availability")]/span/text()')
                                   .extract_first() != 'In stock',
            'site_online': True,
            'site_online_out_of_stock': product.get('is_out_of_stock'),
            'in_stores': 'In Stores' in response.xpath('//*[contains(@title, "product-status")]//text()').extract(),
            'variants': self._parse_variants(response),
        })

        # buyer_reviews
        sku = product.get('sku')
        if sku:
            yield MergeRequest(
                url=BazaarvoiceReviews.compile_url(product_id=sku,
                                                   passkey='cavYXG7YTBHgEHTW3tlWsgRqHCxBTqq3Sa1Gf2tucJuYo',
                                                   displaycode='16397-en_us'),
                item=product,
                callback=self._on_reviews_response
            )

        yield product

    @staticmethod
    def _on_reviews_response(response):
        product = response.meta.get('item')
        cond_set_value(product, 'buyer_reviews', BazaarvoiceReviews.parse_reviews(response.body))

    @staticmethod
    def _get_meta_field(response, field):
        return response.xpath('/html/head/meta[@property="{field}"]/@content'.format(field=field)).extract_first()

    def _parse_price_amount(self, response):
        price_amount = self._get_meta_field(response, 'product:price:amount')
        return float(price_amount) if price_amount else None

    @staticmethod
    def _parse_image_urls(response):
        images = re.findall(r'(?<=full\":\")(\S+?\.jpg)', response.body)
        return [img.replace('\\/', '/') for img in images]

    @staticmethod
    def _parse_specs(response):
        keys = response.xpath('//*[contains(@id, "product-attribute-specs-table")]'
                              '//tbody/tr/th[not(@style="display: none;")]/text()').extract()
        values = response.xpath('//*[contains(@id, "product-attribute-specs-table")]'
                                '//tbody/tr/td[not(@style="display: none;")]/text()').extract()
        if keys and values:
            return dict(zip(keys, map(unicode.strip, values)))

    @staticmethod
    @catch_json_exceptions
    def _get_json_from_string(string):
        return json.loads(string)

    def _parse_variants(self, response):
        def _parse_properties(_attributes, _product_id):
            _properties = {}
            for attribute in _attributes.itervalues():
                options = attribute.get('options', [])  # actual values, i.e. L, XL, etc.
                for option in options:
                    product_has_such_variant = _product_id in option.get('products', [])
                    property_name = attribute.get('code')
                    if product_has_such_variant and property_name:
                        _properties[property_name] = option.get('label')
                        break
            return _properties

        variants = []
        variants_json = re.search(r'"spConfig": (.*),', response.body)
        if not variants_json:
            return variants
        variants_json = variants_json.group(1)
        variants_obj = self._get_json_from_string(variants_json)

        skus = variants_obj.get('index', {})
        prices = variants_obj.get('optionPrices', {})
        attributes = variants_obj.get('attributes', {})  # color, size, etc.

        product_ids = skus.keys()
        for product_id in product_ids:
            variant = {'sku': product_id,
                       'price': prices.get(product_id, {}).get('finalPrice', {}).get('amount', 0),
                       'in_stock': True,  # Now only In_stock variants are displayable
                       'properties': _parse_properties(attributes, product_id),
                       # no default 'selected' on web page
                       }
            variants.append(variant)
        return variants
