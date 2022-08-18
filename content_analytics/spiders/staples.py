import json
import re

from scrapy import Field

from content_analytics.items import BuyerReviews, SiteProductItem
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.utils import (catch_dictionary_exception,
                                     catch_json_exceptions)


class StaplesProductItem(SiteProductItem):
    layout_issue = Field()


class StaplesProductsSpider(BaseProductsSpider):
    name = 'staples_products'
    allowed_domains = ['www.staples.com', 'static.www.turnto.com']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
            'X-Forwarded-For': '127.0.0.1'
        }
    }

    # Urls
    REVIEW_URL = 'https://static.www.turnto.com/sitedata/jwmno8RkY7SXz4jsite/v4_3/{sku}/d/en_US/catitemreviewshtml'
    SEARCH_URL = 'https://www.staples.com/{search_term}/directory_{search_term}?pn={page}'
    SHELF_URL = 'https://www.staples.com/{directory_name}/cat_{category_id}?pn={page}'
    PRODUCT_URL = 'https://www.staples.com/product_{sku}'

    def __init__(self, *args, **kwargs):
        super(StaplesProductsSpider, self).__init__(*args, **kwargs)
        if getattr(self, '_shelf_url', None):
            self._shelf_url = self.get_shelf_url(self._shelf_url, 1)
        elif getattr(self, '_product_url', None):
            self._product_url = self.get_product_url(self._product_url)

    def get_default_item(self, *args, **kwargs):
        return StaplesProductItem()

    def parse_product(self, response):
        product = response.meta.get('item')
        product['invalid_url'] = not self.is_valid_url(response.request.url)

        if product['invalid_url']:
            yield product
            return

        data = self.extract_json_data(response)

        product['layout_issue'] = not data

        if product['layout_issue']:
            yield product
            return

        product.update(
            {
                'title': self.parse_title(data),
                'departments': self.parse_departments(data),
                'zip_code': self.parse_zip_code(data),
                'price_currency': self.parse_price_currency(data),
                'image_urls': self.parse_image_urls(data),
                'upc': self.parse_upc(data),
                'sku': self.parse_sku(data),
                'model': self.parse_model(data),
                'price_amount': self.parse_price_amount(data),
                'brand': self.parse_brand(data),
                'is_out_of_stock': self.parse_is_out_of_stock(data),
                'url': self.parse_url(data),
                'bullets': self.parse_bullets(data),
                'specs': self.parse_specs(data),
                'temp_price_cut': self.parse_temp_price_cut(data),
                'short_description': self.parse_short_description(data),
                'long_description': self.parse_long_description(data),
                'site_online': True,
                'site_online_out_of_stock': self.parse_is_out_of_stock(data)
            }
        )
        if product['sku']:
            yield MergeRequest(
                self.REVIEW_URL.format(
                    sku=product['sku']
                ),
                item=product,
                callback=self.parse_buyer_reviews
            )

            variant_skus = self.parse_variant_skus(response, product['sku'])
            if variant_skus:
                product.update(
                    {
                        'variants': [{
                            'image_url': product.get('image_urls')[0] if product.get('image_urls') else None,
                            'in_stock': not product.get('is_out_of_stock'),
                            'price': product.get('price_amount'),
                            'properties': self.parse_variant_properties(data),
                            'selected': True,
                            'sku_id': product.get('sku'),
                            'upc': product.get('upc')
                        }]
                    }
                )
            for variant_sku in variant_skus:
                yield MergeRequest(
                    self.PRODUCT_URL.format(
                        sku=variant_sku
                    ),
                    item=product,
                    callback=self.parse_variant_data
                )

    def get_product_url(self, url):
        sku = re.search('product_([^/]+)', url)
        if sku:
            return self.PRODUCT_URL.format(
                sku=sku.group(1)
            )

    def parse_buyer_reviews(self, response):
        product = response.meta.get('item')
        try:
            buyer_reviews = BuyerReviews(
                stars={
                    index: int(response.xpath(
                        '//div[@id="TTreviewSummaryBreakdown-{}"]/text()'.format(index)
                    ).extract_first()) for index in range(1, 5+1)
                    },
                count=int(response.xpath('//div[@class="TTreviewCount"]/text()').re_first(r'[\d,]+').replace(',', '')),
                average=float(response.xpath('//span[@id="TTreviewSummaryAverageRating"]').re_first(r'\d\.\d'))
            )
            product['buyer_reviews'] = buyer_reviews
        except:
            self.log('Empty buyer_reviews')

    def parse_variant_data(self, response):
        product = response.meta.get('item')
        data = self.extract_json_data(response)
        product['variants'].append(
            {
                'image_url': self.parse_image_urls(data)[0],
                'in_stock': not self.parse_is_out_of_stock(data),
                'price': self.parse_price_amount(data),
                'properties': self.parse_variant_properties(data),
                'selected': False,
                'sku_id': self.parse_sku(data),
                'upc': self.parse_upc(data)
            }
        )

    @staticmethod
    def parse_variant_skus(response, base_sku):
        variants = response.xpath('//div[@class="skuset"]//*[@data-sku]/@data-sku').extract()
        if variants:
            variants.remove(base_sku)
        return variants

    # Check if product_url correct
    @staticmethod
    def is_valid_url(url):
        return re.match('https?://www.staples.com/(?:.*/)?product_([^/]+)', url.split('?')[0])

    # Extract json block
    @catch_json_exceptions
    def extract_json_data(self, response):
        return json.loads(response.xpath('//div[@id="analyticsItemData"]/@content').extract_first())

    # Parse fields from json block above
    @catch_dictionary_exception
    def parse_url(self, data):
        return data['product']['seoData']['canonical']

    @catch_dictionary_exception
    def parse_title(self, data):
        return data['product']['name']

    @catch_dictionary_exception
    def parse_departments(self, data):
        return [breadcrumb['displayName'] for breadcrumb in data['product']['breadcrumb']]

    @catch_dictionary_exception
    def parse_zip_code(self, data):
        return data['price']['zipCode']

    @catch_dictionary_exception
    def parse_price_currency(self, data):
        return data['price']['currency']

    @catch_dictionary_exception
    def parse_price_amount(self, data):
        return data['price']['item'][0]['nowPrice']

    @catch_dictionary_exception
    def parse_image_urls(self, data):
        return [image_url.replace('?$std$', '') for image_url in data['product']['images']['standard']]

    @catch_dictionary_exception
    def parse_upc(self, data):
        return data['product']['upcCode']

    @catch_dictionary_exception
    def parse_model(self, data):
        return data['product']['manufacturerPartNumber']

    @catch_dictionary_exception
    def parse_sku(self, data):
        return data['itemID']

    @catch_dictionary_exception
    def parse_brand(self, data):
        return data['product']['manufacturerName']

    @catch_dictionary_exception
    def parse_is_out_of_stock(self, data):
        return data['inventory']['items'][0]['productIsOutOfStock']

    @catch_dictionary_exception
    def parse_bullets(self, data):
        return '\n'.join([x.strip() for x in data['product']['description']['bullets']])

    @catch_dictionary_exception
    def parse_specs(self, data):
        specs = {}
        for spec in data['product']['description']['specification']:
            specs[spec['name']] = spec['value']
        return specs if specs else None

    @catch_dictionary_exception
    def parse_temp_price_cut(self, data):
        return data['price']['item'][0]['data']['priceInfo'][0]['totalSavings']

    @catch_dictionary_exception
    def parse_short_description(self, data):
        description = data['product']['description'].get('paragraph')
        headliner = data['product']['description'].get('headliner')
        if headliner:
            return '<font size="4">' + headliner[0] + '</font><br/>' + (description[0] if description else '')
        return description[0] if description else None

    @catch_dictionary_exception
    def parse_long_description(self, data):
        return data['product']['description']['expandedDescr'][0]

    @catch_dictionary_exception
    def parse_variant_properties(self, data):
        return {'option_name': data['product']['description']['skuSetDisplayDescription'][0]}

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term, page=1)

    def parse_search_term_items(self, response):
        sku_list = response.xpath(
            '//div[@class="stp--new-product-tile-container desktop"]/div[@class="tile-container"]/@id'
        ).extract()
        for sku in sku_list:
            yield self.PRODUCT_URL.format(sku=sku)

    def get_search_term_next_page(self, response):
        if response.xpath('//input[@id="lastPage" and @value="false"]'):
            return self.SEARCH_URL.format(
                search_term=response.meta.get('search_term'),
                page=int(response.xpath('//input[@id="pagenum"]/@value').extract_first()) + 1
            )

    def parse_search_term_total_matches(self, response):
        return int(response.xpath('//span[@class="results-number"]/text()').re_first(r'\d+'))

    def parse_search_term_results_per_page(self, response):
        return

    # Shelf component
    def get_shelf_url(self, url, page):
        def get_directory_name(url):
            return re.search(r'staples\.com\/(.+?)\/cat_', url).group(1)
        def get_category_id(url):
            return re.search(r'staples\.com\/.+?\/cat_([\w\d]+)', url).group(1)
        return self.SHELF_URL.format(
            directory_name=get_directory_name(url),
            category_id=get_category_id(url),
            page=page
        )

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        if response.xpath('//input[@id="lastPage" and @value="false"]'):
            return self.get_shelf_url(
                self._shelf_url,
                int(response.xpath('//input[@id="pagenum"]/@value').extract_first()) + 1
            )

    def parse_shelf_page_total_matches(self, response):
        return

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
