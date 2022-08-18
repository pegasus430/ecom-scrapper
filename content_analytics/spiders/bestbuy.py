import re
import json
import urlparse
from collections import defaultdict
from lxml import etree

from . import BaseProductsSpider
from ..items import BuyerReviews
from ..utils import catch_dictionary_exception, catch_json_exceptions


class BestBuyProductsSpider(BaseProductsSpider):
    name = "bestbuy_products"
    allowed_domains = ['bestbuy.com']

    custom_settings = {
        'USER_AGENT': 'Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)'
    }

    # Urls
    SEARCH_URL = 'https://www.bestbuy.com/site/searchpage.jsp?st={search_term}&_dyncharset=UTF-8' \
                 '&id=pcat17071&type=page&sc=Global&cp=1&list=n&af=true&iht=y' \
                 '&usc=All+Categories&ks=960&keys=keys'

    FULFILLMENT_URL = 'https://www.bestbuy.com/fulfillment/shipping/api/v1/fulfillment/sku;' \
            'skuId={};postalCode={};deliveryDateOption=EARLIEST_AVAILABLE_DATE'

    PRICE_URL = 'https://www.bestbuy.com/api/1.0/carousel/prices?skus={skus}'

    # Constants
    PRICE_CURRENCY = 'USD'
    ZIP_CODE = '94012'

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        # Embedded json
        data = self._parse_main_inline_json(response)
        specs_data = self._parse_specs_json(response)
        price_data = self._parse_price_inline_json(response)
        image_data = self._parse_image_inline_json(response)
        variants_data = self._parse_variants_inline_json(response)

        product_data = data.get('productDetails') or {}

        sku = self._parse_sku(product_data, response)

        product.update(
            {
                # General fields
                'title': self._parse_title(product_data, response),
                'short_description': self._parse_description(product_data),
                'buyer_reviews': self._parse_buyer_reviews(data),
                'image_urls': self._parse_image_urls(image_data),
                'video_urls': [],
                'variants': self._parse_variants(variants_data, sku),

                # CH fields
                'features': self._parse_features(response),
                'specs': {},

                # Price
                'price_amount': self._parse_price_amount(price_data),
                'price_currency': self._parse_price_currency(price_data),

                # Categorization
                'brand': self._parse_brand(product_data),
                'departments': self._parse_departments(data),

                # Identification
                'model': self._parse_model(product_data),
                'gtin': self._parse_gtin(price_data),
                'sku': sku,
                'reseller_id': sku,

                # Availability
                'in_stores': True,
                'site_online': True,
                'no_longer_available': self._parse_no_longer_available(response)
            }
        )

        # Specs
        for tab in specs_data or []:
            if tab['id'] == 'specifications' or 'Details' in tab['id']:
                yield response.request.replace(
                    url=urlparse.urljoin(response.url, tab['fragmentUrl']),
                    callback=self.parse_specs
                )

        # Stock status
        yield response.request.replace(
            url=self.FULFILLMENT_URL.format(sku, self.ZIP_CODE),
            headers={'x-client-id': 'BROWSE'},
            callback=self.parse_site_online_out_of_stock
        )

        # Variants
        if product['variants']:
            skus = [variant['sku'] for variant in product['variants']]
            for index in range(0, len(skus), 25):
                _skus = ",".join(skus[index:index + 25])
                if _skus:
                    yield response.request.replace(
                        url=self.PRICE_URL.format(skus=_skus),
                        callback=self.parse_prices
                    )

        # Videos
        video_ids = re.findall('"liveClickerId":"([^"]+?)"', response.body)

        for video_id in video_ids:
            yield response.request.replace(
                url='https://sc.liveclicker.net/service/getXML?widget_id={}'.format(video_id),
                callback=self.parse_videos,
                dont_filter=True
            )

    # Parse prices from a separate API request
    @catch_json_exceptions
    def parse_prices(self, response):
        product = response.meta.get('item')
        data = json.loads(response.body_as_unicode())
        prices = {variant['skuId']: variant['priceDomain']['currentPrice'] for variant in data}
        for variant in product['variants']:
            if variant['sku'] in prices:
                price = prices[variant['sku']]
                variant['price'] = price

                if variant['sku'] == product['sku']:
                    product['price_amount'] = price

    def parse_specs(self, response):
        product = response.meta.get('item')
        specs_html = response.xpath('//div[contains(@class, "key-specs")]')

        if specs_html:
            specs_html = specs_html[0]

            names = specs_html.xpath(".//div[@class='specification-name']//text()")
            values = specs_html.xpath(".//div[@class='specification-value']//text()")

            for name, value in zip(names, values):
                product['specs'][name.extract().strip()] = value.extract().strip()

    @catch_json_exceptions
    def parse_site_online_out_of_stock(self, response):
        product = response.meta.get('item')
        data = json.loads(response.body_as_unicode())
        stock_data = data['responseInfos'][0]
        product['site_online_out_of_stock'] = not(stock_data['shippingEligible'])

    def parse_videos(self, response):
        product = response.meta.get('item')
        e = etree.fromstring(response.body)
        product['video_urls'].extend(e.xpath('//location/text()'))

    # Parsing inline product json blocs
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        text = response.xpath(
            '//script[contains(., "window.__UGC_APP_INITIAL_STATE__")]//text()'
        ).extract_first()
        if text:
            return json.loads(re.sub(r'window\.__UGC_APP_INITIAL_STATE__\s*=\s*', '', text))
        return {}

    @catch_json_exceptions
    def _parse_specs_json(self, response):
        return json.loads(
            response.xpath("//div[@id='pdp-model-data']/@data-tabs").extract()[0]
        )

    @catch_json_exceptions
    def _parse_price_inline_json(self, response):
        return json.loads(
            response.xpath('//script[contains(., "priceCurrency")]//text()').extract()[0]
        )

    @catch_json_exceptions
    def _parse_image_inline_json(self, response):
        return json.loads(
            response.xpath(
                '//script[contains(., "thumbnailUrl") and @type="application/ld+json"]//text()'
            ).extract()[0]
        )

    @catch_json_exceptions
    def _parse_variants_inline_json(self, response):
        return json.loads(
            response.xpath(
                '//script[contains(., "product-variations")]//text()'
            ).re(r'(\{"app":.+?\}),\s')[0]
        )

    # Parsing json data
    @catch_dictionary_exception
    def _parse_title(self, data, response):
        def parse_title_from_html(response):
            return response.xpath(
                '//h1[@class="type-subhead-alt-regular"]/text()'
            ).extract_first()

        if data:
            return data['name']
        return parse_title_from_html(response)

    @catch_dictionary_exception
    def _parse_brand(self, data):
        return data['brandName']

    @catch_dictionary_exception
    def _parse_departments(self, data):
        return [breadcrumb['displayName'] for breadcrumb in data['breadcrumb']
                if breadcrumb.get('categoryId')][1:]

    @catch_dictionary_exception
    def _parse_buyer_reviews(self, data):
        statistics = data['stats']
        overall_statistics = statistics['overallStats']

        return BuyerReviews(
            stars={dist['value']: dist['count'] for dist in statistics['distribution']},
            average=overall_statistics['averageOverallRating'],
            count=overall_statistics['totalReviewCount']
        )

    @catch_dictionary_exception
    def _parse_description(self, data):
        return data['description'].strip()

    def _parse_features(self, response):
        features = []

        for f in response.xpath('//div[@class="feature"]'):
            title = f.xpath('./span/text()').extract()
            value = f.xpath('./p/text()').extract()
            if title and value:
                if title == 'Need more information?':
                    continue
                features.append(title[0] + ': ' + value[0])
            if not title and value:
                features.append(value[0])
            if title and not value:
                features.append(title[0])

        return features

    @catch_dictionary_exception
    def _parse_image_urls(self, data):
        if isinstance(data, list):
            return [image['thumbnailUrl'] for image in data]

    @catch_dictionary_exception
    def _parse_variants(self, data, _sku):
        variants_names = {category['id']: category['name'] for category in data['categories']}
        variants = defaultdict(lambda: {})
        for _variants in data['variationSkus']:
            category_id = _variants['categoryId']
            name = _variants['name']
            for sku in _variants['skus']:
                variants[sku][variants_names[category_id].lower()] = name

        return [
            {'sku': sku, 'properties': properties, 'selected': sku == _sku}
            for sku, properties in variants.items()
            ]

    # Parse html data
    def _parse_no_longer_available(self, response):
        return bool(
            response.xpath(
                '//div[@class="alert alert-warning"'
                ' and text() = "This item is no longer available."]'
            )
        )

    # Codes parsing
    @catch_dictionary_exception
    def _parse_sku(self, data, response):
        if data.get('sku'):
            return data['sku']

        return response.xpath("//span[@id='sku-value']/text()").extract_first()

    @catch_dictionary_exception
    def _parse_model(self, data):
        return data['model']

    @catch_dictionary_exception
    def _parse_gtin(self, data):
        return data['gtin13'].zfill(14)

    # Price parsing
    @catch_dictionary_exception
    def _parse_price_amount(self, data):
        return float(data['offers']['price'])

    @catch_dictionary_exception
    def _parse_price_currency(self, data):
        try:
            price_currency = data['offers']['priceCurrency']
        except (KeyError, TypeError):
            price_currency = self.PRICE_CURRENCY
        return price_currency

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        links = response.xpath('//div[@class="sku-title"]/h4/a/@href').extract()
        for link in links:
            yield urlparse.urljoin(response.url, link)

    def get_search_term_next_page(self, response):
        next_page = response.xpath('//a[@title="Next Page"]/@href').extract()
        if next_page:
            return next_page[0]

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//a[@data-type="All"]/@data-count').extract()
        if total_matches:
            return total_matches[0]

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
