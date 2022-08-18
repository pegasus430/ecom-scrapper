import re
import json

from content_analytics.spiders import BaseProductsSpider
from content_analytics.items import BuyerReviews
from content_analytics.middlewares.proxy import ProxyContext
from content_analytics.utils import catch_json_exceptions, cond_set_value, replace_http_with_https


class WayfaircaProductsSpider(BaseProductsSpider):
    name = 'wayfair_ca_products'
    allowed_domains = ['www.wayfair.ca']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/66.0.3359.139 Safari/537.36'
    }

    SEARCH_URL = "https://www.wayfair.ca/keyword.php?keyword={search_term}"

    # ############################
    # Requests generator methods #
    # ############################
    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(WayfaircaProductsSpider, self).make_single_product_requests(url, *args, **kwargs))
        request = request.replace(url=replace_http_with_https(request.url))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(WayfaircaProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    def make_shelf_page_requests(self, url, *args, **kwargs):
        request = next(super(WayfaircaProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        with ProxyContext(request) as proxy_request:
            yield proxy_request

    ################################
    # Search term abstract methods #
    ################################
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(
            search_term=search_term
        )

    def get_search_term_next_page(self, response):
        next_page = response.xpath('//a[contains(@class, "Pagination-link")]/@href').extract()
        if next_page:
            return next_page[0]

    def parse_search_term_items(self, response):
        links = response.xpath('//div[@id="sbprodgrid"]'
                               '//a[contains(@class, "ProductCard")]/@href').extract()
        return [response.urljoin(link) for link in links]

    def parse_search_term_total_matches(self, response):
        total_matches = re.findall(r'"product_count":(\d+)', response.body)
        return int(total_matches[0]) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        return self.parse_search_term_total_matches(response)

    ################################
    # Shelf pages abstract methods #
    ################################
    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    ###################
    # Parsing methods #
    ###################
    @staticmethod
    @catch_json_exceptions
    def _get_product_json(response):
        """ Get product json
        :param response (HtmlResponse): general url response
        :return (dict) product json data
        """
        product_json = None
        raw_data = response.xpath('//script[@type="application/ld+json"]/text()').extract_first()
        if raw_data:
            product_json = json.loads(raw_data)
        return product_json

    @staticmethod
    @catch_json_exceptions
    def _get_json_from_response(response):
        """ Get json data from response
        :param response (HtmlResponse): general url response
        :return (dict) json data
        """
        data = json.loads(response.body_as_unicode())
        return data

    def parse_product(self, response):
        product = response.meta.get('item')

        product_json = self._get_product_json(response)
        if not product_json:
            return

        # brand
        cond_set_value(product, 'brand', self._parse_brand(product_json))

        # title
        cond_set_value(product, 'title', self._parse_title(product_json))

        # product short description
        short_description = self._parse_short_description(product_json)
        cond_set_value(product, 'short_description', short_description)

        # site_product_id
        site_product_id = self._parse_product_id(product_json)
        cond_set_value(product, 'site_product_id', site_product_id)

        # reseller_id
        reseller_id = self._parse_reseller_id(product_json)
        cond_set_value(product, 'reseller_id', reseller_id)

        # image_url, image_urls, image_alts
        image_urls, image_alts = self._parse_images(response)
        if image_urls:
            cond_set_value(product, 'image_urls', image_urls)
            cond_set_value(product, 'image_url', image_urls[0])
        cond_set_value(product, 'image_alts', image_alts)

        # department, departments
        departments = self._parse_departments(response)
        if departments:
            cond_set_value(product, 'departments', departments)
            cond_set_value(product, 'department', departments[-1])

        # price_amount
        price_amount = self._parse_price_amount(product_json)
        cond_set_value(product, 'price_amount', price_amount)

        # price_currency
        price_currency = self._parse_price_currency(product_json)
        cond_set_value(product, 'price_currency', price_currency)

        # features
        features = self._parse_features(response)
        cond_set_value(product, 'features', features)

        # in_stores
        cond_set_value(product, 'in_stores', False)

        # site_online
        cond_set_value(product, 'site_online', True)

        # site_online_out_of_stock
        site_online_out_of_stock = self._parse_site_online_out_of_stock(response)
        cond_set_value(product, 'site_online_out_of_stock', site_online_out_of_stock)

        # variants
        variants = self._parse_variants(response)
        cond_set_value(product, 'variants', variants)

        # buyer_reviews
        num_of_reviews = product_json.get('aggregateRating', {}).get('reviewCount')
        if num_of_reviews:
            buyer_reviews = self._parse_buyer_reviews(response)
            cond_set_value(product, 'buyer_reviews', buyer_reviews)

        yield product

    @staticmethod
    def _parse_short_description(product_json):
        """ Get short_description
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `short_description`
        """
        return product_json.get('description')

    @staticmethod
    def _parse_product_id(product_json):
        """ Get product_id
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `product_id`
        """
        return product_json.get('sku')

    @staticmethod
    def _parse_reseller_id(product_json):
        """ Get reseller_id
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `reseller_id`
        """
        return product_json.get('sku')

    @staticmethod
    def _parse_departments(response):
        """ Get departments
        :param response (HtmlResponse): general url response
        :return (list) product `departments`
        """
        departments = response.xpath(
            '//*[@class="Breadcrumbs-listItem"]//a/text()'
        ).extract()
        return departments if departments else None

    @staticmethod
    def _parse_title(product_json):
        return product_json.get('name')

    @staticmethod
    def _parse_brand(product_json):
        return product_json.get('brand')

    @staticmethod
    def _parse_price_amount(product_json):
        """ Get price amount
        :param price (str): price from method `_parse_price`
        :return (float) product `price amount`
        """
        price_amount = product_json.get('offers', {}).get('price')
        return float(price_amount) if price_amount else None

    @staticmethod
    def _parse_price_currency(product_json):
        """ Get price currency
        :param product_json (dict): product json from method `_get_product_json`
        :return (str) product `price currency`
        """
        return product_json.get('offers', {}).get('priceCurrency')

    @staticmethod
    def _parse_images(response):
        """ Get image urls and alts
        :param response (HtmlResponse): general url response
        :return (tuple) product `image_urls` and `image_alts`
        """
        urls = []
        alts = []
        images = response.xpath(
            '//ul[contains(@class, "InertiaCarouselComponent")]'
            '/li//div[@class="ImageComponent"]/img[@class="ImageComponent-image"]'
        )
        for image in images:
            url = image.xpath('./@src').extract_first()
            urls.append(url)
            alts.append(image.xpath('./@alt').extract_first())
        return urls, alts

    @staticmethod
    def _parse_features(response):
        """ Get features
        :param response (HtmlResponse): general url response
        :return (list) product `features`
        """
        features = []
        feature_info = response.xpath(
            "//div[@class='Specifications']"
            "//table[@class='Specifications-table']"
            "//tr")
        for feature in feature_info:
            title = feature.xpath('./td[1]/text()').extract_first()
            value = feature.xpath('./td[2]/text()').extract_first()
            if title and value:
                features.append(title + ': ' + value)

        return features if features else None

    @staticmethod
    def _parse_site_online_out_of_stock(response):
        """ Get site online outt of stock
        :param response (HtmlResponse): general url response
        :return (bool) product `site_online_out_of_stock`
        """
        return bool(response.xpath(
            '//button[contains(text(), "Out")]'
        ))

    @staticmethod
    def _parse_variants(response):
        """Get product variants (full data)
        :param response (HtmlResponse): general url response
        :return (product): product `variants`
        """
        try:
            json_data = re.search(r'{"isReduxPDP":(.*?),"finalProps"', response.body_as_unicode()).group(1)
            variants_json = json.loads('{"isReduxPDP":' + json_data)
        except:
            variants_json = {}

        variants = []
        base_price = variants_json.get('price', {}).get('salePrice')
        product_variants = variants_json.get('options', {}).get('standardOptions')
        if product_variants:
            product_variants = product_variants[0]
            for product_variant in product_variants.get('options', {}):
                if not product_variant:
                    continue
                variant = {}
                properties = {}

                key = product_variant.get("category", '').lower()
                if key:
                    properties[key] = product_variant.get("name")

                variant["properties"] = properties
                variant["in_stock"] = product_variant.get("is_active")
                variant["image_url"] = product_variant.get("thumbnail")

                delta_price = float(product_variant.get("cost", 0))
                if base_price:
                    variant["price"] = round(base_price + delta_price, 2)

                variants.append(variant)

        return variants if variants else None

    @staticmethod
    def _parse_buyer_reviews(response):
        """Get product variants (full data)
        :param response (HtmlResponse): general url response
        :return (BuyerReviews): product `buyer_reviews`
        """
        average = response.xpath('//div[@class="ProductDetailReviews-header"]'
                                 '//p[@class="ReviewStars-reviews"]/text()').re(r'\d\.?\d*')
        if average:
            average = float(average[0])
        reviews = [int(i) for i in response.xpath('//div[@class="ProductReviewsHistogram-count"]/text()').re(r'\d+')]
        if reviews:
            reviews = reviews[:5]
            rating_by_star = {5-i: review for (i, review) in enumerate(reviews)}
            review_count = sum(reviews)

            buyer_reviews = BuyerReviews(
                stars=rating_by_star,
                count=review_count,
                average=average if average else 0,
            )

            return buyer_reviews
