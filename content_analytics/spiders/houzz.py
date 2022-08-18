import re
import json
import urlparse
from datetime import datetime

from content_analytics.spiders import BaseProductsSpider
from content_analytics.utils import catch_dictionary_exception, catch_json_exceptions
from content_analytics.items import BuyerReviews


class HouzzProductsSpider(BaseProductsSpider):
    name = "houzz_products"
    allowed_domains = ['houzz.com']

    # Urls
    SEARCH_URL = 'https://www.houzz.com/photos/products/query/{search_term}/nqrwns'

    IMAGE_URL = 'https://st.hzcdn.com/simgs/{image_id}_9-{ts}.jpg'

    RESULTS_PER_PAGE = 36

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/65.0.3325.181 Safari/537.36'
    }

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        # Embedded json
        data = self._parse_main_inline_json(response)
        media_data = self._parse_media_inline_json(response)
        image_data = self._parse_images_inline_json(response)
        variants_data = self._parse_variants_inline_json(response)

        product_id = self._parse_product_id(response)

        product.update(
            {
                # General fields
                'title': self._parse_title(response, data),
                'product_id': product_id,
                'short_description': self._parse_description(response),
                'buyer_reviews': self._parse_buyer_reviews(response),
                'image_urls': self._parse_image_urls(response, media_data, image_data, product_id),
                'video_urls': [],
                'variants': self._parse_variants(variants_data, product_id),

                # CH fields
                'features': self._parse_features(response),
                'specs': self._parse_specs(response),

                # Price
                'price_amount': self._parse_price_amount(response, data),
                'price_currency': 'USD',

                # Categorization
                'brand': self._parse_brand(response),
                'departments': self._parse_departments(response),

                # Identification
                'sku': product_id,
                'reseller_id': product_id,

                # Availability
                'in_stores': True,
                'site_online': True,
                'is_out_of_stock': self._parse_out_of_stock(data)
            }
        )

        yield product

    # Parsing inline product json blocs
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        inline_data = response.xpath('//script[@type="application/ld+json"]/text()').extract_first()
        return json.loads(inline_data)

    @catch_json_exceptions
    def _parse_media_inline_json(self, response):
        media_data = re.search(r'HZ.data.Spaces.addAll\((.*?)\);', response.body)
        if media_data:
            return json.loads(media_data.group(1))

        media_data = re.search(r'"imageIds":\[.*?\]', response.body)
        if media_data:
            media_data = json.loads('{%s}' % (media_data.group()))
            return media_data if media_data else None

    @catch_json_exceptions
    def _parse_images_inline_json(self, response):
        image_data = re.search(r'HZ.data.Images.addAll\((.*?)\);', response.body)
        if image_data:
            return json.loads(image_data.group(1))

        image_data = re.search(r'"images":(\{.*?\}),"users"', response.body)
        if image_data:
            image_data = json.loads(image_data.group(1))
            return image_data if image_data else None

    @catch_json_exceptions
    def _parse_variants_inline_json(self, response):
        variants_data = response.xpath('//script[@id="hz-ctx"]/text()').extract()
        if variants_data:
            return json.loads(variants_data[0])

    @staticmethod
    def _parse_title(response, data):
        if isinstance(data, dict):
            return data.get('name')

        return response.xpath(
            '//h1[@itemprop="name" and @class="header-1"]/text()'
        ).extract_first()

    @staticmethod
    def _parse_brand(data):
        if isinstance(data, dict):
            return data.get('brand')

    @staticmethod
    def _parse_departments(response):
        departments = response.xpath(
            '//ul[contains(@class, "breadcrumb")]//li//a//span/text()'
        ).extract()

        if not departments:
            departments = response.xpath(
                '//ol[contains(@class, "hz-breadcrumb__list")]//li//a//span/text()'
            ).extract()

        return departments[1:] if departments else None

    @staticmethod
    def _parse_description(response):
        return response.xpath('//div[@itemprop="description"]/text()').extract_first()

    @staticmethod
    def _parse_features(response):
        features = response.xpath(
            '//p[@class="description-header" and contains(text(), "Features:")]'
            '/following-sibling::ul[contains(@class, "description-item-list")][1]//li/text()'
        ).extract()

        return features

    @staticmethod
    def _parse_specs(response):
        specs = response.xpath(
            '//h2[*[contains(text(), "Specifications")]]/following-sibling::dl[1]')

        if not specs:
            specs = response.xpath('//div[@id="productSpecification"]//dl[1]')

        if specs:
            spec_terms = specs.xpath('.//dt/text()').extract()
            spec_defs = specs.xpath('.//dd/descendant::text()').extract()

            if spec_terms and spec_defs and len(spec_terms) == len(spec_defs):
                return dict(zip(spec_terms, spec_defs))

    @catch_dictionary_exception
    def _parse_image_urls(self, response, data, image_data, sku):
        image_urls = []
        thumb_images = response.xpath(
            '//div[@class="hzui-carousel__inner"]'
            '//div[contains(@class, "alt-images__thumb")]//img/@src').extract()

        for thumb_image in thumb_images:
            thumb_image = re.sub(r'w(\d+)', 'w640', thumb_image)
            thumb_image = re.sub(r'h(\d+)', 'h640', thumb_image)
            image_urls.append(thumb_image)

        if image_urls:
            return image_urls

        if image_data:
            image_ids = data.get(sku, {}).get('iids', [])
            if not image_ids:
                image_ids = data.get('imageIds', [])

            for image_id in image_ids:
                ts = image_data.get(image_id, {}).get('ts')
                if ts:
                    image_urls.append(self.IMAGE_URL.format(image_id=image_id, ts=ts))
        else:
            image_urls = response.xpath('//div[contains(@class, "view-product-image")]'
                                        '//img[contains(@class, "view-product-image-print")]/@src').extract()

        return image_urls if image_urls else None

    @staticmethod
    def _parse_out_of_stock(data):
        if isinstance(data, dict):
            return 'InStock' not in data.get('offers', {}).get('availability', '')

    @staticmethod
    def _parse_product_id(response):
        product_id = response.xpath('//span[@class="feed-question-text"]').re_first(r'\d+')
        if product_id:
            return product_id

        product_id = re.search(r'product/(\d+)\-', response.url)
        if product_id:
            return product_id.group(1)

    # Price parsing
    @catch_dictionary_exception
    def _parse_price_amount(self, response, data):
        if isinstance(data, dict):
            price = data.get('offers', {}).get('price')
            if price:
                return float(price)

        price = response.xpath(
            '//span[@itemprop="price"]/@content'
        ).re(r'\d*\.\d+|\d+')
        if price:
            return float(price)

    @staticmethod
    def _parse_buyer_reviews(response):
        average_review = response.xpath(
            '//span[contains(@class, "review-avg") or '
            'contains(@class, "product-reviews__rating")]/text()').extract_first()

        review_count = response.xpath(
            '//span[@class="reviews-count"]//span[@itemprop="ratingCount"]/text()').extract_first()

        if not review_count:
            review_count = response.xpath(
                '//span[@itemprop="reviewCount"]/text()').extract_first()

        ratings = []
        articles = response.xpath('//div[@class="reviews"]//article[contains(@class, "review")]')
        if not articles:
            articles = response.xpath('//div[@class="product-reviews__list"]//div[@class="product-reviews__review"]')

        for article in articles:
            review = len(
                article.xpath(
                    './/span[contains(@class, "hzi-Star") and not(contains(@class, "fill"))]'
                )
            )

            if review != 0:
                ratings.append(review)

        stars = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for rating in ratings:
            stars[rating] += 1

        last_review_date = response.xpath(
            '//div[@class="reviews"]//article//span[@class="js-publish-date"]'
            '//meta[@itemprop="datePublished"]/@content').extract_first()

        if not last_review_date:
            last_review_date = response.xpath(
                '//div[@class="product-reviews__list"]//div[@class="product-reviews__review"]'
                '//meta[@itemprop="datePublished"]/@content').extract_first()

        last_review_date = datetime.strptime(last_review_date, '%Y-%m-%d').date() if last_review_date else None

        return BuyerReviews(
            stars=stars,
            count=int(review_count.replace(',', '')) if review_count else 0,
            average=float(average_review) if average_review else 0,
            last_review_date=last_review_date
        )

    @catch_dictionary_exception
    def _parse_variants(self, variants_data, product_id):
        variants = []
        variants_info = variants_data['data']['stores']['data']['ProductVariationsStore']['data'][product_id]
        variants_map = variants_info['variationsMap']
        variants_items = variants_info['variationProducts']

        if isinstance(variants_map, dict) and isinstance(variants_items, dict):
            for prop, property_variantion in variants_map.iteritems():
                if prop == 'c':
                    property_name = 'color'
                elif prop == 's':
                    property_name = 'size'
                else:
                    property_name = prop

                if isinstance(property_variantion, dict):
                    for property_value, property_info in property_variantion.iteritems():
                        sku = property_info.get('spaceId', '')
                        variant_item = variants_items[str(sku)]
                        if variant_item:
                            variant_item['properties'] = {}
                            variant_item['properties'][property_name] = property_value

            for sku, variant_info in variants_items.iteritems():
                variant = {
                    'sku': sku,
                    'in_stock': variant_info.get('isAvailable'),
                    'properties': variant_info.get('properties'),
                    'url': variant_info.get('url'),
                }

                price = variant_info.get('price', {}).get('amount')
                if price:
                    variant['price'] = price

                variants.append(variant)

        return variants if variants else None

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        items = response.xpath(
            '//div[contains(@class, "hz-product-card__meta")]'
            '//a[contains(@class, "hz-product-card__product-title")]/@href'
        ).extract()

        for item in items:
            yield item

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//div[contains(@class, "hz-pagination-bottom")]'
            '//a[contains(@class, "hz-pagination-link--next")]/@href'
        ).extract_first()

        if next_page:
            return urlparse.urljoin(response.url, next_page)

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//h1[@class="header-1"]/text()').extract_first()
        if not total_matches:
            pagination_text = response.xpath('//span[@class="hz-top-pagination__text"]').extract_first() or ''
            total_matches = re.search(r'of(.*?)products', pagination_text)
            total_matches = total_matches.group(1) if total_matches else None

        if total_matches:
            total_matches = re.search(r'\d+', total_matches.replace(',', ''))
            total_matches = int(total_matches.group()) if total_matches else None

        return total_matches

    def parse_search_term_results_per_page(self, response):
        return self.RESULTS_PER_PAGE

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
