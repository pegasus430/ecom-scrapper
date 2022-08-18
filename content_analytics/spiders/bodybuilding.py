# -*- coding: utf-8 -*-
import re
import json
import traceback

from scrapy.item import Field

from content_analytics.utils import catch_json_exceptions
from content_analytics.items import SiteProductItem
from content_analytics.spiders import BaseProductsSpider


class BodyBuildingProductItem(SiteProductItem):
    buy_getfree=Field()


class BodyBuildingProductsSpider(BaseProductsSpider):
    name = 'bodybuilding_products'
    allowed_domains = ['bodybuilding.com']

    SEARCH_URL = 'https://search.bodybuilding.com/slp/full?context=store&query={search_term}&type=json'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    def parse_product(self, response):
        product = response.meta.get('item')

        sku = self._parse_sku(response)
        current_price = self._parse_price_amount(response)

        price_per_volume, volume_measure = self._parse_volume_measure(response)

        product.update(
            {
                # Main data
                'title': self._parse_title(response),
                'brand': self._parse_brand(response),
                'image_urls': self._parse_image_urls(response),
                'variants': self._parse_variants(response),
                'short_description': self._parse_short_desc(response),
                'long_description': self._parse_long_desc(response),
                'warnings': self._parse_warnings(response),
                'nutrition_fact_count': self._parse_nutrition_fact_count(response),
                'directions': self._parse_directions(response),
                'ingredients': self._parse_ingredients(response),
                # Codes
                'sku': sku,
                'site_product_id': sku,

                # price_per_volume:
                'price_per_volume': price_per_volume,
                'volume_measure': volume_measure,

                'buy_getfree': self._parse_buy_getfree(response),

                'is_out_of_stock': False,
                'in_stores': False
            }
        )

        if current_price:
            product.update({
                'price_amount': current_price,
                'now_price': str(current_price),
                'was_price': self._parse_was_price(response),
                'price_currency': 'USD',
            })

        return product

    @staticmethod
    def _parse_title(response):
        return response.xpath('//*[@class="Product__name"]//span[@itemprop="name"]/text()').extract_first()

    @staticmethod
    def _parse_brand(response):
        return response.xpath('//a[@class="Product__brand"]/text()').extract_first()

    @staticmethod
    def _parse_short_desc(response):
        return response.xpath('//div[@class="Product__desc-short"]/text()').extract_first()

    @staticmethod
    def _parse_long_desc(response):
        return response.xpath('//div[@class="Product__desc-long"]/text()').extract_first()

    @staticmethod
    def _parse_warnings(response):
        warnings = [
            i
            for i in response.xpath(
                '//p[@class="extraText" and strong[contains(text(), "Warnings")]]/text()[normalize-space()]'
            ).extract()
        ]
        return warnings[0] if warnings else None

    @staticmethod
    def _parse_nutrition_fact_count(response):
        count = response.xpath('//table[@id="facts_table"]//tr[@class="facts_label"]').extract()
        return len(count)

    @staticmethod
    def _parse_directions(response):
        directions = response.xpath(
            '//p[@class="extraText" and contains(strong/text(), "Directions")]/text()'
        ).extract()
        return ''.join(directions) if directions else None

    @staticmethod
    def _parse_ingredients(response):
        ingredients = response.xpath(
            '//table[@id="facts_table"]'
            '//tr[@class="other_label" and contains(td/span/text(), "Ingredients")]'
            '/following-sibling::tr'
            '//span[contains(@class, "label_ing_2")]/text()[normalize-space(.)]'
        ).extract()

        return ''.join(ingredients).split(',') if ingredients else None

    @staticmethod
    def _parse_save_percent(response):
        return response.xpath('//div[@class="Promotions"]//div[@class="Promo__title"]').re_first(r'%([\d+\.]+)')

    @staticmethod
    def _parse_image_urls(response):
        image_urls = response.xpath('//img[@class="Product__img"]/@src').extract()
        return image_urls if image_urls else None

    def _parse_variants(self, response):
        data = re.search(r'skuGroups: (\[.*?\]) },', response.body, re.DOTALL)
        if not data:
            return
        try:
            data = json.loads(data.group(1))
        except:
            self.logger.warning('Error Parsing Variant Json:{}'.format(traceback.format_exc()))
        else:
            variant_list = []
            for group in data:
                price = group.get('salePrice', '')
                for sku in group.get('skus', []):
                    variant = {
                        'price': price,
                        'properties': {
                        },
                        'sku_id': sku.get('skuId'),
                        'selected': sku.get('selected'),
                        'image_url': sku.get('largeImageURL'),
                        'is_available': sku.get('inventory', {}).get('labelDisplay', '') != 'Currently Unavailable'
                    }
                    if sku.get('type') and sku.get('name'):
                        variant['properties'][sku.get('type').lower()] = str(sku.get('name'))
                        variant_list.append(variant)
            return variant_list if variant_list else None

    # Codes parsing
    @staticmethod
    def _parse_sku(response):
        return response.xpath('//div[@id="label_preview"]/@data-ingredient-sku').extract_first()

    # Price parsing
    @staticmethod
    def _parse_price_amount(response):
        price = response.xpath('//div[contains(@class, "SkuGroup") and position()=1]'
                               '//div[@class="SkuGroup__sale-price"]/text()').re_first(r'\d{1,3}[,\d{3}]*\.?\d*')
        return float(price.replace(',', '')) if price else None

    @staticmethod
    def _parse_was_price(response):
        return response.xpath(
            '//div[contains(@class, "SkuGroup") and position()=1]//span[@class="strike-price__price"]/text()'
        ).re_first(r'\d{1,3}[,\d{3}]*\.?\d*')

    @staticmethod
    def _parse_volume_measure(response):
        volume_measure = response.xpath(
            '//div[@class="SkuGroup" and position()=1]//div[@class="SkuGroup__servings"]/text()'
        ).re_first(r'\$\d{1,3}[,\d{3}]*\.?\d* Per .+')
        if volume_measure:
            volume_measure = volume_measure.lower().split(' per ')
            return volume_measure[0].replace('$', '').strip(), volume_measure[1].strip()
        return None, None

    @staticmethod
    def _parse_buy_getfree(response):
        return response.xpath('//div[@class="Promo__title"]/text()').re_first(r'Buy (\d+) Get \d+ Free')

    @staticmethod
    @catch_json_exceptions
    def _parse_review_json(response):
        data = json.loads(response.body)
        return data

    def get_default_item(self, *args, **kwargs):
        return BodyBuildingProductItem()

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(BodyBuildingProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        yield request.replace(
            headers={
                'Accept': 'application/json, text/plain, */*'
            }
        )

    def parse_search_term_items(self, response):
        try:
            data = json.loads(response.body)
            if 'results' in data:
                data = data.get('results', {})
            items = data.get('storeResults', {}).get('items', [])
        except:
            self.logger.warning('Error Parsing the Json from {0}: {1}'.format(response.url, traceback.format_exc()))
        else:
            for product in items:
                link = product.get('productSeoUrl')
                if not link:
                    continue
                yield link

    def get_search_term_next_page(self, response):
        try:
            data = json.loads(response.body)
            if 'results' in data:
                data = data.get('results', {})
        except:
            self.logger.warning('Error Parsing the Json from {0}: {1}'.format(response.url, traceback.format_exc()))
        else:
            return data.get('storeResults', {}).get('nextPageLink')

    def parse_search_term_total_matches(self, response):
        try:
            data = json.loads(response.body)
            if 'results' in data:
                data = data.get('results', {})
            total_matches = data.get('storeResults', {}).get('resultLabel', '')
            total_matches = re.search(r'([\d,]+)', total_matches).group(1)
            total_matches = int(total_matches)
        except:
            self.logger.warning('Error Parsing the Json from {0}: {1}'.format(response.url, traceback.format_exc()))
        else:
            return total_matches

    def parse_search_term_results_per_page(self, response):
        pass

    # Shelf component
    def parse_shelf_page_items(self, response):
        items = response.xpath('//a[@class="product__name"]/@href').extract()
        for link in items:
            yield link

    def get_shelf_page_next_page(self, response):
        return response.xpath('//a[@next-link]/@href').extract_first()

    def parse_shelf_page_total_matches(self, response):
        total_matches = response.xpath('//div[@class="results"]/text()').re_first(r'[\d,]+')
        try:
            return int(total_matches.replace(',', ''))
        except:
            self.log('Error Parsing Total matches: {}'.format(traceback.format_exc()))

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
