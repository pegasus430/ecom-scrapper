import re
from six import string_types

from content_analytics.utils import cond_set_value, guess_brand
from content_analytics.items import HTags, Meta


class ContentMiddleware(object):

    def process_spider_input(self, response, spider):
        item = response.meta.get('item')
        if item is not None:
            # meta
            meta = self.parse_meta(response)
            item.update({'meta': meta})

            # htags
            htags = self.parse_htags(response)
            item.update({'htags': htags})

            # brand
            if item.get('title') and not item.get('brand'):
                item.update({'brand': guess_brand(item['title'])})

            # department
            if item.get('departments') and not item.get('department'):
                item.update({'department': item['departments'][-1]})

    @staticmethod
    def parse_meta(page_response):
        """ Get meta info from product page
        :param page_response: general url response
        :return (Meta): object represents meta tags from product page
        """

        def remove_extra_spaces(s):
            if isinstance(s, string_types):
                return re.sub(r'\s+', ' ', s)
            return s

        charset = page_response.xpath('//meta[@charset]/@charset').extract_first()
        canonical_url = page_response.xpath('//link[@rel="canonical"]/@href').extract_first()
        browser_title = page_response.xpath('//title/text()').extract_first()
        keywords = page_response.xpath('//meta[@name="keywords"]/@content').extract_first()
        description = page_response.xpath('//meta[@name="description"]/@content').extract_first()
        meta_tags = page_response.xpath('//meta').extract()

        return Meta(
            charset=charset,
            canonical_url=canonical_url,
            browser_title=browser_title,
            keywords=keywords,
            description=description,
            meta_tags=[remove_extra_spaces(m) for m in meta_tags]
        )

    @staticmethod
    def parse_htags(page_response):
        """ Get heading tags from product page (h1 ,h2)
        :param page_response: general url response
        :return (HTags): object represents headings from product page
        """
        h1 = page_response.xpath('//h1//text()').extract()
        h2 = page_response.xpath('//h2//text()').extract()
        return HTags(h1, h2)
