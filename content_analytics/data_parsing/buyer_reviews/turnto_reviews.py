from content_analytics.items import BuyerReviews


class TurntoReviews(object):
    REVIEWS_URL = 'https://static.www.turnto.com/sitedata/{site_id}/v4_3/{sku}/d/en_US/catitemreviewshtml'

    @staticmethod
    def compile_url(site_id, sku):
        return TurntoReviews.REVIEWS_URL.format(
            site_id=site_id,
            sku=sku
        )

    @staticmethod
    def parse_reviews(response):
        review_count = response.xpath('//div[@class="TTreviewCount"]/text()').re_first(r'[\d,]+')
        if review_count:
            review_count = int(review_count.replace(',', ''))
        average = response.xpath('//span[@id="TTreviewSummaryAverageRating"]').re_first(r'\d\.\d')

        if review_count and average:
            return BuyerReviews(
                stars={
                    index: int(response.xpath(
                        '//div[@id="TTreviewSummaryBreakdown-{}"]/text()'.format(index)
                    ).extract_first()) for index in range(1, 6)
                },
                count=review_count,
                average=float(average)
            )
