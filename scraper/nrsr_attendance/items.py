import scrapy


class RawRecord(scrapy.Item):
    kind = scrapy.Field()
    source_url = scrapy.Field()
    fetched_at_utc = scrapy.Field()
    http_status = scrapy.Field()
    payload = scrapy.Field()
