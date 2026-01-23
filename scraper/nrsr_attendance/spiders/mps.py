import scrapy


class MpsSpider(scrapy.Spider):
    name = "mps"
    start_urls: list[str] = []

    def parse(self, response: scrapy.http.Response):
        raise NotImplementedError("MVP: implement MP roster scrape")
