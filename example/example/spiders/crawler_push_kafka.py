import scrapy


class Crawler(scrapy.Spider):
    name = 'crawler_push_kafka'
    produce_item_topic = 'my_topic'
    start_urls = ['https://www.thebookofjoel.com/python-kafka-consumers']

    def parse(self, response, **kwargs):
        print('urls', response.url)
        return {'url': response.url}
