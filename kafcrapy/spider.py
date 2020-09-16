from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider


class KafkaSpiderMixin(object):

    def __init__(self):
        self.consumer = None

    @classmethod
    def process_message(cls, message):
        if not message:
            return None
        return message.message.value

    def setup_kafka_consumer(self, settings):
        pass

    def start_request(self):
        pass

    def next_request(self):
        pass

    def schedule_next_request(self):
        pass

    def spider_idle(self):
        pass

    def item_scrapped(self):
        pass

    def closed(self, reason):
        pass


class KafkaFeedSpider(KafkaSpiderMixin, Spider):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        pass
