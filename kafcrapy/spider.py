from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider
from . import connection


class KafkaSpiderMixin(object):

    def __init__(self):
        self.consumer = None

    @classmethod
    def process_message(cls, message):
        if not message:
            return None
        return message.message.value

    def setup_kafka_consumer(self):
        self.logger.info('setting up with kafka consumer')

        if not self.crawler:
            raise ValueError("Crawler is required")

        if not hasattr(self, 'topic') or not self.topic:
            raise ValueError('kafka topic is required')

        if self.consumer:
            return

        settings = self.crawler.settings
        self.consumer = connection.consumer_from_settings(topic_name=self.topic, settings=settings)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

        # This will be called just after item has been scraped, reason is to call this not to stop crawler
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def start_request(self):
        return self.next_request()

    def next_request(self):
        print("starting next message_batch")
        message_batch = self.consumer.poll(timeout_ms=5000, max_records=5)
        for partition_batch in message_batch.values():
            for message in partition_batch:
                print("message", message.value)
                url = self.process_message(message.value)
                yield self.make_requests_from_url(url)

    def schedule_next_request(self):
        for req in self.next_request():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """
        Schedules a requests if available, otherwise waits, If there is no request available in the queue
        so it will not close the spider
        :return:
        """
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """
        After item has been scrapped, avoid waiting for scheduler to schedule next request
        :param args:
        :param kwargs:
        :return:
        """
        self.schedule_next_request()

    def closed(self, reason):
        if self.consumer:
            self.consumer.close()
        self.logger.info('Closing spider name: %s, reason: %s', getattr(self, 'name', None), reason)


class KafkaFeedSpider(KafkaSpiderMixin, Spider):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj: KafkaFeedSpider = super(KafkaFeedSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_kafka_consumer()
        return obj
