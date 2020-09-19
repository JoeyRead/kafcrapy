from . import connection


class KafkaProducerPipeline(object):
    """
    Publish serialize item to configured topic
    """

    def __init__(self, producer):
        self.producer = producer
        self.topic = None

    def open_spider(self, spider):
        if not hasattr(spider, 'produce_item_topic'):
            return ValueError('produce_item_topic name is not provided')
        self.topic = spider.produce_item_topic

    def process_item(self, item, spider):
        """
        This method has overridden for pipeline to process the item
        :param item:
        :param spider:
        :return:
        """
        """
        send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        """
        self.producer.send(topic=self.topic, value=item)

    @classmethod
    def from_settings(cls, settings):
        """
        This
        :param settings: the current scrapy spider settings
        :return: KafkaProducerPipeline instance
        """
        producer = connection.producer_from_settings({})
        return cls(producer)

    @classmethod
    def from_crawler(cls, crawler):
        cls.from_settings(crawler.settings)

    def close_spider(self, spider):
        if self.producer:
            self.producer.close()
