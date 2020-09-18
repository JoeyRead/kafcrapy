from kafcrapy.spider import KafkaFeedSpider
import scrapy


class KafkaSpider(KafkaFeedSpider):
    name = "demo_spider"
    topic = "my_topic"
    # start_urls = ["https://www.thebookofjoel.com/python-kafka-consumers"]

    @classmethod
    def process_message(cls, message):
        print("message", message)
        if not message:
            return None
        return message.get('url')

    def parse(self, response,  **kwargs):
        print(response.url)
