from dotenv import load_dotenv
from scrapy.crawler import CrawlerProcess
from spider import ExampleSpider

def main():
    load_dotenv()
    process = CrawlerProcess()
    process.crawl(ExampleSpider)
    process.start()

if __name__ == "__main__":
    main()
