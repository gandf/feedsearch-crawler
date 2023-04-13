import asyncio
import json
# import time
from feedsearch_crawler import FeedsearchSpider, sort_urls
from feedsearch_crawler.crawler import coerce_url
import sys

urls = [
    # "arstechnica.com",
    # "https://davidbeath.com",
    # "http://xkcd.com",
    # "http://jsonfeed.org",
    # "en.wikipedia.com",
    # "scientificamerican.com",
    # "newyorktimes.com",
    # "https://www.dancarlin.com",
    # "https://www.hanselminutes.com/",
    # "nytimes.com",
    # "https://www.jeremydaly.com/serverless-microservice-patterns-for-aws/",
    # "feedhandbook.com",
    # "https://americanaffairsjournal.org/2019/05/ubers-path-of-destruction/",
    # "localhost:8080/test",
    # "theatlantic.com",
    # "nypost.com",
    # "https://www.washingtonpost.com",
    # "localhost:5000",
    # "latimes.com",
    # "http://feeds.washingtonpost.com/rss/rss_fact-checker?noredirect=on",
    # "http://tabletopwhale.com/index.html"
    # "www.vanityfair.com",
    # "bloomberg.com",
    # "http://www.bloomberg.com/politics/feeds/site.xml",
    # "propublica.org"
    # "npr.org",
    # "rifters.com",
    # "https://www.bbc.co.uk/podcasts"
    # "https://www.bbc.co.uk/programmes/p02nrsln/episodes/downloads",
    # "https://breebird33.tumblr.com/",
    # "https://neurocorp.tumblr.com/",
    # "https://breebird33.tumblr.com/rss"
    # "https://resel.fr/rss-news"
    # "https://muhammadraza.me"
    # "https://www.franceinter.fr/rss/a-la-une.xml",
    # "harpers.org",
    # "slashdot.com",
    # "https://bearblog.dev",
    # "aeon.co",
    # "https://davidgerard.co.uk/blockchain/"
    # "raymii.org/s/"
    # "stratechery.com",
    # "www.internet-law.de",
    # "https://medium.com/zendesk-engineering/the-joys-of-story-estimation-cda0cd807903",
    # "https://danwang.co/",
    # "http://matthewdickens.me/podcasts/TWIS-feed.xml"
]


def get_pretty_print(json_object: object):
    return json.dumps(json_object, sort_keys=True, indent=2, separators=(",", ": "))


# @profile()
def run_crawl(setup_type_p: int):
    # user_agent = "Mozilla/5.0 (Compatible; Bot)"
    user_agent = "Mozilla/5.0 (Compatible; Feedsearch Bot)"
    # user_agent = "curl/7.58.0"
    # user_agent = (
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0"
    # )
    # user_agent = (
    #     "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    # )

    # headers = {
    #     "User-Agent": user_agent,
    #     "DNT": "1",
    #     "Upgrade-Insecure-Requests": "1",
    #     "Accept-Language": "en-US,en;q=0.5",
    #     "Accept-Encoding": "gzip, deflate, br",
    #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    #     "Referrer": "https://www.google.com/",
    # }

    setup_concurrency = 15
    setup_total_timeout = 3
    setup_max_depth = 4
    full_crawl = False

    if setup_type_p == 1:
        setup_concurrency = 15
        setup_total_timeout = 8
        setup_max_depth = 10
        full_crawl = False

    if setup_type_p == 2:
        setup_concurrency = 20
        setup_total_timeout = 20
        setup_max_depth = 5
        full_crawl = True

    crawler = FeedsearchSpider(
        concurrency=setup_concurrency,
        total_timeout=setup_total_timeout,
        request_timeout=30,
        user_agent=user_agent,
        # headers=headers,
        favicon_data_uri=False,
        max_depth=setup_max_depth,
        max_retries=2,
        ssl=True,
        full_crawl=full_crawl,
        delay=0,
        try_urls=True,
    )
    crawler.start_urls = urls
    asyncio.run(crawler.crawl())

    items = sort_urls(list(crawler.items))

    serialized = [item.serialize() for item in items]

    results = get_pretty_print(serialized)
    print(results)


def create_allowed_domains(urls_p):
    domain_patterns = []
    for url in urls_p:
        url = coerce_url(url)
        host = url.host
        pattern = f"*.{host}"
        domain_patterns.append(host)
        domain_patterns.append(pattern)
    return domain_patterns


if __name__ == "__main__":
    # start = time.perf_counter()
    if len(sys.argv) > 2:
        setup_type = int(sys.argv[1])
        urls = sys.argv[2:]
        run_crawl(setup_type)

    # duration = int((time.perf_counter() - start) * 1000)
    # print(f"Entire process ran in {duration}ms")
