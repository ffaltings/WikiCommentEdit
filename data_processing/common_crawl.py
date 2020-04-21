import json, requests, gzip
import logging
from io import StringIO, BytesIO

def get_first_or_none(iter):
    return next(filter(None, iter), None)

class CommonCrawlS3():

    # hide logging of urllib3
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    DefaultIndices = [
        'CC-MAIN-2020-16-index',
        #'CC-MAIN-2020-10-index',
        #'CC-MAIN-2020-05-index',
        #'CC-MAIN-2019-51-index',
        #'CC-MAIN-2019-47-index',
        #'CC-MAIN-2019-43-index',
        #'CC-MAIN-2019-39-index',
        #'CC-MAIN-2019-35-index',
        #'CC-MAIN-2019-30-index',
        #'CC-MAIN-2019-26-index',
        #'CC-MAIN-2019-22-index',
        #'CC-MAIN-2019-18-index',
        #'CC-MAIN-2019-13-index',
        #'CC-MAIN-2019-09-index',
        'CC-MAIN-2019-04-index'
    ]

    def __init__(self, indices = DefaultIndices):
        self.indices = indices
        self.cache = {} # TODO: limit memory of this cache
        self.call_count = 0
        self.fail_count = 0

    @staticmethod
    def fetch_html_from_s3_file(meta):
        offset, length = int(meta['offset']), int(meta['length'])
        offset_end = offset + length - 1
        prefix = 'https://commoncrawl.s3.amazonaws.com/'
        resp = requests.get(prefix + meta['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        with BytesIO(resp.content) as raw_data:
            with gzip.GzipFile(fileobj = raw_data) as f:
                data = f.read().decode("utf8")
                sections = data.strip().split('\r\n\r\n', 2)
                if len(sections) != 3:
                    logging.error("Received a non-standard data blob from CommonCrawl for page {}".format(str(meta)))
                    return None
                warc, header, response = sections
                if not "200 OK" in header:
                    # found the page in CC, but a non 200 HTTP code was crawled!
                    return None
                return response

    def get_html_from_index(self, index, url):
        index_url = 'http://index.commoncrawl.org/' + index
        resp = requests.get(index_url, params={'url': url, 'output': 'json'})
        if resp.status_code != 200:
            return None
        lines = resp.content.decode("utf-8").strip().split('\n')
        references = (json.loads(x) for x in lines)
        return get_first_or_none(map(CommonCrawlS3.fetch_html_from_s3_file, references))
        
    def get_html(self, url):
        if url in self.cache:
            return self.cache[url]

        self.call_count += 1
        all_indices = (self.get_html_from_index(index, url) for index in self.indices)
        result = get_first_or_none(all_indices)
        self.cache[url] = result
        if result is None:
            self.fail_count += 1
            fail_percent = self.fail_count * 100 / self.call_count
            logging.debug("Could not fetch grounding for {:}. Failed: {}/{} ({:.1f}%)"
                .format(url, self.fail_count, self.call_count, fail_percent))
            self.cache[url] = None
        return result

if __name__ == "__main__":
    from grounding_helpers import extract_text_bs4
    
    cc = CommonCrawlS3()
    html = cc.get_html("https://en.wikipedia.org/wiki/Barack_Obama")
    text = extract_text_bs4(html)
    print(text)