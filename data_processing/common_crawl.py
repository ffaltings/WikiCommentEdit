import json, requests, gzip
import logging
import urllib
from io import StringIO, BytesIO
from functools import partial
from concurrent.futures import ThreadPoolExecutor

def silence_exceptions_with_none(f):
    def ignoring_f(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            return None
    return ignoring_f

def get_first_or_none(iter):
    return next(filter(None, iter), None)

def first_result_parallel(func, iter, n_workers):
    """Executes a function func in parallel by n_workers until one result is not None, then returns this result preemptively.
        Returns (result, n_attempts)."""

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    attempts = 0
    batches = list(chunks(iter, n_workers))
    with ThreadPoolExecutor(max_workers = n_workers) as executor:
        for batch in batches:
            attempts += len(batch)
            result = get_first_or_none(executor.map(func, batch))
            if result: 
                return result, attempts

    return (None, attempts)

class CommonCrawlS3():

    # hide logging of urllib3
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    DefaultIndices = [
        'CC-MAIN-2020-16',
        'CC-MAIN-2020-10',
        'CC-MAIN-2020-05',
        'CC-MAIN-2019-51',
        'CC-MAIN-2019-47',
        'CC-MAIN-2019-43',
        'CC-MAIN-2019-39',
        'CC-MAIN-2019-35',
        'CC-MAIN-2019-30',
        'CC-MAIN-2019-26',
        'CC-MAIN-2019-22',
        'CC-MAIN-2019-18',
        'CC-MAIN-2019-13',
        'CC-MAIN-2019-09',
        'CC-MAIN-2019-04',
        'CC-MAIN-2018-51',
        'CC-MAIN-2018-47',
        'CC-MAIN-2018-43',
        'CC-MAIN-2018-39',
        'CC-MAIN-2018-34',
        'CC-MAIN-2018-30',
        'CC-MAIN-2018-26',
        'CC-MAIN-2018-22',
        'CC-MAIN-2018-17',
        'CC-MAIN-2018-13',
        'CC-MAIN-2018-09',
        'CC-MAIN-2018-05',
        'CC-MAIN-2017-51',
        'CC-MAIN-2017-47',
        'CC-MAIN-2017-43',
        'CC-MAIN-2017-39',
        'CC-MAIN-2017-34',
        'CC-MAIN-2017-30',
        'CC-MAIN-2017-26',
        'CC-MAIN-2017-22',
        'CC-MAIN-2017-17',
        'CC-MAIN-2017-13',
        'CC-MAIN-2017-09',
        'CC-MAIN-2017-04',
        'CC-MAIN-2016-50',
        'CC-MAIN-2016-44',
        'CC-MAIN-2016-40',
        'CC-MAIN-2016-36',
        'CC-MAIN-2016-30',
        'CC-MAIN-2016-26',
        'CC-MAIN-2016-22',
        'CC-MAIN-2016-18',
        'CC-MAIN-2016-07',
        'CC-MAIN-2015-48',
        'CC-MAIN-2015-40',
        'CC-MAIN-2015-35',
        'CC-MAIN-2015-32',
        'CC-MAIN-2015-27',
        'CC-MAIN-2015-22',
        'CC-MAIN-2015-18',
        'CC-MAIN-2015-14',
        'CC-MAIN-2015-11',
        'CC-MAIN-2015-06',
        'CC-MAIN-2014-52',
        'CC-MAIN-2014-49',
        'CC-MAIN-2014-42',
        'CC-MAIN-2014-41',
        'CC-MAIN-2014-35',
        'CC-MAIN-2014-23',
        'CC-MAIN-2014-15',
        'CC-MAIN-2014-10',
        'CC-MAIN-2013-48',
        'CC-MAIN-2013-20',
        'CC-MAIN-2012',
        'CC-MAIN-2009-2010',
        'CC-MAIN-2008-2009'
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

    @silence_exceptions_with_none
    def get_html_from_index(self, index, url):
        index_url = 'http://index.commoncrawl.org/' + index + "-index"
        params = [
            ('output', 'json'),
            ('limit', 1),
            ('url', url)]
        #queryparams = urllib.parse.urlencode(params)
        resp = requests.get(index_url, params)
        if resp.status_code != 200:
            return None
        lines = resp.content.decode("utf-8").strip().split('\n')
        references = (json.loads(x) for x in lines)
        return get_first_or_none(map(CommonCrawlS3.fetch_html_from_s3_file, references))
        
    def get_html(self, url):
        if url in self.cache:
            return self.cache[url]

        self.call_count += 1
        result, attempts = first_result_parallel(partial(self.get_html_from_index, url=url), self.indices, n_workers = 10)
        self.cache[url] = result
        if result is None:
            self.fail_count += 1
            fail_percent = self.fail_count * 100 / self.call_count
            logging.debug("Could not fetch grounding for {:} after trying {} indices. Failed: {}/{} ({:.1f}%)"
                .format(url, attempts, self.fail_count, self.call_count, fail_percent))
            self.cache[url] = None
        else:
            logging.debug("Succedded fetching grounding document for {} after trying {} indices".format(url, attempts))
        return result

if __name__ == "__main__":
    from grounding_helpers import extract_text_bs4
    
    cc = CommonCrawlS3()
    html = cc.get_html("https://en.wikipedia.org/wiki/Barack_Obama")
    text = extract_text_bs4(html)
    print(text)