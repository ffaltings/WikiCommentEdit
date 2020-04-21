"""
Contains self-contained isolated filters/processors implemented as python generators
"""

import re
from profiling import Profiled

def page_id_filter(accepted_ids):
    def generate(meta):
        if meta['page_id'] in accepted_ids:
            yield meta
    return generate

def comment_length(min_len, max_len):
    @Profiled.generator
    def comment_length(meta):
        clen = len(meta["comment_text"])
        if clen >= min_len and clen < max_len:
            yield meta
    return comment_length

def comment_token_length(min_len, max_len):
    @Profiled.generator
    def comment_token_length(meta):
        approx_tokens = meta["comment_text"].split(" ")
        clen = len(approx_tokens)
        if clen >= min_len and clen < max_len:
            yield meta
    return comment_token_length


def comment_blocklist_filter(exclude_words = ["[[Project:AWB|AWB]]", "[[Project:AutoWikiBrowser|AWB]]", "Undid revision"]):
    @Profiled.generator
    def comment_blocklist_filter(meta):
        comment = meta["comment_text"]
        if not any(word in comment for word in exclude_words):
            yield meta
    return comment_blocklist_filter

def text_length(min_len, max_len):
    @Profiled.generator
    def text_length(instance):
        len_src = len(instance["src_text"])
        len_tgt = len(instance["tgt_text"])
        if len_src >= min_len and len_tgt >= min_len and len_src < max_len and len_tgt < max_len:
            yield instance
    return text_length

def exclude_page_types(excludes_prefixes = ["Talk:"]):
    @Profiled.generator
    def exclude_page_types(meta):
        has_any_prefix = any(prefix in meta["page_title"] for prefix in excludes_prefixes)
        if not has_any_prefix:
            yield meta
    return exclude_page_types

@Profiled.generator
def has_section_title(instance):
    if instance['section_title']:
        yield instance

def is_human_edit(instance):
    raise NotImplementedError

def has_grounding(look_in_src = True, look_in_tgt = True):

    @Profiled.generator
    def has_grounding(instance):
        sources = []
        if look_in_src: sources.append(instance["src_text"])
        if look_in_tgt: sources.append(instance["tgt_text"])
        if any("http://" in source for source in sources):
            source_text = "".join(sources)
            url_set = set(re.findall(r"https?://[^\s|\]]+", source_text))
            instance["grounding_urls"] = [url.lower() for url in url_set]
            yield instance

    return has_grounding

def remove_all_urls(replacement=''):
    @Profiled.generator
    def remove_all_urls(instance):
        def remove_urls (text):
            return re.sub(r"https?://[^\s|\]]+", replacement, text, flags=re.MULTILINE)
        instance["src_text"] = remove_urls(instance["src_text"])
        instance["tgt_text"] = remove_urls(instance["tgt_text"])
        yield instance
    return remove_all_urls

def grounding_domain_whitelist(whitelist=[], file=None):
    if file:
        with open(file, "r", encoding="utf-8") as f:
            whitelist = [x.strip() for x in f.readlines()]
    whitelist = ["://" + x for x in whitelist]

    @Profiled.generator
    def grounding_domain_whitelist(instance):
        instance["grounding_urls"] = [url for url in instance["grounding_urls"] if any(domain in url for domain in whitelist)]
        if len(instance["grounding_urls"]) > 0:
            yield instance

    return grounding_domain_whitelist


def extract_common_crawl_groundings(target_length):
    from common_crawl import CommonCrawlS3
    from grounding_helpers import extract_overlapping_tokens
    from grounding_helpers import extract_text_bs4
    from nltk import word_tokenize
    cc = CommonCrawlS3()

    @Profiled.generator
    def extract_common_crawl_groundings(instance):
        reference_tokens = set(instance["tgt_tokens"])

        def download_grounding(url):
            html = cc.get_html(url)
            if not html: return None
            text = extract_text_bs4(html)
            grounding_tokens = word_tokenize(text)
            overlap = extract_overlapping_tokens(target_length, reference_tokens, grounding_tokens)
            return overlap
        instance["grounding_docs"] = list(filter(None, map(download_grounding, instance["grounding_urls"])))
        yield instance
    
    return extract_common_crawl_groundings

@Profiled.generator
def remove_without_grounding_docs(instance):
    """Removes all documents lacking a grounding document"""
    if instance.get("grounding_docs"):
        yield instance