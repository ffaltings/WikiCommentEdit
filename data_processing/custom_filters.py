"""
Contains self-contained isolated filters/processors implemented as python generators
"""

import logging, re
from profiling import Profiled

def page_id_filter(accepted_ids):
    def generate(meta):
        if meta['page_id'] in accepted_ids:
            yield meta
    return generate

def has_comment(meta):
    if meta["comment_text"]: yield meta

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

    from pywb.utils.canonicalize import canonicalize

    @Profiled.generator
    def has_grounding(instance):
        sources = []
        if look_in_src: sources.append(instance["src_text"])
        if look_in_tgt: sources.append(instance["tgt_text"])
        if any("http://" in source for source in sources):
            source_text = "".join(sources)
            url_set = list(set(re.findall(r"https?://[^\s|\]]+", source_text)))
            instance["grounding_urls"] = [url.lower() for url in url_set]

            try:
                instance["grounding_canonical_urls"] = [canonicalize(url) for url in url_set]
                yield instance
            except:
                pass

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


def extract_common_crawl_groundings(prejoined_index_file = None):
    from common_crawl import CommonCrawlS3, PreindexedCommonCrawlS3
    from grounding_helpers import extract_text_bs4
    cc = PreindexedCommonCrawlS3(prejoined_index_file) if prejoined_index_file else CommonCrawlS3()

    @Profiled.generator
    def extract_common_crawl_groundings(instance):
        def download_grounding(url):
            try:
                html = cc.get_html(url)
                if not html: return None
                text = extract_text_bs4(html)
                return text

            except Exception as e:
                logging.error("Error fetching grounding for {}: {}".format(url, str(e)))
                return None
        
        grounding_docs = list(filter(None, map(download_grounding, instance["grounding_urls"])))
        instance["grounding_docs"] = grounding_docs
        yield instance
    
    return extract_common_crawl_groundings

def extract_grounding_snippet(target_length, min_overlap_tokens):
    from grounding_helpers import extract_overlapping_tokens
    from nltk import word_tokenize
    from nltk.corpus import words
    from nltk.corpus import stopwords

    stopwords = stopwords.words('english')
    #english_words = set(words.words('en'))
    #punctuation = set("{}[]()<>.,;!?+-'’‘\\/`")
    #subtract_set = punctuation.union(stopwords)

    @Profiled.generator
    def extract_grounding_snippet(instance):
        target_tokens = set(instance["tgt_tokens"])
        approx_tokens = set(instance["comment_text"].split(" "))
        reference_tokens = target_tokens.union(approx_tokens)
        reference_tokens = set(r for r in reference_tokens if re.match(r"\w+", r))
        reference_tokens = reference_tokens.difference(stopwords)

        def extract_snippet(text):
            tokens = word_tokenize(text)
            overlap, overlap_count = extract_overlapping_tokens(target_length, reference_tokens, tokens)
            if overlap_count < min_overlap_tokens:
                return None
            overlap_text = " ".join(overlap) # dirty hack
            return overlap_text

        snippets = list(filter(None, map(extract_snippet, instance["grounding_docs"])))
        instance["grounding_snippets"] = " ;; ".join(snippets) if len(snippets) else None # dirty hack
        yield instance

    return extract_grounding_snippet

@Profiled.generator
def remove_without_grounding_snippets(instance):
    """Removes all documents lacking a grounding snippet"""
    if instance.get("grounding_snippets"):
        yield instance

@Profiled.generator
def remove_without_grounding_docs(instance):
    """Removes all documents lacking a grounding document"""
    if instance.get("grounding_docs"):
        yield instance