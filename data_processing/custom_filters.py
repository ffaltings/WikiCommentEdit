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
    def generate(meta):
        clen = len(meta["comment_text"])
        if clen >= min_len and clen < max_len:
            yield meta
    return generate

def comment_token_length(min_len, max_len):
    def generate(meta):
        approx_tokens = meta["comment_text"].split(" ")
        clen = len(approx_tokens)
        if clen >= min_len and clen < max_len:
            yield meta
    return generate


def comment_blocklist_filter(exclude_words = ["[[Project:AWB|AWB]]", "[[Project:AutoWikiBrowser|AWB]]", "Undid revision"]):
    def generate(meta):
        comment = meta["comment_text"]
        if not any(word in comment for word in exclude_words):
            yield meta
    return generate

def text_length(min_len, max_len):
    def generate(instance):
        len_src = len(instance["src_text"])
        len_tgt = len(instance["tgt_text"])
        if len_src >= min_len and len_tgt >= min_len and len_src < max_len and len_tgt < max_len:
            yield instance
    return generate

def exclude_page_types(excludes_prefixes = ["Talk:"]):
    def generate(meta):
        has_any_prefix = any(prefix in meta["page_title"] for prefix in excludes_prefixes)
        if not has_any_prefix:
            yield meta
    return generate

def has_section_title(instance):
    if instance['section_title']:
        yield instance

def is_human_edit(instance):
    raise NotImplementedError

def has_grounding(look_in_src = True, look_in_tgt = True):
    def generate(instance):
        sources = []
        if look_in_src: sources.append(instance["src_text"])
        if look_in_tgt: sources.append(instance["tgt_text"])
        if any("http://" in source for source in sources):
            source_text = "".join(sources)
            instance["grounding_urls"] = list(set(re.findall(r"http://[^\s|\]]+", source_text)))
            yield instance

    return generate

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


def extract_left_right_context(left_window_size, right_window_size):
    def generate(instance):
        if len(instance["src_action"]) < 1:
            return
        #start_token_idx = instance["src_action"][0]
        #end_token_idx = instance["src_action"][-1]
        yield instance

    return generate
