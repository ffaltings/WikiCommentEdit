#!/usr/bin/env python

import sys
import datetime
import logging
import json
import os
import io
import itertools
import mwparserfromhell
from copy import deepcopy
from nltk.tokenize import word_tokenize, sent_tokenize

from wiki_util import *
from profiling import Profiled

def generate_revision_pairs(wiki_stream):
    logger=logging.getLogger(__name__)
    start_time = datetime.datetime.now()

    revision_count = 0
    page_count = 0
    prev_text = '' # changed from None!
    prev_page_title = ''

    records = split_records(wiki_stream)

    for page_title, page_id, revision in records:
        revision_count += 1

        # fields 
        rev_id, parent_id, timestamp, username, userid, userip, comment, text = extract_data(revision)
        comment = cleanCmntText(comment)
#        sect_title, comment = extractSectionTitle(comment)
        sect_title = ''
        if prev_page_title != page_title:
            page_count += 1
            prev_page_title = page_title
            prev_text = text

        else:
            meta = {
                "rev_id": rev_id,
                "page_id": page_id,
                "parent_id": parent_id,
                "src_text": prev_text,
                "tgt_text": text,
                "comment_text":comment,
                "section_title":sect_title,
                "page_title": page_title,
                "timestamp": timestamp
            }

            # for next iteration, current text becomes prev_text
            prev_text = text

            yield meta

    time_elapsed = datetime.datetime.now() - start_time
    logger.debug("=== iterated through " + str(revision_count) + " revisions " \
                    + 'across ' + str(page_count) + ' pages. ' \
                    + 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed) + ' ===')

@Profiled.generator
def generate_section_pairs(meta):
    try:
        if not meta["section_title"]:
            return

        meta["src_text"] = extractSectionText(meta["src_text"], meta["section_title"])
        meta["tgt_text"] = extractSectionText(meta["tgt_text"], meta["section_title"])
        meta['diff_url'] = 'https://en.wikipedia.org/w/index.php?title=' + \
            meta["page_title"].replace(" ",'%20') + '&type=revision&diff=' + meta["rev_id"] + '&oldid=' + meta["parent_id"]

        # only yield pairs that are not empty
        if meta["src_text"] and meta["tgt_text"]:
            yield meta
    except:
        # don't yield anything if any exception happens
        logging.error("Regex error in " + str(meta['page_id']))
        return


@Profiled.generator
def clean_markup_mediawikiparser(instance):
    instance['src_text'] = str(mwparserfromhell.parse(instance['src_text'] ).strip_code())
    instance['tgt_text'] = str(mwparserfromhell.parse(instance['tgt_text']).strip_code())
    yield instance

@Profiled.generator
def clean_markup_custom(instance):
    instance['src_text'] = cleanWikiText(instance['src_text'])
    instance['tgt_text'] = cleanWikiText(instance['tgt_text'])
    yield instance

def tokenize(mode):
    def nltk_version(text):
        sentences = [word_tokenize(s) for s in sent_tokenize(text) if len(s)]
        flattened = [t for s in sentences for t in s]
        return sentences, flattened

    tokenize_impl = {"nltk": nltk_version, "spacy": tokenizeText}[mode]
    
    @Profiled.generator
    def tokenize(instance):
        instance['src_sents'], instance['src_tokens'] = tokenize_impl(instance['src_text'])
        instance['tgt_sents'], instance['tgt_tokens'] = tokenize_impl(instance['tgt_text'])
        if instance['src_sents'] == None or instance['tgt_sents'] == None:
            return
        yield instance
    
    return tokenize



@Profiled.generator
def compute_diff(instance):
    # extract the offset of the changed tokens in both src and tgt
    src_token_diff, tgt_token_diff = diffRevision(instance['src_tokens'], instance['tgt_tokens'])
    instance['tgt_token_diff'] = tgt_token_diff
    instance['src_token_diff'] = src_token_diff

    if len(src_token_diff) == 0 and len(tgt_token_diff) == 0:
        return

    if (len(src_token_diff) > 0 and src_token_diff[0] < 0) or (
            len(tgt_token_diff) > 0 and tgt_token_diff[0] < 0):
        return

    yield instance


def group_by_continuous(num_iterable):
    """Creates a nested iterable based on an iterable of numbers, where each sub-list is continuous"""
    buffer = []
    for i in num_iterable:
        if buffer and buffer[-1] + 1 == i:
            buffer.append(i)
        else:
            if buffer: yield buffer
            buffer = [i]
    if buffer: yield buffer

def find_continous_edits(instance):
    instance['tgt_token_diffs'] = list(group_by_continuous(instance['tgt_token_diff']))
    instance['src_token_diffs'] = list(group_by_continuous(instance['src_token_diffs']))
    yield instance

def filter_additions(min_length, max_length):
    def filter_additions(instance):
        len_tgt_diff = len(instance['tgt_token_diff'])
        len_src_diff = len(instance['src_token_diff'])
        if len_tgt_diff >= min_length and len_tgt_diff < max_length and len_src_diff == 0:
            yield instance
    return filter_additions

def extract_context_around_diff(ctx_window_size):
    """Creates a context window around the diff, based on tokens to be added to the left/right"""
    
    @Profiled.generator
    def extract_context(instance):
        src_ctx_tokens, src_action = extContext(instance['src_tokens'], instance['src_token_diff'], ctx_window_size)
        tgt_ctx_tokens, tgt_action = extContext(instance['tgt_tokens'], instance['tgt_token_diff'], ctx_window_size)

        instance.update({"src_tokens": src_ctx_tokens, "src_action": src_action,
                        "tgt_tokens": tgt_ctx_tokens, "tgt_action": tgt_action})
        yield instance
    return extract_context

def generate_sentence_level(instance):
    tgt_sents = instance['tgt_sents']
    tgt_tokens = instance['tgt_tokens']
    #src_tokens = instance['src_tokens']
    tgt_token_diff = instance['tgt_token_diff']
    #del instance['tgt_tokens']
    #del instance['src_tokens']
    del instance['tgt_sents']
    del instance['src_sents']
    del instance['tgt_token_diff']
    tgt_sent_diff = findSentDiff(tgt_sents, tgt_tokens, tgt_token_diff)

    extracted_sentences = 0
    for i in tgt_sent_diff:
        # for each sentence instance, create a deep copy, so that filters/processors can mutate them
        sent_instance = deepcopy(instance)
        sent_instance['tgt_sentence'] = tgt_sents[i]
        sent_instance['left_sentence'] = tgt_sents[i-1] if i-1 >= 0 else None
        sent_instance['right_sentence'] = tgt_sents[i+1] if i+1 < len(tgt_sents) else None

        extracted_sentences += 1
        yield sent_instance
