#!/usr/bin/env python

import sys
import datetime
import logging
import json
import argparse
import os
import io
import itertools
from copy import deepcopy

from wiki_util import *
from wiki_dump_download import existFile

def printSample(task_id, sample_count, revision_count, page_title, sect_title, comment, diff_url, parent_tokens,
                target_tokens, origin_diff, target_diff, delimitor='^'):
    logger=logging.getLogger(__name__)
    
    # print the sampled revision in excel format
    revision_info = '[' + str(len(parent_tokens)) + '|' + str(len(target_tokens)) + ']'
    origin_diff_tokens = '[' + (
        str(origin_diff[0]) if len(origin_diff) == 1 else ','.join([str(i) for i in origin_diff])) + ']'
    target_diff_tokens = '[' + (
        str(target_diff[0]) if len(target_diff) == 1 else ','.join([str(i) for i in target_diff])) + ']'               
    logger.info("[" + str(task_id) + "] " + str(sample_count) + '/' + str(
        revision_count) + delimitor + page_title + delimitor + sect_title +
                 delimitor + comment + delimitor + diff_url + delimitor + revision_info + delimitor + origin_diff_tokens + delimitor + target_diff_tokens)


def process(task_id, wiki_stream, output_stream,
                sample_ratio, min_cmnt_length, ctx_window, negative_cmnt_num=10,
                negative_edit_num=10,
                max_page_count=None, 
                azure=False,
                sentence_level=True,
                extractor=None,
                filters=[]):

    logger=logging.getLogger(__name__)
    
    start_time = datetime.datetime.now()

    sample_count = 0
    revision_count = 0
    page_count = 0
    total_instances = 0

    sample_parent_id = None
    sample_parent_text = None
    page_comment_list = []
    prev_page_title = ''

    try:
        records = split_records(wiki_stream, azure)
        ##records = itertools.islice(records, 1000) # local debugging

        for page_title, page_id, revision in records:
            revision_count += 1

            # fields 
            rev_id, parent_id, timestamp, username, userid, userip, comment, text = extract_data(revision)
            comment = cleanCmntText(comment)
            sect_title, comment = extractSectionTitle(comment)

            meta = {"page_id": page_id, "comment_text":comment,
                "text_length":len(text), "parent_id": parent_id,
                "section_title":sect_title, "page_title": page_title}

            if not all(filter.apply_meta(meta) for filter in filters):
                continue

            # store the comments
            if prev_page_title != page_title:
                # TEMP
                print('CURRENT PAGE TITLE: {}'.format(page_title))
                
                page_count += 1
                prev_page_title = page_title
                page_comment_list.clear()
                if max_page_count:
                    if page_count > max_page_count: break

            page_comment_list.append(comment)

            # write the sampled revision to json file
            # Skip the line if it is not satisfied with some criteria
            if sample_parent_id == parent_id:
                # do sample
                diff_url = 'https://en.wikipedia.org/w/index.php?title=' + \
                    page_title.replace(" ",'%20') + '&type=revision&diff=' + rev_id + '&oldid=' + parent_id

                # check whether the comment is appropriate by some criteria
                try:
                    src_text = extractSectionText(sample_parent_text, sect_title)
                    tgt_text = extractSectionText(text, sect_title)
                except:
                    print("ERROR-RegularExpression:", sample_parent_text, text, " Skip!!")
                    # skip the revision if any exception happens
                    continue

                # clean the wiki text
                src_text = cleanWikiText(src_text)
                tgt_text = cleanWikiText(tgt_text)

                rev_instance = {"page_id": page_id, "revision_id": rev_id, "parent_id": parent_id, "timestamp": timestamp, \
                            "diff_url": diff_url, "page_title": page_title, \
                            "src_text": src_text, "tgt_text": tgt_text,
                            "comment": comment }

                if not all(filter.apply_pre_diff(rev_instance) for filter in filters):
                    continue

                # tokenization
                src_sents, src_tokens = tokenizeText(src_text)
                tgt_sents, tgt_tokens = tokenizeText(tgt_text)

                # extract the offset of the changed tokens in both src and tgt
                src_token_diff, tgt_token_diff = diffRevision(src_tokens, tgt_tokens)

                if len(src_token_diff) == 0 and len(tgt_token_diff) == 0:
                    continue

                if (len(src_token_diff) > 0 and src_token_diff[0] < 0) or (
                        len(tgt_token_diff) > 0 and tgt_token_diff[0] < 0):
                    continue

                if src_sents == None or tgt_sents == None:
                    continue

                src_ctx_tokens, src_action = extContext(src_tokens, src_token_diff, ctx_window)
                tgt_ctx_tokens, tgt_action = extContext(tgt_tokens, tgt_token_diff, ctx_window)

                rev_instance.update({"src_token": src_ctx_tokens, "src_action": src_action,
                                  "tgt_token": tgt_ctx_tokens, "tgt_action": tgt_action})

                if not all(filter.apply_post_diff(rev_instance) for filter in filters):
                    continue

                # src_sent_diff = findSentDiff(src_sents, src_tokens, src_token_diff)
                tgt_sent_diff = findSentDiff(tgt_sents, tgt_tokens, tgt_token_diff)
            
                if sentence_level:

                    extracted_sentences = 0
                    for i in tgt_sent_diff:
                        # for each sentence instance, create a deep copy, so that filters/processors can mutate them
                        sent_instance = deepcopy(rev_instance)
                        sent_instance['edits'] = tgt_sents[i]
                        sent_instance['left_sentence'] = tgt_sents[i-1] if i-1 >= 0 else None
                        sent_instance['right_sentence'] = tgt_sents[i+1] if i+1 < len(tgt_sents) else None

                        if not all(filter.apply_instance(sent_instance) for filter in filters):
                            continue
                        
                        extracted_sentences += 1
                        extractor.write_instance(output_stream, sent_instance)

                    total_instances += extracted_sentences
                    if extracted_sentences > 0:
                        sample_count += 1
                        #printSample(task_id, sample_count, revision_count, page_title, sect_title, comment, diff_url,
                        #            src_tokens, tgt_tokens, src_token_diff, tgt_token_diff)

                else:
                    # randomly sample the negative comments
                    if negative_cmnt_num > len(page_comment_list) - 1:
                        neg_cmnt_idx = range(len(page_comment_list) - 1)
                    else:
                        neg_cmnt_idx = random.sample(range(len(page_comment_list) - 1), negative_cmnt_num)
                    neg_comments = [page_comment_list[i] for i in neg_cmnt_idx]

                    # generate the positive edits
                    pos_edits = [tgt_sents[i] for i in tgt_sent_diff]

                    # generate negative edits
                    neg_edits_idx = [i for i in range(len(tgt_sents)) if i not in tgt_sent_diff]
                    if negative_edit_num > len(neg_edits_idx):
                        sampled_neg_edits_idx = neg_edits_idx
                    else:
                        sampled_neg_edits_idx = random.sample(neg_edits_idx, negative_edit_num)
                    neg_edits = [tgt_sents[i] for i in sampled_neg_edits_idx]

                    if (len(src_token_diff) > 0 or len(tgt_token_diff) > 0):
                        rev_instance = {"revision_id": rev_id, "parent_id": parent_id, "timestamp": timestamp, \
                                        "diff_url": diff_url, "page_title": page_title, \
                                        "src_text": src_text, "tgt_text": tgt_text,
                                        "comment": comment, "src_token": src_ctx_tokens, "src_action": src_action, \
                                        "tgt_token": tgt_ctx_tokens, "tgt_action": tgt_action, \
                                        "neg_cmnts": neg_comments, "neg_edits": neg_edits, "pos_edits": pos_edits
                                        }
                        
                        if not all(filter.apply_instance(rev_instance) for filter in filters):
                            continue

                        extractor.write_instance(output_stream, rev_instance)
                        sample_count += 1
                        total_instances += 1
                        printSample(task_id, sample_count, revision_count, page_title, sect_title, comment, diff_url,
                                    src_tokens, tgt_tokens, src_token_diff, tgt_token_diff)

            # decide to sample next
            if sampleNext(sample_ratio):
                sample_parent_id = rev_id
                sample_parent_text = text
            else:
                sample_parent_id = None
                sample_parent_text = None

    finally:
        time_elapsed = datetime.datetime.now() - start_time
        logger.debug("=== " + str(sample_count) + " revisions sampled in total " + str(revision_count) + " revisions " \
                      + 'across ' + str(page_count) + ' pages. ' \
                      + 'Total instances: ' + str(total_instances) + ". " \
                      + 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed) + ' ===')
            

