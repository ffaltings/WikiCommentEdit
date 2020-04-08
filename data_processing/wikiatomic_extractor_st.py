import sys
import datetime
import bz2
import logging
import json
import argparse
import os
import io
import wiki_util

sys.path.append('../')
from wiki_util import *
from wiki_dump_download import existFile

#def splitSections(revision):
#    source_sections, source_titles = split_into_sections(revision['source']['text'])
#    target_sections, target_titles = split_into_sections(revision['target']['text'])
#    
#    tgt_sect_dict = {title: text for title,text in zip(target_titles, target_sections)}
#
#    for src_sect, src_title in zip(source_sections, source_titles):
#        # recover matching target sections
#        try: tgt_sect = tgt_sect_dict[src_title]
#        except KeyError: continue
#
#        revision['source']['text'] = src_sect
#        revision['target']['text'] = tgt_sect
#        revision['source']['sect_title'] = src_title
#        revision['target']['sect_title'] = tgt_title
#
#        yield revision
#
#def processRefs(revision):
#    revision['source']['text'], revision['source']['refs'] = retrieveReferences(revision['source']['text'])
#    revision['target']['text'], revision['target']['refs'] = retrieveReferences(revision['target']['text'])
#
#    yield revision
#
#def stripMarkup(revision):
#    revision['source']['text'] = str(mwparserfromhell.parse(revision['source']['text']).strip_code())
#    revision['target']['text'] = str(mwparserfromhell.parse(revision['target']['text']).strip_code())
#
#    yield revision
#
#def tokenize(revision):
#    splitter = SentenceSplitter(language='en') # might want to pass this object by reference?
#
#    def _tokenize(text):
#        return [work_tokenize(s) for s in splitter.split(text) if len(s)]
#
#    revision['source']['sentences'] = _tokenize(revision['source']['text'])
#    revision['target']['sentences'] = _tokenize(revision['target']['text'])
#
#    yield revision
#
#def splitSentences(revision, k=5):
#    
#    for i, s in enumerate(revision['source']['sentences']):
#        min_idx = max(i-k, 0)
#        max_idx = min(i+k, len(revision['target']['sentences']))
#
#        if min_idx >= max_idx: continue
#
#        bleu_scores = [sentence_bleu([s], revision['target']['sentences'][j])\
#                for j in range(min_idx, max_idx)]
#
#        match_idx = np.argmax(bleu_scores) + min_idx
#        match_score = np.max(bleu_scores)
#
#        revision['source']['text'] = s
#        revision['target']['text'] = revision['target']['sentences'][match_idx]
#        revision['target']['match_score'] = match_score
#
#        yield revision
#
#def diffText(revision):
#    revision['source']['diff'], revision['target']['diff'] = diffRevision(
#            revision['source']['text'], revision['target']['text'])
#    yield revision
#
#def sequentialProcess(task_id, wiki_stream, output_streak, azure=False, extractor=None,
#        processors = []):
#
#    logger = logging.getLogger(__name__)
#
#    start_time = datetime.datetime.now()
#
#    sample_count = 0
#    revision_count = 0
#    page_count = 0
#    total_instances = 0
#
#    sample_parent_id = None
#    sample_parent_text = None
#    prev_page_title = ''
#
#    splitter = SentenceSplitter(language='en')
#
#    try:
#        records = split_records(wiki_stream, azure)
#        records = itertools.islice(records, 1000) # debug
#
#        for page_title, page_id, revision in records:
#            revision_count +=  1
#
#            rev_id, parent_id, timestamp, username, userid, userip, comment,\
#                    text = extract_data(revision)
#            comment = cleanCmntText(comment)
#            sect_title, comment = extractSectionTitle(comment)
#
#            revision = {"page_id": page_id, "comment_text": comment, "page_title": page_title,
#                    "section_title": sec_title}
#            revision['source'] = {'text': text, 'parent_id': parent_id, 'id': rev_id}
#
#            if not all(filter.apply_meta(meta) for filter in filters):
#                continue
#
#            if prev_page_title != page_title:
#                page_count += 1
#                prev_page_title = page_title
#            
#            # check that parent id matches previous id
#            if sample_parent_id == parent_id:
#                revision['target'] = {'text': sample_parent_text, 'id': sample_parent_id}
#
#                in_list  = [revision]
#                for processor in processors:
#                    out_list = []
#                    for item in in_list:
#                        for out in processor(item):
#                            out_list.append(out)
#                    in_list = out_list
#    finally:
#        return
#
#            # write out_list to stream
#
#def processAtomicEdits(task_id, wiki_stream, output_stream, k=5, azure=False, extractor=None,\
#        filters = []):
#
#    logger = logging.getLogger(__name__)
#
#    start_time = datetime.datetime.now()
#
#    sample_count = 0
#    revision_count = 0
#    page_count = 0
#    total_instances = 0
#
#    sample_parent_id = None
#    sample_parent_text = None
#    prev_page_title = ''
#
#    splitter = SentenceSplitter(language='en')
#
#    try:
#        records = split_records(wiki_stream, azure)
#        records = itertools.islice(records, 1000) # debug
#
#        for page_title, page_id, revision in records:
#            revision_count +=  1
#
#            rev_id, parent_id, timestamp, username, userid, userip, comment,\
#                    text = extract_data(revision)
#            comment = cleanCmntText(comment)
#            sect_title, comment = extractSectionTitle(comment)
#
#            meta = {"page_id": page_id, "comment_text": comment,
#                    "text_length": len(text), "parent_id": parend_id,
#                    "section_title": sect_title, "page_title": page_title}
#
#            if not all(filter.apply_meta(meta) for filter in filters):
#                continue
#
#            if prev_page_title != page_title:
#                page_count += 1
#                prev_page_title = page_title
#            
#            # check that parent id matches previous id
#            if sample_parent_id == parent_id:
#               
#                # split into sections
#                source_sections, source_titles =\
#                    split_into_sections(sample_parent_text)
#                target_sections, target_titles =\
#                    split_into_sections(text)
#                
#                tgt_sect_dict = {title: text for title,text in\
#                        zip(target_titles, target_sections)}
#
#                # iterate over source sections
#                for src_sect, src_title in zip(source_sections, source_titles):
#                    # recover matching target sections
#                    try: tgt_sect = tgt_sect_dict[src_title]
#                    except KeyError: continue
#                
#                    # retrieve references
#                    src_sect, src_references = retrieveReferences(src_sect)
#                    tgt_sect, tgt_references = retrieveReferences(tgt_sect)
#
#                    # strip markup
#                    src_sect =\
#                        str(mwparserfromhell.parse(src_sect).strip_code())
#                    tgt_sect =\
#                        str(mwparserfromhell.parse(src_sect).strip_code())
#
#                    # not sure where to put post diff filters since the
#                    # pipeline is slightly different here (processing
#                    # sentences)
#                    rev_instance = {"page_id" : page_id, "revision_id": rev_id,
#                            "parent_id": parent_id, "diff_url": diff_url,
#                            "page_title": page_title, "src_text": src_sect,
#                            "tgt_text": tgt_sect, "comment": comment,
#                            "src_references": src_references,
#                            "tgt_references": tgt_references}
#
#                    if not all(filter.apply_pre_diff(rev_instance) for filter
#                            in filters): continue
#
#                    # tokenize text
#                    source_sentences = [word_tokenize(s)\
#                            for s in splitter.split(src_sect) if len(s)]
#                    target_sentences = [word_tokenize(s)\
#                            for s in splitter.split(tgt_sect) if len(s)]
#
#                    for i, src_sent in enumerate(source_sentences):
#                        min_idx = max(i-k, 0)
#                        max_idx = min(i+k, len(target_sentences)
#                        if min_idx >= max_ix: continue
#
#                        bleu_scores = [sentence_bleu([src_sent], target_sentences[j])\
#                            for j in range(min_idx, max_idx)]
#
#                        match_idx = np.argmax(bleu_scores) + min_idx
#                        match_score = np.max(bleu_scores)
#
#                        tgt_sent = target_sentences[match_idx]
#                        tgt_lctx = target_sentences[max(match_idx-k,0):match_idxmatch_idx]
#                        src_lctx = source_sentences[max(i-k, 0):i]
#
#                        source_diff, target_diff = diffRevision(src_sent, tgt_sent)
#
#                        #write to stream...


def sampleEdits(dump_file, output_file):

    logger = logging.getLogger(__name__)

    json_file = open(output_file, 'w', buffering=1, encoding='utf-8')

    start_time = datetime.datetime.now()

    wiki_file = bz2.open(dump_file, 'rt', encoding='utf-8')

    edit_count = 0
    revision_count = 0
    page_count = 0

    prev_page_title = ''
    prev_comment = ''
    prev_text = ''

    try:
        for page_title, revision in split_records(wiki_file):
            revision_count += 1
            if revision_count % 200 == 0:
                logging.debug("{} revisions processed".format(revision_count))

            rev_id, parent_id, timestamp, username, userid, userip, comment,\
                    text = extract_data(revision)

            comment = cleanCmntText(comment)

            if prev_page_title != page_title:
                page_count += 1
                prev_page_tile = page_title

            text = html.unescape(text)

            for i, (src, tgt, src_ctx, tgt_ctx, src_diff, tgt_diff) in enumerate(split_edits(text,prev_text)):
                json_dict = {'revision_id': rev_id, 'parent_id': parent_id,\
                        'timestamp': timestamp, 'username': username,\
                        'userid': userid, 'comment': comment, 
                        'source_sentence': src, 'target_sentence': tgt,
                        'source_context': src_ctx, 'target_context': tgt_ctx,
                        'source_diff': src_diff, 'target_diff': tgt_diff}

                json_str = json.dumps(json_dict, indent=None, sort_keys=False,
                        separators=(',',': '), ensure_ascii=False)
                json_file.write(json_str + '\n')
                edit_count += 1

            prev_comment = comment
            prev_text = text

            if edit_count >= 1: break

    finally:
        time_elapsed = datetime.datetime.now() - start_time
        logger.debug("=== " + str(edit_count) + " edits sampled out of " \
                + str(revision_count) + "revisions. Time elapsed: "\
                + str(time_elapsed))

        json_file.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--dump_file', type=str,\
            default='raw/wikipedia-subsample/enwiki-sample-history1.xml-p1037p2028.txt.bz2')
    parser.add_argument('--output_file', type=str,\
            default='processed/atomic-edits/enwiki-sample-p1037p2028.json')
    parser.add_argument('--container_name', type=str,\
            default='wikipedia-data')
    args = parser.parse_args()

    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=args.container_name,\
            blob=args.dump_file)
    with open('input.text.bz', 'wb') as download_file:
        download_file.write(blob_client.download_blob().readall())    

    logging.basicConfig(level=logging.DEBUG,
            format='(%(threadName)s) (%(name)s) %(message)s',
            )
    
    sampleEdits('input.text.bz','output.json')

    blob_client = blob_service_client.get_blob_client(container=args.container_name,\
            blob=args.output_file)
    container_client = blob_service_client.get_container_client(args.container_name)
    if existFile('processed/', args.output_file, container_client, azure=True):
        blob_client.delete_blob()
    with open('output.json', 'rb') as data:
        blob_client.upload_blob(data)


