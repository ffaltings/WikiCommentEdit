"""
Runs a partial pipeline from a set of NDJson checkpoint files
"""

import os, io, argparse
import logging
import urllib
import json

from functools import partial
from tqdm import tqdm
from generator_chaining import process
from profiling import Profiled
from custom_filters import *
from custom_extractors import *
from generic_extractor import *

def scriptdir(filename):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

def ndjson_generator(input_stream):
    for line in input_stream:
        object = json.loads(line)
        yield object

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-path', type=str, default="./data/in/", help='the input directory')
    parser.add_argument('--output-path', type=str, default="./data/out/", help='the output directory')
    parser.add_argument('--index', type=int, help='the index of the file in the input path to process')
    parser.add_argument('--prejoined-cc-index', type=str, default=None, help='optional file containing a pre-joined CommonCrawl index in CDX format')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) %(message)s')
    checkpoint_file = os.path.join(args.input_path, "{}.json".format(args.index))
    output_file = os.path.join(args.output_path, "{}.json".format(args.index))
    checkpoint_input_stream = open(checkpoint_file, "rt", encoding='utf-8')
    json_output_stream = open(output_file, "w", buffering=1, encoding='utf-8')

    ### chose processing and filtering steps here:
    processors = [
        # has_section_title,
        # comment_length(5, 200),
        # exclude_page_types(["Talk:", "User talk:", "Wikipedia talk:", "Template talk:", "User:", "Wikipedia:"]),
        # comment_blocklist_filter(["[[Project:AWB|AWB]]", "[[Project:AutoWikiBrowser|AWB]]", "Undid revision"]),
        # comment_token_length(2, 1000),
        # text_length(5, 10000000),
        # restrict_to_section,
        # clean_urls(replacement='URL'),
        # has_urls_in_text(look_in_src=True, look_in_tgt=True),
        # grounding_domain_whitelist(file=scriptdir("domains-official.txt")), ## NOTE: disabled for now
        # clean_markup_mediawikiparser,
        # clean_markup_custom,
        # tokenize(mode='nltk'), ## NOTE: mode can be 'spacy' or 'nltk'
        # compute_diff,
        # find_continous_edits,
        # filter_single_edit_span, # alternative step: split_into_continuous_edits,
        # filter_additions(min_length=3, max_length=100),
        # restrict_grounding_to_max_distance(max_token_distance = 20), # only include URLs cited within 20 tokens of the edit
        # has_grounding(), # abort here if there is no grounding documents left
        # extract_sentence_context_around_target(1, 1), # original: extract_context_around_diff(ctx_window_size=5),
        # filter_to_min_context(min_left_tokens=10), # require some left context, right context for now optional
        # canonize_grounding(), # convert grounding_urls into canonical grounding urls for CommonCrawl

        ## Start processing from here ##
        extract_common_crawl_groundings(prejoined_index_file=args.prejoined_cc_index),
        filter_grounding_docs_by_language(languages = ['en']), # only keep english
        remove_without_grounding_docs,
        extract_grounding_snippet(target_length=500, min_overlap_tokens=5),
        remove_without_grounding_snippets,
        project_to_fields([
            'rev_id', 'page_id', 'parent_id', 'timestamp',
            'src_text', 'tgt_text', 'comment_text',
            'section_title', 'page_title',
            'diff_url',
            'src_tokens', 'tgt_tokens', 'src_action', 'tgt_action',
            'left_context', 'right_context', "left_text", "right_text",
            'grounding_urls', "grounding_docs", "grounding_canonical_urls", "grounding_snippets"]),
        save_to_disk(json_output_stream, NDJsonExtractor()), # chose extractor here
    ]
    
    process(
        checkpoint_input_stream,
        base_generator = ndjson_generator,
        processors=processors
    )
    
    checkpoint_input_stream.close()
    json_output_stream.close()
    logging.info("Done with task %d" % args.index)
    logging.info(Profiled.summarize(processors))