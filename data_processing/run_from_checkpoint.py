"""
Runs a partial pipeline from a set of NDJson checkpoint files
"""

import os, io, argparse
import logging
import urllib
import json

from functools import partial
from tqdm import tqdm
from generator_chaining import chain_generators
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

def process(input_stream, base_generator, processors):
    """Applies the base_generator on input_stream, then chains processor steps in processors, finally uses extractor to write to output_stream"""
    iterable = tqdm(base_generator(input_stream), "baseline generator", mininterval=3.0)
    results = chain_generators(iterable, processors)
    for instance in tqdm(results, "final results", mininterval=3.0):
        Profiled.total_count += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-path', type=str, default="./data/in/", help='the input directory')
    parser.add_argument('--output-path', type=str, default="./data/out/", help='the output directory')
    parser.add_argument('--index', type=int, help='the index of the file in the input path to process')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) %(message)s')
    checkpoint_file = os.path.join(args.input_path, "{}.json".format(args.index))
    output_file = os.path.join(args.output_path, "{}.json".format(args.index))
    checkpoint_input_stream = open(checkpoint_file, "rt", encoding='utf-8')
    json_output_stream = open(output_file, "w", buffering=1, encoding='utf-8')

    ### chose processing and filtering steps here:
    processors = [
        # has_section_title,
        # comment_length(20, 200),
        # exclude_page_types(["Talk:", "User talk:"]),
        # comment_blocklist_filter(["[[Project:AWB|AWB]]", "[[Project:AutoWikiBrowser|AWB]]", "Undid revision"]),
        # comment_token_length(2, 1000),
        # text_length(5, 10000000),
        # restrict_to_section,
        # has_grounding(look_in_src=True, look_in_tgt=True),
        # grounding_domain_whitelist(file=scriptdir("domains-official.txt")), ## NOTE: disabled for now
        # remove_all_urls(replacement='URL'),
        # clean_markup_mediawikiparser,
        # clean_markup_custom,
        # tokenize(mode='nltk'), ## NOTE: mode can be 'spacy' or 'nltk'
        # compute_diff,
        # find_continous_edits,
        # filter_single_edit_span, # alternative step: split_into_continuous_edits,
        # filter_additions(min_length=3, max_length=200),
        # extract_sentence_context_around_target(1, 1), # original: extract_context_around_diff(ctx_window_size=5),
        # save_to_disk(json_output_stream, NDJsonExtractor()), # chose extractor here

        ## Start processing from here ##
        #extract_common_crawl_groundings(), # download grounding documents from CommonCrawl
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