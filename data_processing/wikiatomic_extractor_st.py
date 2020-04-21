# TODO: Fix references to include self closing tags (<ref name=... />)

from dotenv import load_dotenv
load_dotenv()

import sys
import datetime
import bz2
import logging
import json
import argparse
import os
import io
import html
import copy
import itertools
from nltk import word_tokenize
from sentence_splitter import SentenceSplitter
from tempfile import TemporaryFile, NamedTemporaryFile
from profanityfilter import ProfanityFilter

from wiki_util import *
#from wiki_dump_download import existFile
#from run_all_processing import process, scriptdir
from custom_extractors import NDJsonExtractor
from generic_extractor import generate_revision_pairs
from generator_chaining import chain_generators
from wikitext_processing import clean_wiki_text
from custom_filters import exclude_page_types, has_section_title, has_comment

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def unescapeHTML(revision):
    revision['src_text'] = html.unescape(revision['src_text'])
    revision['tgt_text'] = html.unescape(revision['tgt_text'])
    yield revision

def retrieveSection(revision):
    def _retrieveSection(text, title):
        sections, titles = split_into_sections(text)
        try:
            sect_idx = titles.index(title)
        except ValueError:
            return ''
        return sections[sect_idx]

    title = re.escape(revision['section_title']) #important to escape title
    src_sect = extractSectionText(revision['src_text'], title)
    tgt_sect = extractSectionText(revision['tgt_text'], title)

    #only yield if found section in both source and target texts
    if src_sect and tgt_sect: 
        # remove some unused fields, restructure revision
        revision.pop('src_text')
        revision.pop('tgt_text')
        revision['source'] = {'text': src_sect}
        revision['target'] = {'text': tgt_sect}

        yield revision

def splitSections(revision):
    source_sections, source_titles = split_into_sections(revision['src_text'])
    target_sections, target_titles = split_into_sections(revision['tgt_text'])
    
    tgt_sect_dict = {title: text for title,text in zip(target_titles, target_sections)}

    for src_sect, src_title in zip(source_sections, source_titles):
        # recover matching target sections
        try: tgt_sect = tgt_sect_dict[src_title]
        except KeyError: continue

        sect_rev = {
            'rev_id': revision['rev_id'],
            'page_id': revision['page_id'],
            'page_title': revision['page_title'],
            'comment_text': revision['comment_text'],
            'section_title': src_title,
            'source': {
                'text': src_sect
                },
            'target': {
                'text': tgt_sect
                }
            }

        yield sect_rev

def processRefs(revision): #TODO: extract urls from refs and merge source and target refs
    revision['source']['text'], _ = retrieveReferences(revision['source']['text'])
    revision['target']['text'], revision['refs'] = retrieveReferences(revision['target']['text'])
    # right now use target refs
    yield revision

def stripMarkup(revision):
#    revision['source']['text'] = str(mwparserfromhell.parse(revision['source']['text']).strip_code())
#    revision['target']['text'] = str(mwparserfromhell.parse(revision['target']['text']).strip_code())
    revision['source']['text'] = ' '.join(clean_wiki_text(revision['source']['text']))
    revision['target']['text'] = ' '.join(clean_wiki_text(revision['target']['text']))

    yield revision

def tokenize(revision):
    splitter = SentenceSplitter(language='en') # might want to pass this object by reference?

    def _tokenize(text):
        return [word_tokenize(s) for s in splitter.split(text) if len(s)]

    revision['source']['sentences'] = [s for s in splitter.split(revision['source']['text'])\
            if len(s)]
    revision['target']['sentences'] = [s for s in splitter.split(revision['target']['text'])\
            if len(s)]
    revision['source']['tokens'] = [word_tokenize(s) for s in revision['source']['sentences']]
    revision['target']['tokens'] = [word_tokenize(s) for s in revision['target']['sentences']]
    yield revision

def splitSentences(k=5):
    def generator(revision):
        for i, s in enumerate(revision['source']['tokens']):
            min_idx = max(i-k, 0)
            try:
                max_idx = min(i+k, len(revision['target']['tokens']))
            except KeyError as e:
                print(revision)
                raise e

            if min_idx >= max_idx: continue

            bleu_scores = [sentence_bleu([s], revision['target']['tokens'][j])\
                    for j in range(min_idx, max_idx)]

            match_idx = np.argmax(bleu_scores) + min_idx
            match_score = np.max(bleu_scores)
            src_sent = revision['source']['sentences'][i]
            tgt_sent = revision['target']['sentences'][match_idx]
            tgt_tokens = revision['target']['tokens'][match_idx]
            
            src_ctx = revision['target']['sentences'][max(i-k, 0):i]
            tgt_ctx = revision['target']['sentences'][max(match_idx-k,0):match_idx]
            # deep copy is a potential slow down. Can speed up at the cose
            # of making the code less maintainable (i.e. select which fields
            # to copy right here...)
            sent_revision = copy.deepcopy(revision) # deep copy revision
            sent_revision['source'] = {
                    'sentence': src_sent,
                    'tokens': s,
                    'context': src_ctx}
            sent_revision['target'] = {
                'sentence': tgt_sent,
                'tokens': tgt_tokens,
                'context': tgt_ctx,
                'match_score': match_score}


      #      sent_revision = {
      #          'comment_text': revision['comment_text'],
      #          'page_id': revision['page_id'],
      #          'page_title': revision['page_title'],
      #          'rev_id': revision['rev_id'],
      #          'refs': revision['refs'],
      #          'section_title': revision['section_title'],
      #          'source': {
      #              'sentence': src_sent,
      #              'tokens': s,
      #              'context': src_ctx
      #          },
      #          'target': {
      #              'sentence': tgt_sent,
      #              'tokens': tgt_tokens,
      #              'context': tgt_ctx,
      #              'match_score': match_score
      #          }
      #      }
      #      
            yield sent_revision

    return generator

def profanityFilter():
    pf = ProfanityFilter()
    def generator(revision):
        has_profanity = pf.has_bad_word(revision['source']['sentence']) or\
                pf.has_bad_word(revision['target']['sentence'])
        if not has_profanity:
            yield revision

    return generator

def diffText(revision):
    revision['source']['diff'], revision['target']['diff'] = diffRevision(
            revision['source']['tokens'], revision['target']['tokens'])
    yield revision

def filterContiguousEdits(revision):
    if revision['source']['diff'] and not revision['target']['diff'] \
            and is_contiguous(revision['source']['diff']):
                revision['type'] = 'deletion'
                yield revision
    elif not revision['source']['diff'] and revision['target']['diff'] \
            and is_contiguous(revision['target']['diff']):
                revision['type'] = 'insertion'
                yield revision

def process(input_stream, output_stream, extractor, base_generator, processors, max_edits=0):
    iterable = tqdm(base_generator(input_stream), "baseline generator", mininterval=3.0)
    results = chain_generators(iterable, processors)
    if max_edits > 0: # 0 is special value to process everything
        results = itertools.islice(results, 0, max_edits)
    for instance in tqdm(results, "final results", mininterval=3.0):
        extractor.write_instance(output_stream, instance)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', type=int, help='index of file to process', default=1)
    parser.add_argument('--input_dir', type=str, help='directory on disk to read from',
            default='/mnt/nlp-storage/data/raw/wikipedia-subsample/')
    parser.add_argument('--output_dir', type=str, help='directory on disk to write to',
            default='/mnt/nlp-storage/data/processed/atomic-edits/temp/')
    parser.add_argument('--azure', action='store_true', default=False, help='whether to read from azure')
    parser.add_argument('--container', type=str, help='aws container', default='wikipedia-data')
    parser.add_argument('--blob_path', type=str, help='blob prefix on azure',
            default='raw/wikipedia-subsample')
    parser.add_argument('--out_path', type=str, default='processed/atomic-edits/latest/')
    parser.add_argument('--max_edits', type=int, default=0)
    parser.add_argument('--temp_dir', type=str, default='tmp') # TODO: remove this line?
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) (%(name)s) %(message)s')
    logger = logging.getLogger('azure')
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger('urllib3')
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger(__name__)

    
    if args.azure:
        # set up azure connection
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        container_client = blob_service_client.get_container_client(args.container)
        in_blobs = [b.name for b in container_client.list_blobs() if args.blob_path in b.name]
        dump_blob = in_blobs[args.index]

        logger.debug('processing file: {}'.format(dump_blob))

        input_file = NamedTemporaryFile()
        output_file = NamedTemporaryFile()
        
        blob_client = blob_service_client.get_blob_client(container=args.container,\
                blob=dump_blob)
        
        with open(input_file.name, 'wb') as f:
            f.write(blob_client.download_blob().readall())
        
        wiki_input_stream = bz2.open(input_file.name, 'rt', encoding='utf-8')
        json_output_stream = open(output_file.name, 'w', buffering=1, encoding='utf-8')

    else:
        in_files = glob.glob(os.path.join(args.input_dir, "*.txt.bz2"))
        in_file = in_files[args.index]

        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        out_file = os.path.join(args.output_dir, 'enwiki-sample-' + os.path.basename(in_file)[27:-8] + '.json')
        wiki_input_stream = bz2.open(in_file, 'rt', encoding='utf-8')
        json_output_stream = open(out_file, 'w', buffering=1, encoding='utf-8')
    


    process(
        wiki_input_stream,
        json_output_stream,
        extractor = NDJsonExtractor(),
        base_generator = generate_revision_pairs,
        processors = [
            exclude_page_types(["Talk:", "User", "Wikipedia"]),
            has_section_title,
            has_comment,
            unescapeHTML,
            retrieveSection,
   #         splitSections,
            processRefs,
            stripMarkup,
            tokenize,
            splitSentences(k=5),
            diffText,
            filterContiguousEdits
        ],
        max_edits = args.max_edits
    )

    wiki_input_stream.close()
    json_output_stream.close()
   
    if args.azure:
        out_blob = args.out_path + 'enwiki-sample-' + os.path.basename(dump_blob)[27:-8] + '.json'
        blob_client = blob_service_client.get_blob_client(container=args.container,\
                blob=out_blob)
        with open(output_file.name, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)

        input_file.close() # deletes temp file
        output_file.close()
 


    
