"""
Downloads (on-demand) a range of dump files and processes them according to filters and processors specified below.
"""

import logging
import argparse
import urllib
import os
import io
import bz2

from azure_utils import open_azure_input_stream, upload_to_azure_output_stream
from wiki_dump_download import existFile, get_dump_task
from generic_extractor import process

from custom_filters import *
from custom_extractors import *

# Hack for HPC: cert verification issues
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def download_on_demand(url, dump_file, temp_path, compress_type):
    if not existFile(temp_path, dump_file, compress_type):
        urllib.request.urlretrieve(url, dump_file)
    else:
        logging.debug("File Exists, Skip: " + url)
    return dump_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--index', type=int, help='the index of the file (within the dump status file) to download and process')
    #parser.add_argument('--start', type=int, default=1, help='the first file to download [default: 0]')
    #parser.add_argument('--end', type=int, default=-1, help='the last file to download [default: -1]')
    parser.add_argument('--dumpstatus_path', type=str, default='./data/dumpstatus.json')
    parser.add_argument('--temp-path', type=str, default="./data/raw/", help='the temp / data directory, used to download wiki dump files')
    parser.add_argument('--output-path', type=str, default="./data/out/", help='the output directory')
    parser.add_argument('--compress-type', type=str, default='bz2', help='the compressed file type to download: 7z or bz2 [default: bz2]')
    parser.add_argument('--azure', action='store_true')
    parser.add_argument('--delete-temp-files', action='store_true', help='if set, temporary files are deleted again after run is complete')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) %(message)s')

    logging.debug("Determining dump task..")
    dump_tasks = get_dump_task(args.dumpstatus_path, args.temp_path, args.compress_type, args.index, args.index, azure=False)
    url, dump_file, cur_progress, total_num = dump_tasks.assign_task()
    download_on_demand(url, dump_file, args.temp_path, args.compress_type)

    output_file = os.path.join(args.output_path, os.path.basename(dump_file.replace(args.compress_type, "json")))
    logging.debug("Dumped to " + dump_file + " processing to " + output_file)

    ## add the filtering criteria and processing steps here. Each step is self-contained and removable
    filters_and_processors = [
        ExcludePageTypes(["Talk:"]),
        CommentLength(20, 200),
        CommentBlocklist(["[[Project:AWB|AWB]]", "[[Project:AutoWikiBrowser|AWB]]", "Undid revision"]),
        CommentTokenLength(2, 1000),
        HasSectionTitle(),
        TextLength(0, 1000000),
        HasGrounding(look_in_src=True, look_in_tgt=True),
        ExtractLeftRightContext(5, 5)
    ]

    ## chose extractor here, determines how each instance is serialized to output_stream
    extractor = TsvExtractor(["comment"])
    extractor = NDJsonExtractor()

    wiki_input_stream = open_azure_input_stream() if args.azure else bz2.open(dump_file, "rt", encoding='utf-8')
    json_output_stream = io.StringIO() if args.azure else open(output_file, "w", buffering=1, encoding='utf-8')

    process(1, wiki_input_stream, json_output_stream, 1, 10, ctx_window=5, extractor=extractor, filters=filters_and_processors)
    
    wiki_input_stream.close()
    if not args.azure:
        json_output_stream.close()
    else:
        upload_to_azure_output_stream()
    logging.debug("Done with task %d" % args.index)

    if args.delete_temp_files:
        os.remove(dump_file)
        logging.debug("Removed temporary file " + dump_file)
        