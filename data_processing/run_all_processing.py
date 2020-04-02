import logging
import argparse
import urllib
import os
import io
import bz2

from dotenv import load_dotenv
load_dotenv()
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from wiki_dump_download import existFile, get_dump_task
from generic_extractor import process

from custom_filters import *
from custom_extractors import NDJsonExtractor

def download_on_demand(url, dump_file, data_path, compress_type):
    if not existFile(data_path, dump_file, compress_type):
        urllib.request.urlretrieve(url, dump_file)
    else:
        logging.debug("File Exists, Skip: " + url)

    return dump_file



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', type=int, help='the index of the file to download')
    #parser.add_argument('--start', type=int, default=1, help='the first file to download [default: 0]')
    #parser.add_argument('--end', type=int, default=-1, help='the last file to download [default: -1]')
    parser.add_argument('--dumpstatus_path', type=str, default='./data/raw/dumpstatus.json')
    parser.add_argument('--data-path', type=str, default="./data/raw/", help='the data directory')
    parser.add_argument('--compress-type', type=str, default='bz2', help='the compressed file type to download: 7z or bz2 [default: bz2]')
    parser.add_argument('--azure', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) %(message)s')

    logging.debug("Determining dump task..")
    dump_tasks = get_dump_task(args.dumpstatus_path, args.data_path, args.compress_type, args.index, args.index, azure=False)
    url, dump_file, cur_progress, total_num = dump_tasks.assign_task()
    download_on_demand(url, dump_file, args.data_path, args.compress_type)
    output_file = dump_file.replace(args.compress_type, "json")
    logging.debug("Dumped to " + dump_file + " processing to " + output_file)

    ## add the filtering criteria and processing steps here. Each step is self-contained and removable
    filters_and_processors = [
        ExcludePageTypes(["Talk:"]),
        CommentLength(20, 200),
        HasSectionTitle(),
        TextLength(0, 1000000),
        HasGrounding(look_in_src=True, look_in_tgt=True),
        ExtractLeftRightContext(5, 5)
    ]

    def open_azure_input_stream():
        raise NotImplementedError

    def upload_to_azure_output_stream():
        #json_output_stream.seek(0)
        #data = json_output_stream.read().encode('utf-8')
        #blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_file)
        #container_client = blob_service_client.get_container_client(container_name)
        #if existFile('processed/', output_file, container_client, azure=2):
        #    blob_client.delete_blob()
        #blob_client.upload_blob(data)
        raise NotImplementedError

    wiki_input_stream = open_azure_input_stream() if args.azure else bz2.open(dump_file, "rt", encoding='utf-8')
    json_output_stream = io.StringIO() if args.azure else open(output_file, "w", buffering=1, encoding='utf-8')

    process(1, wiki_input_stream, json_output_stream, 1, 10, ctx_window=5, extractor=NDJsonExtractor(), filters=filters_and_processors)
    
    wiki_input_stream.close()
    if not args.azure:
        json_output_stream.close()
    else:
        upload_to_azure_output_stream()
