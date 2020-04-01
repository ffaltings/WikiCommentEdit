import logging
import argparse
import urllib
import os

from wiki_dump_download import existFile, get_dump_task
from wikicmnt_extractor_generic import randSampleRev

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
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format='(%(threadName)s) %(message)s')

    logging.debug("Determining dump task..")
    dump_tasks = get_dump_task(args.dumpstatus_path, args.data_path, args.compress_type, args.index, args.index, azure=False)
    url, dump_file, cur_progress, total_num = dump_tasks.assign_task()
    download_on_demand(url, dump_file, args.data_path, args.compress_type)
    output_file = dump_file.replace(args.compress_type, "json")
    logging.debug("Dumped to " + dump_file + " processing to " + output_file)

    randSampleRev(1, dump_file, output_file, 1, 10, 5, count_revision_only=False)



