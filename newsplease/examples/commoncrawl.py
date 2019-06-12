#!/usr/bin/env python
"""
This scripts downloads WARC files from commoncrawl.org's news crawl and extracts articles from these files. You can
define filter criteria that need to be met (see YOUR CONFIG section), otherwise an article is discarded. Currently, the
script stores the extracted articles in JSON files, but this behaviour can be adapted to your needs in the method
on_valid_article_extracted. To speed up the crawling and extraction process, the script supports multiprocessing. You can
control the number of processes with the parameter my_number_of_extraction_processes.

You can also crawl and extract articles programmatically, i.e., from within your own code, by using the class
CommonCrawlCrawler provided in newsplease.crawler.commoncrawl_crawler.py

In case the script crashes and contains a log message in the beginning that states that only 1 file on AWS storage
was found, make sure that awscli was correctly installed. You can check that by executing aws --version from a terminal.
If aws is not installed, you can (on Ubuntu) also install it using sudo apt-get install awscli.

This script uses relative imports to ensure that the latest, local version of news-please is used, instead of the one
that might have been installed with pip. Hence, you must run this script following this workflow.
git clone https://github.com/fhamborg/news-please.git
cd news-please
python3 -m newsplease.examples.commoncrawl
"""
import argparse
import gzip
import json
import hashlib
import json
import logging
import multiprocessing
import os
import time

from ..crawler import commoncrawl_crawler as commoncrawl_crawler

__author__ = "Felix Hamborg"
__copyright__ = "Copyright 2017"
__credits__ = ["Sebastian Nagel"]


parser = argparse.ArgumentParser(description='')
parser.add_argument('--warc-file', required=True)
parser.add_argument('--outfile', required=True) # .gz file with json data of all articles
parser.add_argument('--download-dir', default=None, required=False) # download dir for original .gz file
args = parser.parse_args()


############ YOUR CONFIG ############
# download dir for warc files
my_local_download_dir_warc = args.download_dir
# download dir for articles
my_local_download_dir_article = args.outfile
my_local_download_dir_log = args.outfile + '.log'
# hosts (if None or empty list, any host is OK)
my_filter_valid_hosts = []  # example: ['elrancaguino.cl']
# start date (if None, any date is OK as start date), as datetime
my_filter_start_date = None  # datetime.datetime(2016, 1, 1)
# end date (if None, any date is OK as end date), as datetime
my_filter_end_date = None  # datetime.datetime(2016, 12, 31)
# if date filtering is strict and news-please could not detect the date of an article, the article will be discarded
my_filter_strict_date = True
# if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
# again. Note that there is no check whether the file has been downloaded completely or is valid!
my_reuse_previously_downloaded_files = True
# continue after error
my_continue_after_error = True
# show the progress of downloading the WARC files
my_show_download_progress = True
# log_level
my_log_level = logging.INFO
# number of extraction processes
my_number_of_extraction_processes = 1
# if True, the WARC file will be deleted after all articles have been extracted from it
my_delete_warc_after_extraction = False
# if True, will continue extraction from the latest fully downloaded but not fully extracted WARC files and then
# crawling new WARC files. This assumes that the filter criteria have not been changed since the previous run!
my_continue_process = True
############ END YOUR CONFIG #########

logger = logging.getLogger('chardet.charsetprober')
logger.setLevel(my_log_level)

from newsplease import NewsPlease



def __get_pretty_filepath(path, article):
    """
    Pretty might be an euphemism, but this function tries to avoid too long filenames, while keeping some structure.
    :param path:
    :param name:
    :return:
    """
    short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
    sub_dir = article.source_domain
    final_path = path + sub_dir + '/'
    if not os.path.exists(final_path):
        os.makedirs(final_path)
    return final_path + short_filename + '.json'


def process_warc_record(record):
    try:
        if record.rec_type == 'response':
            article = NewsPlease.from_warc(record)
            if article is not None:
                return json.dumps(article.__dict__, default=str, separators=(',', ':'))
    except Exception as e:
        log = logging.getLogger()
        log.warning('skipping record due to Exception: ' + str(e))
    return None


class TimeMeter(object):
    """Computes the average occurrence of some event per second"""
    def __init__(self, init=0):
        self.reset(init)

    def reset(self, init=0):
        self.init = init
        self.start = time.time()
        self.n = 0

    def update(self, val=1):
        self.n += val

    @property
    def avg(self):
        return self.n / self.elapsed_time

    @property
    def elapsed_time(self):
        return self.init + (time.time() - self.start)


def on_valid_article_extracted(warc_records):
    """
    This function will be invoked for each article that was extracted successfully from the archived data and that
    satisfies the filter criteria.
    :param articles:
    :return:
    """
    num_records = len(warc_records)
    log = logging.getLogger()
    start_time = time.time()
    with multiprocessing.Pool() as pool:
        json_articles = pool.imap_unordered(process_warc_record, warc_records, 25)

        with gzip.open(my_local_download_dir_article, 'wt', encoding='utf-8') as outfile:
            timer = TimeMeter()
            for i, article in enumerate(json_articles):
                if article is not None:
                    print(article, file=outfile)
                timer.update(1)
                if i % 100 == 0:
                    log.info('extraction timer: {}/{} at {} articles per second'.format(
                        i+1, num_records, round(timer.avg, 1),
                    ))

    end_time = time.time()
    with open(my_local_download_dir_article + '.done', 'w') as h:
        print('done in {} seconds'.format(round(end_time - start_time, 1)), file=h)
    print('done in {} seconds'.format(round(end_time - start_time, 1)))


if __name__ == '__main__':
    commoncrawl_crawler.crawl_from_commoncrawl(on_valid_article_extracted,
                                               valid_hosts=my_filter_valid_hosts,
                                               start_date=my_filter_start_date,
                                               end_date=my_filter_end_date,
                                               strict_date=my_filter_strict_date,
                                               reuse_previously_downloaded_files=my_reuse_previously_downloaded_files,
                                               local_download_dir_warc=my_local_download_dir_warc,
                                               continue_after_error=my_continue_after_error,
                                               show_download_progress=my_show_download_progress,
                                               number_of_extraction_processes=my_number_of_extraction_processes,
                                               log_level=my_log_level,
                                               delete_warc_after_extraction=True,
                                               continue_process=True,
                                               warc_file=args.warc_file)
