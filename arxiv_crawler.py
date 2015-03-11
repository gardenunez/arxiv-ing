#!/usr/bin/env python
# -*- coding: utf-8 -*-

#TODO
# Log errors
# Add arguments to script: crawl all, category, list-of-ids, etc
# Update if greater updated date
# allow crawl into files
# Subject categories into DB
import urllib
import argparse
from xml.dom import minidom
import datetime
from arxiv_subject_classification import SUBJECT_CLASSIFICATION
from arxiv_parser import getArxivId, getUpdatedDate
from db_utils import create_db, insert_raw_data_list

def get_args():
    """Get arguments"""
    parser = argparse.ArgumentParser(description="ArXiv crawler")
    parser.add_argument("-c", type=str, dest="cat",
                        help="Subject Classification or Category of Arxiv")


def save_to_file(data, filename):
    with open(filename, 'w+') as arxiv_file:
        arxiv_file.write(data)
        print '{} file saved'.format(filename)


def fetch_arxiv_data(category, offset=0, limit=100):
    url = 'http://export.arxiv.org/api/query?search_query=cat:{}&start={}&max_results={}'.format(
        category,
        offset,
        limit
    )
    data = urllib.urlopen(url).read()
    return data


def create_raw_data_entities(data):
    xml_obj = minidom.parseString(data)
    entries = xml_obj.getElementsByTagName('entry')
    raw_data_entries = []
    for entry in entries:
        arxiv_id = getArxivId(entry)
        updated_date = getUpdatedDate(entry)
        raw_data_entries.append((arxiv_id,
                                 entry.toxml().replace('\n', ''),
                                 updated_date,
                                 str(datetime.datetime.now())))
    return raw_data_entries

def crawl_all_categories():
    """Crawl all entries from all categories"""
    print '[INFO] start crawling all categories'
    for cat in SUBJECT_CLASSIFICATION.keys():
        try:
            crawl_by_category(cat)
        except Exception:
            print '[Error] crawling: {}'.format(cat)
    print '[INFO] end process!!'


def crawl_by_category(cat):
    if cat not in SUBJECT_CLASSIFICATION:
        raise Exception("Invalid category")
    cat_name = SUBJECT_CLASSIFICATION[cat]
    print '[INFO] {} crawling category: {}'.format(datetime.datetime.now(),
                                                   cat_name)
    data = fetch_arxiv_data(cat, 0, 1)
    xml_data = minidom.parseString(data)
    total_results = int(xml_data.getElementsByTagName('opensearch:totalResults')[0].childNodes[0].data)
    print '[INFO] starting to fetch {} entries'.format(total_results)
    if total_results > 0:
        offset = -100
        limit = 100
        while offset < total_results:
            offset += limit
            data = fetch_arxiv_data(cat, offset, limit)
            raw_data = create_raw_data_entities(data)
            insert_raw_data_list(raw_data)
            #filename = "arxiv_{}_{}_{}.xml".format(cat, offset, limit)
            #saveToFile(data, filename)
    print '[INFO] {} end crawling {}!!'.format(datetime.datetime.now(),
                                               cat_name)


def main():
    create_db()
    crawl_all_categories()

if __name__ == '__main__':
    main()