#!/usr/bin/env python
# -*- coding: utf-8 -*-

#TODO
# Log errors
# Add arguments to script: crawl all, category, list-of-ids, etc
# Update if greater updated date
# allow crawl into files
# remove unnecessary empty strings in raw data: " ".join(abstract.split())
import urllib
import argparse
from xml.dom import minidom
import datetime
from arxiv_parser import get_arxiv_id, get_updated_date
from db_utils import insert_raw_data_list, \
    get_all_subject_classifications


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
        arxiv_id = get_arxiv_id(entry)
        updated_date = get_updated_date(entry)
        raw_data_entries.append((arxiv_id,
                                 entry.toxml().replace('\n', ''),
                                 updated_date,
                                 str(datetime.datetime.now())))
    return raw_data_entries


def crawl_all_categories():
    """Crawl all entries from all categories"""
    print '[INFO] start crawling all categories'
    subject_classifications = get_all_subject_classifications()
    for category in subject_classifications:
        try:
            crawl_by_category(category)
        except Exception:
            print '[Error] crawling: {}'.format(category.Description)
    print '[INFO] end process!!'


def crawl_by_category(category):
    """
    Crawl all entries by category
    :param category: Subject classification to crawl
    :return:None
    """
    print '[INFO] {} crawling category: {}'.format(datetime.datetime.now(),
                                                   category.Description)
    data = fetch_arxiv_data(category.Name, 0, 1)
    xml_data = minidom.parseString(data)
    total_results = int(xml_data.getElementsByTagName('opensearch:totalResults')[0].childNodes[0].data)
    print '[INFO] starting to fetch {} entries'.format(total_results)
    if total_results > 0:
        offset = -100
        limit = 100
        while offset < total_results:
            offset += limit
            data = fetch_arxiv_data(category.Name, offset, limit)
            raw_data = create_raw_data_entities(data)
            insert_raw_data_list(raw_data)
            #filename = "arxiv_{}_{}_{}.xml".format(cat, offset, limit)
            #saveToFile(data, filename)
    print '[INFO] {} end crawling {}!!'.format(datetime.datetime.now(),
                                               category.Description)


def main():
    crawl_all_categories()

if __name__ == '__main__':
    main()