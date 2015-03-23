#!/usr/bin/env python
# -*- coding: utf-8 -*-

#TODO
# Log errors
# Update if greater updated date
# allow crawl into files
# remove unnecessary empty strings in raw data: " ".join(abstract.split())
import urllib
import argparse
from xml.dom import minidom
import datetime
from arxiv_parser import get_arxiv_id, get_updated_date
from db_utils import insert_raw_data_list, \
    get_all_subject_classifications, \
    get_subject_classification_by_name

BASE_URL = 'http://export.arxiv.org/api/query'
PAGE_SIZE = 100

def get_parser():
    """Get arguments"""
    parser = argparse.ArgumentParser(description="ArXiv crawler")
    parser.add_argument("-a", "--all", action="store_true", dest="all",
                        help="Crawl all arxiv if present")
    parser.add_argument("-c", type=str, dest="cat",help="Subject Classification of Arxiv")
    parser.add_argument('-i', '--id', type=str, dest="id", nargs='+',help="List of Ids of Arxiv")
    parser.add_argument('--size', type=int, dest="size",help='size of the pagination')

    return parser


def save_to_file(data, filename):
    with open(filename, 'w+') as arxiv_file:
        arxiv_file.write(data)
        print '{} file saved'.format(filename)


def fetch_arxiv_data(category, offset=0, limit=100):
    url = '{base}?search_query=cat:{cat}&start={os}&max_results={sz}'.format(
        base=BASE_URL,
        cat=category,
        os=offset,
        sz=limit)
    data = urllib.urlopen(url).read()
    return data


def fetch_arxiv_data_by_id(id_list):
    url = '{base}?id_list={ids}'.format(base=BASE_URL, ids=",".join(id_list))
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
        offset = -1 * PAGE_SIZE
        while offset < total_results:
            offset += PAGE_SIZE
            data = fetch_arxiv_data(category.Name, offset, PAGE_SIZE)
            raw_data = create_raw_data_entities(data)
            insert_raw_data_list(raw_data)
            #filename = "arxiv_{}_{}_{}.xml".format(cat, offset, limit)
            #saveToFile(data, filename)
    print '[INFO] {} end crawling {}!!'.format(datetime.datetime.now(),
                                               category.Description)


def crawl_by_id_list(id_list):
    """
    Crawl by arxiv id list
    :param id_list: list of arxiv ids
    :return:None
    """
    print '[INFO] {} crawling by id list.'.format(datetime.datetime.now())
    data = fetch_arxiv_data_by_id(id_list)
    data = minidom.parseString(data)
    raw_data = create_raw_data_entities(data)
    insert_raw_data_list(raw_data)
    print '[INFO] {} end crawling {}!!'.format(datetime.datetime.now())


def main():
    parser = get_parser()
    args = parser.parse_args()
    if args.all:
        crawl_all_categories()
    elif args.id:
        crawl_by_id_list(args.id)
    elif args.cat:
        cat = get_subject_classification_by_name(args.cat)
        if cat:
            crawl_by_category(args.cat)


if __name__ == '__main__':
    main()