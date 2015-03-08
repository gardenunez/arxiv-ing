#!/usr/bin/env python
import urllib
import argparse
import sqlite3
from xml.dom import minidom
import datetime
from arxiv_subject_classification import SUBJECT_CLASSIFICATION


def get_args():
    """Get arguments"""
    parser = argparse.ArgumentParser(description="ArXiv crawler")
    parser.add_argument("-c", type=str, dest="cat",
                        help="Subject Classification or Category of Arxiv")


def create_db():
    print 'creating table'
    conn = sqlite3.connect('arxiv_crawler.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS raw_data
                 (arxiv_id text, data text, created_date text)''')
    conn.commit()
    conn.close()

def select_top_ten_raw_data():
    print 'selecting crawled data'
    conn = sqlite3.connect('arxiv_crawler.db')
    c = conn.cursor()

    for row in c.execute('select * from raw_data limit 100'):
        print row
    conn.close()

def clean_raw_data():
    print 'cleaning raw data table'
    conn = sqlite3.connect('arxiv_crawler.db')
    c = conn.cursor()

    c.execute('delete from raw_data')
    conn.commit()
    conn.close()

def save_to_file(data, filename):
    with open(filename, 'w+') as arxiv_file:
        arxiv_file.write(data)
        print '{} file saved'.format(filename)


def save_arxiv_data_to_db(data):
    """Save arxiv xml raw data into arxiv_crawler db"""
    conn = sqlite3.connect('arxiv_crawler.db')
    try:
        c = conn.cursor()
        xmlobj = minidom.parseString(data)
        entries = xmlobj.getElementsByTagName('entry')
        raw_data_entries = []
        for entry in entries:
            url = entry.getElementsByTagName('id')[0].childNodes[0].data
            tokens = url.split('/')
            arxiv_id = tokens[len(tokens) - 1]
            raw_data_entries.append((arxiv_id, entry.toxml().replace('\n', ''), str(datetime.datetime.now())))
        c.executemany('INSERT INTO raw_data(arxiv_id, data, created_date) values (?,?,?)',
                      raw_data_entries)
        conn.commit()
    except Exception:
        conn.close()
        raise


def fetch_arxiv_data(category, offset=0, limit=100):
    url = 'http://export.arxiv.org/api/query?search_query=cat:{}&start={}&max_results={}'.format(
        category,
        offset,
        limit
    )
    data = urllib.urlopen(url).read()
    return data


def digest_arxiv_entry(xmlsource):
    """
    Parse an arxiv xml string
    :param xmlsource: str
    :return: list of articles
    """
    xmldata = minidom.parseString(xmlsource)
    articles = []
    entries = xmldata.getElementsByTagName('entry')
    for entry in entries:
        url = entry.getElementsByTagName('id')[0].childNodes[0].data
        doi_elem = entry.getElementsByTagName('arxiv:doi')
        doi = None
        if doi_elem:
            doi = doi_elem[0].childNodes[0].data
        title = entry.getElementsByTagName('title')[0].childNodes[0].data.encode('ascii', 'ignore').replace('\n', ' ')
        abstract = entry.getElementsByTagName('summary')[0].childNodes[0].data.encode('ascii', 'ignore').replace('\n', ' ').lstrip()
        published_date = entry.getElementsByTagName('published')[0].childNodes[0].data
        updated_date = entry.getElementsByTagName('updated')[0].childNodes[0].data
        articles.append(
            {
                'doi': doi,
                'title': title,
                'abstract': abstract,
                'published': published_date,
                'updated': updated_date,
                'url': url
            }
        )
    return articles

def crawl_all_categories():
    """Crawl all entries from all categories"""
    print 'start crawling all categories'
    #TODO
    # for cat in SUBJECT_CLASSIFICATION.keys():
    for cat in SUBJECT_CLASSIFICATION.keys()[:5]:
        crawl_by_category(cat)
    print 'end process!!'

def crawl_by_category(cat):
    if cat not in SUBJECT_CLASSIFICATION:
        raise Exception("Invalid category")
    cat_name = SUBJECT_CLASSIFICATION[cat]
    print 'crawling category: {}'.format(cat_name)
    data = fetch_arxiv_data(cat, 0, 1)
    xml_data = minidom.parseString(data)
    # TODO
    # total_results = int(xml_data.getElementsByTagName('opensearch:totalResults')[0].childNodes[0].data)
    total_results = 5
    print 'starting to fetch {} entries'.format(total_results)
    if total_results > 0:
        offset = -10
        limit = 10
        while offset < total_results:
            offset += limit
            data = fetch_arxiv_data(cat, offset, limit)
            save_arxiv_data_to_db(data)
            #filename = "arxiv_{}_{}_{}.xml".format(cat, offset, limit)
            #saveToFile(data, filename)
    print 'end crawling {}!!'.format(cat_name)


def main():
    create_db()
    clean_raw_data()
    crawl_all_categories()
    select_top_ten_raw_data()

if __name__ == '__main__':
    main()