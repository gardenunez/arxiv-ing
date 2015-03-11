#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
from xml.dom import minidom
import sys
from arxiv_parser import parse_arxiv_entry

def digest_arxiv_entry(source):
    """
    Parse an arxiv xml string
    :param source: str
    :return: list of articles
    """
    xml_data = minidom.parseString(source.encode('utf-8'))
    entries = xml_data.getElementsByTagName('entry')
    if entries:
        article = parse_arxiv_entry(entries[0])
        return article


def select_top_ten_raw_data():
    rows = []
    with sqlite3.connect('arxiv_crawler.db') as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('select arxiv_id, data from raw_data limit 10')
        rows = c.fetchall()
    return rows


def select_raw_data_by_id(arxiv_id):
    """
    Return the arxiv data for the specified id
    :param arxiv_id: Id of arxiv
    :return: dictionary with the arxiv_id and data
    :rtype: dict
    """
    with sqlite3.connect('arxiv_crawler.db') as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('select arxiv_id, data from raw_data where arxiv_id=:Id limit 1', {"Id": arxiv_id})
        row = c.fetchone()
        return row


def main():
    #TODO: add argument parser
    row = select_raw_data_by_id(sys.argv[1])
    result = digest_arxiv_entry(row['data'])
    print result

if __name__ == "__main__":
    main()