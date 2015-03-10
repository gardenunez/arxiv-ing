#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
from xml.dom import minidom
import sys


def parse_arxiv_entry(entry):
    """
    Parse a single arxiv entry xml node into dictionary fields
    :param entry: arxiv node for an article entry
    :return: dictionary with publication data
    """
    url = entry.getElementsByTagName('id')[0].childNodes[0].data
    tokens = url.split('/')
    arxiv_id = tokens[len(tokens) - 1]
    doi_elem = entry.getElementsByTagName('arxiv:doi')
    doi = ""
    if doi_elem:
        doi = doi_elem[0].childNodes[0].data
    title = entry.getElementsByTagName('title')[0].childNodes[0].data.replace('\n', ' ')
    abstract = entry.getElementsByTagName('summary')[0].childNodes[0].data\
        .replace('\n', ' ').lstrip()
    published_date = entry.getElementsByTagName('published')[0].childNodes[0].data
    updated_date = entry.getElementsByTagName('updated')[0].childNodes[0].data

    authors = []

    for author in entry.getElementsByTagName('author'):
        affiliation = author.getElementsByTagName('arxiv:affiliation')
        authors.append(
            {
                "name": author.getElementsByTagName('name')[0].childNodes[0].data,
                "affiliation": affiliation and affiliation[0].childNodes[0].data or ""
            }
        )
    journal_ref = entry.getElementsByTagName('arxiv:journal_ref')
    journal = ""
    if journal_ref:
        journal = journal_ref[0].childNodes[0].data

    full_text = ""
    links = entry.getElementsByTagName('link') or []
    for link in links:
        if link.hasAttribute('title') and link.getAttribute('title') == 'pdf':
            full_text = link.getAttribute('href')
            break

    return {
        'arxiv_id': arxiv_id,
        'doi': doi,
        'title': title,
        'abstract': abstract,
        'authors': authors,
        'published': published_date,
        'updated': updated_date,
        'url': url,
        'journal': journal,
        'full_text': full_text
    }


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