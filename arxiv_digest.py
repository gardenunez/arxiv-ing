#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
from xml.dom import minidom


def parse_arxiv_entry(entry):
    """
    Parse a single arxiv entry xml node into dictionary fields
    :param entry: arxiv node for an article entry
    :return: dictionary with publication data
    """
    url = entry.getElementsByTagName('id')[0].childNodes[0].data
    doi_elem = entry.getElementsByTagName('arxiv:doi')
    doi = None
    if doi_elem:
        doi = doi_elem[0].childNodes[0].data
    title = entry.getElementsByTagName('title')[0].childNodes[0].data.encode('ascii', 'ignore')\
        .replace('\n', ' ')
    abstract = entry.getElementsByTagName('summary')[0].childNodes[0].data.encode('ascii', 'ignore')\
        .replace('\n', ' ').lstrip()
    published_date = entry.getElementsByTagName('published')[0].childNodes[0].data
    updated_date = entry.getElementsByTagName('updated')[0].childNodes[0].data

    authors = []

    for author in entry.getElementsByTagName('author'):
        affiliation = author.getElementsByTagName('arxiv:affiliation')
        authors.append(
            {
                "name": author.getElementsByTagName('name')[0].childNodes[0].data,
                "affiliation": affiliation
                                and affiliation[0].childNodes[0].data
                                or ""
            }
        )
    journal_ref = entry.getElementsByTagName('arxiv:journal_ref')
    journal = ""
    if journal_ref:
        journal = journal_ref[0].childNodes[0].data

    full_text = ""
    links = entry.getElementsByTagName('links') or []
    for link in links:
        if link.hasAttribute('title') and link.getAttribute('title') == 'pdf':
            full_text = link.getAttribute('href')
            break

    return {
        'doi': doi,
        'title': title,
        'abstract': abstract,
        'authors': authors,
        'published': published_date,
        'updated': updated_date,
        'url': url,
        'journal': journal,
        'full_text':full_text
    }

def digest_arxiv_entry(source):
    """
    Parse an arxiv xml string
    :param source: str
    :return: list of articles
    """
    xml_data = minidom.parseString(source.encode('utf-8'))
    articles = []
    entries = xml_data.getElementsByTagName('entry')
    for entry in entries:
        article = parse_arxiv_entry(entry)
        articles.append(article)
    return articles


def select_top_ten_raw_data():
    rows = []
    with sqlite3.connect('arxiv_crawler.db') as conn:
        c = conn.cursor()
        c.execute('select * from raw_data limit 100')
        rows = c.fetchall()
    return rows


def select_raw_data_by_id(arxiv_id):
    conn = sqlite3.connect('arxiv_crawler.db')
    try:
        c = conn.cursor()
        c.execute('select arxiv_id, data from raw_data limit 1')
        row = c.fetchone()
        return row
    except Exception:
        raise
    finally:
        conn.close()


def main():
    rows = select_top_ten_raw_data()
    for row in rows:
        result = digest_arxiv_entry(row[1])
        print result


main()