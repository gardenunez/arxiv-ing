#!/usr/bin/env python
# -*- coding: utf-8 -*-


def get_doi(entry):
    doi = ""
    doi_elem = entry.getElementsByTagName('arxiv:doi')
    if doi_elem:
        doi = doi_elem[0].childNodes[0].data
    return doi


def get_arxiv_id(entry):
    url = entry.getElementsByTagName('id')[0].childNodes[0].data
    tokens = url.split('/')
    arxiv_id = tokens[len(tokens) - 1]
    return arxiv_id


def get_authors(entry):
    authors = []

    for author in entry.getElementsByTagName('author'):
        affiliation = author.getElementsByTagName('arxiv:affiliation')
        authors.append(
            {
                "name": author.getElementsByTagName('name')[0].childNodes[0].data,
                "affiliation": affiliation and affiliation[0].childNodes[0].data or ""
            }
        )
    return authors


def get_full_text_url(entry):
    full_text = ""
    links = entry.getElementsByTagName('link') or []
    for link in links:
        if link.hasAttribute('title') and link.getAttribute('title') == 'pdf':
            full_text = link.getAttribute('href')
            break
    return full_text


def get_journal(entry):
    journal = ""
    journal_ref = entry.getElementsByTagName('arxiv:journal_ref')
    if journal_ref:
        journal = journal_ref[0].childNodes[0].data
    return journal


def get_updated_date(entry):
    return entry.getElementsByTagName('updated')[0].childNodes[0].data


def parse_arxiv_entry(entry):
    """
    Parse a single arxiv entry xml node into dictionary fields
    :param entry: arxiv node for an article entry
    :return: dictionary with publication data
    """
    url = entry.getElementsByTagName('id')[0].childNodes[0].data
    arxiv_id = get_arxiv_id(entry)
    doi = get_doi(entry)
    title = entry.getElementsByTagName('title')[0].childNodes[0].data.replace('\n', ' ')
    abstract = entry.getElementsByTagName('summary')[0].childNodes[0].data\
        .replace('\n', ' ').lstrip()
    authors = get_authors(entry)
    journal = get_journal(entry)
    published_date = entry.getElementsByTagName('published')[0].childNodes[0].data
    updated_date = get_updated_date(entry)
    primary_category = entry.getElementsByTagName('arxiv:primary_category')[0].getAttribute('term')
    full_text = get_full_text_url(entry)

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
        'full_text': full_text,
        'primary_category':primary_category
    }