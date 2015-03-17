#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3

ARXIV_DB = 'arxiv_crawler.db'
ARXIV_RAW_DATA_TABLE = 'raw_data'


def create_db():
    with sqlite3.connect(ARXIV_DB) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS raw_data
                     (arxiv_id text PRIMARY KEY,
                     data text,
                     updated_date text,
                     created_date text)''')
        conn.commit()


def insert_raw_data_list(raw_data):
    """Save arxiv xml raw data into arxiv_crawler db"""
    with sqlite3.connect(ARXIV_DB) as conn:
        c = conn.cursor()
        c.executemany(('INSERT OR IGNORE INTO raw_data(arxiv_id, data,updated_date, created_date) \n'
                       '        values (?,?,?,?)'),
                      raw_data)
        conn.commit()


def select_raw_data_by_id(arxiv_id):
    """
    Return the arxiv data for the specified id
    :param arxiv_id: Id of arxiv
    :return: dictionary with the arxiv_id and data
    :rtype: dict
    """
    with sqlite3.connect(ARXIV_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('select arxiv_id, data from raw_data where arxiv_id=:Id limit 1', {"Id": arxiv_id})
        row = c.fetchone()
        return row


def select_top_ten_raw_data():
    with sqlite3.connect(ARXIV_DB) as conn:
        c = conn.cursor()
        for row in c.execute('select * from raw_data limit 10'):
            print row