#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3
from arxiv_subject_classification import SubjectClassification

ARXIV_DB = 'arxiv_crawler.db'
ARXIV_RAW_DATA_TABLE = 'raw_data'
SUBJECT_CLASSIFICATION_TABLE = 'subject_classification'


def create_db():
    """
    Create arxiv crawler db schema
    :return:
    """
    with sqlite3.connect(ARXIV_DB) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS raw_data
                     (arxiv_id text PRIMARY KEY,
                     data text,
                     updated_date text,
                     created_date text);
                     CREATE TABLE `subject_classification` (
                     `name`	TEXT,
                     `desc`	TEXT,
                     PRIMARY KEY(name)
                )''')
        conn.commit()


def insert_raw_data_list(raw_data):
    """Save arxiv xml raw data into arxiv_crawler db"""
    with sqlite3.connect(ARXIV_DB) as conn:
        c = conn.cursor()
        c.executemany(('INSERT OR IGNORE INTO {tn}(arxiv_id, data,updated_date, created_date) \n'
                       '        values (?,?,?,?)').format(tn=ARXIV_RAW_DATA_TABLE),
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
        c.execute('select arxiv_id, data from {tn} where arxiv_id=:Id limit 1'.\
                  format(tn=ARXIV_RAW_DATA_TABLE),
                  {"Id": arxiv_id})
        row = c.fetchone()
        return row


def get_all_subject_classifications():
    """
    Get all subject classifications
    :return:List of SubjectClassification
    """
    with sqlite3.connect(ARXIV_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('select name, desc from {tn}'.format(tn=SUBJECT_CLASSIFICATION_TABLE))
        rows = c.fetchall()
        result = []
        for row in rows:
            sc = SubjectClassification(row["name"], row["desc"])
            result.append(sc)
        return result

def get_subject_classification_by_name(name):
    """
    Get subject classification by name
    :param name: name of the category
    :return: Arxiv category
    :rtype: SubjectClassification
    """
    with sqlite3.connect(ARXIV_DB) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('select name, desc from {tn} where name=:Name'.\
                  format(tn=SUBJECT_CLASSIFICATION_TABLE), {"Name":name})
        row = c.fetchone()
        if row:
            sc = SubjectClassification(row["name"], row["desc"])
            return sc