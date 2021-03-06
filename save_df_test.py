#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Dataframe Chunk Writer
# Author: Ryan Stefan

import psycopg2
import json
import pandas as pd
import sqlite3

import os.path
import math
import time
import functools

import nltk
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize

from data_sumy import lex_sum


settings = {
    'max_comment': 2500,
    'max_title': 500
}


def lconnect():
    # initialize/create local database
    sqlite_file = './db/make_samples.sqlite'
    lconn = sqlite3.connect(sqlite_file)
    lcur = lconn.cursor()
    # lcur, lconn = lconnect()
    # lcur.execute("SELECT COUNT(*) FROM reviews")
    return lcur, lconn


def connect():
    with open('./config.json') as f:
        config = json.load(f)  # returns a dictionary

    conn = psycopg2.connect(**config)
    # similar to psycopg2.connect(host="localhost",database="db", user="postgres", password="postgres")
    cur = conn.cursor()
    # cur, conn = connect()
    # cur.execute("SELECT COUNT(*) FROM reviews")
    return cur, conn


def sentence_trimmer(text, max_len):
    sentences = sent_tokenize(text)
    count = 1
    if len(' '.join(sentences)) <= max_len:
        return sentences
    if len(text) > max_len and len(sentences[0]) <= max_len:
        while len(' '.join(sentences[:-count])) > max_len:
            count += 1
        return sentences[:-count]
    elif len(sentences) == 1 and len(' '.join(sentences)) <= max_len:
        return sentences
    else:
        return False


def length_filter(title, comment):
    title = sentence_trimmer(title, settings['max_title'])
    comment = sentence_trimmer(comment, settings['max_comment'])
    if title and comment:
        data = {}
        data['title'] = ' '.join(title)
        data['comment'] = ' '.join(comment)
        if len(data['title']) > 5 and len(data['comment']) > 30:
            return data


# Needs work (inactive)
def rare_word_filter(title, comment):
    rand_text = nltk.word_tokenize(
        "They I'm peres ewofij ryan refuse to,  permit us to obtain the refuse permit!")
    english_vocab = set(w.lower() for w in nltk.corpus.words.words())
    punc = ['.', ',', '?', '!', '\'', '"', ':', ';', ]
    english_vocab |= set(punc)

    return [x if x.lower() in english_vocab else '<UNK>' for x in rand_text]


def count_products():
    cur, conn = connect()
    counts = {}
    cur.execute("SELECT COUNT(*) FROM products")
    limit = cur.fetchone()
    conn.close()
    return int(limit[0])


def count_reviews():
    cur, conn = connect()
    cur.execute("SELECT COUNT(*) FROM reviews")
    limit = cur.fetchone()
    conn.close()
    return int(limit[0])


def get_asins(limit=0):
    if not limit:
        limit = count_products()
    cur, conn = connect()
    cur.execute("SELECT asin FROM products LIMIT {limit}".
                format(limit=limit))
    rows = cur.fetchall()
    asins = set()
    for row in rows:
        asins.add(row[0])
    conn.close()
    return list(asins)


def sum_reviews(file_name, limit=1000, get_all=True, is_reset=False):
    '''
    instead of offset count which asins have been completed. 
    Don't save summaries to dataframe until all of the ratings have been summarized
    '''
    deftitles = ['One Star', 'Two Stars',
                 'Three Stars', 'Four Stars', 'Five Stars']
    avg_sec = 0
    times = []
    start = time.time()

    if is_reset:
        reset()
    count = int(get_count())

    # if limit is 0 get all from DB
    if not limit:
        # count products instead of reviews
        limit = count_products()
        limit_left = limit - count
        gen_message = 'Generating {} of {}'.\
            format(str(limit_left), str(limit))
    else:
        limit_left = limit - count
        gen_message = 'Generating {} of {}'.\
            format(str(limit_left), str(limit))

    # Print current settings once
    print('Total Remaining: ', str(limit_left))
    print(gen_message)

    asin_list = get_asins(limit)
    cur, conn = connect()
    for current_asin in asin_list:

        cur.execute("SELECT review_title, review_body, asin, review_rating FROM reviews WHERE asin='{}'".
                    format(current_asin))
        rows = cur.fetchall()

        data = {}
        sumtitle = []
        sumtext = []
        sumasin = []
        sumrate = []

        # return dict of a single title & comment sentence
        for rate in range(1, 6):
            asin = None
            titles = []
            comments = []
            for row in rows:
                if not asin:
                    asin = row[2]
                if row[3] == rate:
                    comments.append(row[1])
                    if row[0] not in deftitles:
                        titles.append(row[0])

            if not comments:
                print('None: ', rate, asin)
            elif len(titles) > 3:
                sum_count = min(math.floor(len(rows) / 10), 7)
            else:
                sum_count = 3

            if titles and comments:
                # limit review quantity
                titlejoin = ' '.join(
                    lex_sum(' '.join(titles[:min(len(titles), 50)]), sum_count))
                textjoin = ' '.join(
                    lex_sum(' '.join(comments[:min(len(titles), 50)]), sum_count))

                # Check single rating lengths
                if len(titlejoin) > 10 and len(textjoin) > 20:
                    sumtitle.append(titlejoin)
                    sumtext.append(textjoin)
                    sumasin.append(asin)
                    sumrate.append(rate)
                else:
                    print('Skipping: {} Rating {}'.format(current_asin, rate))

                # Print current batch info
                print('Adding product: {asin} Current: {count} summary to {file_name}.csv'.
                      format(file_name=file_name, count=count, asin=current_asin))

            else:
                print('Skipping: {} Rating {}'.format(current_asin, rate))

        print('Total Product Sums: ', len(sumtitle))

        # Display time remaining
        if avg_sec:
            seconds_left = ((limit - count) / 10) * avg_sec
            m, s = divmod(seconds_left, 60)
            h, m = divmod(m, 60)
            print('Estimated Time Left: {}h {}m {}s'.format(
                round(h), round(m), round(s)))

        data['title'] = sumtitle
        data['text'] = sumtext
        data['asin'] = sumasin
        data['rating'] = sumrate

        # Save file, increase count, save count/offset to local DB
        save_df(file_name, data, count)
        count += 1
        # Zero is interpreted as false
        if(not count % 10):
            end = time.time()
            time_block = end - start
            start = end
            times.append(time_block)
            avg_sec = functools.reduce(
                lambda x, y: x + y, times[-min(len(times), 500):]) / len(times[-min(len(times), 500):])
            print('Average time per 10:', round(avg_sec, 2), 'seconds')

    if conn is not None:
        conn.close()


def get_data(file_name, limit=1000, batch=5000, get_all=True, is_reset=False):
    if is_reset:
        reset()
    que_offset = int(get_offset())
    count = int(get_count())
    try:
        # if limit is 0 get all from DB
        if not limit:
            limit = count_reviews()
            limit_left = limit - (batch * 2 * count)
            gen_message = 'Generating {} rows'.\
                format(str(limit_left + batch))
        else:
            limit_left = limit - (batch * count)
            gen_message = 'Generating {} rows'.\
                format(str(limit_left + batch))

        # Print current settings once
        print('-' * 15)
        print('Total Remaining: ', str(limit_left + batch))
        print('Batch limit: ', batch)
        print('Starting Batch: ', count)
        print('-' * 15)
        print(' ')
        print(gen_message)
        print('queoffset = {}'.format(que_offset))
        cur, conn = connect()
        while que_offset < limit:
            try:
                cur.execute(
                    "SELECT review_title, review_body, asin, review_rating FROM reviews LIMIT {} OFFSET {}".
                    format(batch, que_offset))
                rows = cur.fetchall()

                titles = []
                comments = []
                asins = []
                ratings = []

                data = {}

            except Exception as ex:
                print('DB Error ', ex)
                pass

            for row in rows:
                # return dict of a single title & comment sentence
                try:
                    filtered = length_filter(row[0], row[1])
                    if filtered['title'] and filtered['comment']:
                        titles.append(filtered['title'])
                        comments.append(filtered['comment'])

                        asins.append(row[2])
                        ratings.append(row[3])

                except Exception as ex:
                    # print('Length filter / appending Error: ', ex)
                    pass

            try:
                # Add lists to data dict
                data['title'] = titles
                data['text'] = comments
                data['asin'] = asins
                data['rating'] = ratings

            except Exception as ex:
                print('Adding lists to dict Error: ', ex)
                pass

            # Save file, count,
            try:
                # Print current batch info
                print('Adding {batch} lines to {file_name}.csv - Batch #{count}'.
                      format(batch=batch, file_name=file_name, count=count))

                # Save file, increase count, save count/offset to local DB
                save_df(file_name, data, count)
                count += 1
                data = {}
                que_offset += batch
                set_count(count)
                set_offset(que_offset)

            except Exception as error:
                print('Saving and setting error: ', error)

    except (Exception, psycopg2.DatabaseError) as error:
        print('get_data() postgres error: ', error)
        pass

    finally:
        if conn is not None:
            conn.close()


def save_df(file_name, data, count):
    df = pd.DataFrame(data, columns=['title', 'text', 'asin', 'rating'])
    if int(count) == 1:
        df.to_csv('./generated/{}.csv'.format(file_name))
    else:
        with open('./generated/{}.csv'.format(file_name), 'a') as f:
            df.to_csv(f, header=False)


def get_offset():
    lcur, lconn = lconnect()
    lcur.execute(
        "SELECT setting_value FROM sample_gen_settings WHERE setting_name='offset'")
    offset_value = lcur.fetchone()
    lconn.close()
    return int(offset_value[0])


def set_offset(offset_value):
    lcur, lconn = lconnect()
    lcur.execute("UPDATE sample_gen_settings SET setting_value={offset_value} WHERE setting_name='offset'".
                 format(offset_value=offset_value))
    lconn.close()


def get_count():
    lcur, lconn = lconnect()
    lcur.execute(
        "SELECT setting_value FROM sample_gen_settings WHERE setting_name='count'")
    count = lcur.fetchone()
    lconn.close()
    return int(count[0])


def set_count(count):
    lcur, lconn = lconnect()
    lcur.execute("UPDATE sample_gen_settings SET setting_value={count} WHERE setting_name='count'".
                 format(count=count))
    lconn.close()


def reset():
    import os
    import shutil
    folder = './sumdata/train/split'
    set_count(1)
    set_offset(0)
    print('Database and files cleared.')


if __name__ == "__main__":
    # get_data(file_name='quick-test-2', limit=0, batch=100, is_reset=True) # limit 0 gets all
    #print(lex_sum('this is a test. this is another test. How many tests do I need? I dont know, just keep testing', 2))
    sum_reviews(file_name='sum-timer-test', limit=3000, is_reset=True)
    # db_test(10)
