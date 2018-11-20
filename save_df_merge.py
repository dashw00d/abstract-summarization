#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Dataframe Chunk Writer
# Author: Ryan Stefan

import psycopg2
import json
import pandas as pd
import sqlite3
import redis

import os.path
import math
import time
import functools
import datetime

import nltk
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize
from addons.nltk_rake import Metric, Rake

from data_sumy import lex_sum


settings = {
    'max_comment': 2500,
    'max_title': 500
}

redis_host = '127.0.0.1'  # call back url
redis_port = 6379
redis_db = 0

# Open Redis connection
print("Opening Redis connection")
redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)


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


def word_pool(text):
    pass


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


def add_asins(limit=0):
    if not limit:
        limit = count_products()
    cur, conn = connect()
    cur.execute("SELECT asin FROM products LIMIT {limit}".
                format(limit=limit))
    rows = cur.fetchall()
    for row in rows:
        redis_conn.sadd('sum_ASINs', row[0])
    conn.close()


def sum_reviews(file_name, limit=1000, get_all=True, is_reset=False):
    '''
    instead of offset count which asins have been completed. 
    Don't save summaries to dataframe until all of the ratings have been summarized
    '''
    deftitles = ['one star', 'two stars',
                 'three stars', 'four stars', 'five stars']
    avg_sec = 0
    times = []
    start = time.time()

    if is_reset:
        reset()

    count = int(get_count())
    print('Count:', count)

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

    if not redis_conn.scard('sum_ASINs'):
        add_asins(limit)

    # Print current settings once
    print('Total Remaining: ', str(limit_left))
    print(gen_message)

    cur, conn = connect()
    while redis_conn.scard('sum_ASINs') > 0:
        current_asin = redis_conn.spop('sum_ASINs').decode("utf-8")

        cur.execute("SELECT review_title, review_body, asin, review_rating, review_date, review_helpful, verified FROM reviews WHERE asin='{}'".
                    format(current_asin))
        rows = cur.fetchall()

        # Data functions, append all dates (product meta) once for seperate meta df
        review_meta(file_name, rows, count)

        # Temp Storage
        data = {}
        sumtitle = []
        sumtext = []
        sumasin = []
        sumrate = []
        sumkeywords = []

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
                    if row[0].lower() not in deftitles:
                        titles.append(row[0])

            if not comments:
                print('None: ', rate, asin)
            elif len(comments) > 3:
                sum_count = min(math.floor(len(rows) / 10), 7)
            else:
                sum_count = 3

            if titles and comments:
                comments.extend(titles)
                # limit review quantity
                titlejoin = ' '.join(
                    lex_sum(' '.join(titles[:min(len(titles), 75)]), 1))
                textjoin = ' '.join(
                    lex_sum(' '.join(comments[:min(len(comments), 75)]), sum_count))

                # Check single rating lengths & add to storage
                if len(titlejoin) > 10 and len(textjoin) > 20:
                    sumtitle.append(titlejoin)
                    sumtext.append(textjoin)
                    sumasin.append(asin)
                    sumrate.append(rate)

                    # Rake keywords
                    rake = Rake(min_length=2, max_length=6,
                                ranking_metric=Metric.DEGREE_TO_FREQUENCY_RATIO)
                    rake.extract_keywords_from_text(textjoin)
                    sumkeywords.append(' : '.join(rake.get_ranked_phrases()))
                else:
                    print('Skipping: {} Rating {}'.format(current_asin, rate))

                # Print current batch info
                print('Adding: {asin} | {count} of {limit} | {file_name}.csv'.
                      format(file_name=file_name, count=count, limit=limit, asin=current_asin))

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
        data['keywords'] = sumkeywords

        # Save file, increase count, save count/offset to local DB
        save_df(file_name, data, count)
        count += 1
        set_count(int(count))
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


def review_meta(file_name, rows, count):
    data = {}
    asin = None
    dates = []
    ratings = []
    date_dict = {}
    date_rev_avg = {}
    day_list = {
    "Monday": [],
    "Tuesday": [],
    "Wednesday": [],
    "Thursday": [],
    "Friday": [],
    "Saturday": [],
    "Sunday": []
    }
    days = []
    helpful = []
    verified = []
    total_verified = 0
    for row in rows:
        if not asin:
            asin = row[2]
        dates.append(row[4])
        ratings.append(row[3])
        helpful.append(row[5])
        verified.append(row[6])

    # Calculate percent verified
    for item in verified:
        if str(item) == 'True':
            total_verified += 1
    if verified:
        percent = round(total_verified / len(verified) * 100, 1)
    else:
        percent = 'No Data'
    print('Percent Verified', '{}%'.format(str(percent)))

    # count comments for dates / store ratings for dates in list
    for date, rating in zip(dates, ratings):
        #days.append(date_day(date))
        # build dict with date as key and count as value
        date_dict[date] = dates.count(date)
        if date in date_rev_avg:
            date_rev_avg[date].append(rating)
        else:
            date_rev_avg[date] = [rating]
    # daylist gen
    for date in set(dates):
        day_list[date_day(date)].append(date_dict[date])

    # Create lists for DF
    df_dates = []
    df_counts = []
    df_ratings = []
    df_mon = []
    df_tue = []
    df_wed = []
    df_thu = []
    df_fri = []
    df_sat = []
    df_sun = []
    df_percent = []
    df_help = []
    df_asin = []
    df_percent.append(str(percent))
    df_help.append(helpful)
    df_asin.append(asin)
    for key in date_dict:
        df_dates.append(str(key))
        df_counts.append(date_dict[key])
        df_ratings.append(date_rev_avg[key])

    df_mon.append(day_list['Monday'])
    df_tue.append(day_list['Tuesday'])
    df_wed.append(day_list['Wednesday'])
    df_thu.append(day_list['Thursday'])
    df_fri.append(day_list['Friday'])
    df_sat.append(day_list['Saturday'])
    df_sun.append(day_list['Sunday'])

    data['dates'] = df_dates
    data['counts'] = df_counts
    data['ratings'] = df_ratings
    data['mon'] = df_mon
    data['tue'] = df_tue
    data['wed'] = df_wed
    data['thu'] = df_thu
    data['fri'] = df_fri
    data['sat'] = df_sat
    data['sun'] = df_sun
    data['percent'] = df_percent
    data['helpful'] = df_help
    data['asin'] = df_asin

    #"Dates", "Counts", "Ratings", "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday", "Percent", "Helpful"
    #["dates", "counts", "ratings", "mon", "tue", "wed", "thu", "fri", "sat", "sun", "percent", "helpful", "asin"]
    #print(data)
    if asin:
        save_meta_df(file_name, data, count)


def date_day(in_date):
    days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dayNumber=in_date.weekday()
    return days[dayNumber]

def save_df(file_name, data, count):
    df = pd.DataFrame(
        data, columns=['title', 'text', 'asin', 'rating', 'keywords'])
    if int(count) == 1:
        df.to_csv('./generated/{}.csv'.format(file_name))
    else:
        with open('./generated/{}.csv'.format(file_name), 'a') as f:
            df.to_csv(f, header=False)


def save_meta_df(file_name, data, count):
    df = pd.DataFrame([data])
    if int(count) == 1:
        df.to_csv('./generated/{}-meta.csv'.format(file_name))
    else:
        with open('./generated/{}-meta.csv'.format(file_name), 'a') as f:
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
    return redis_conn.spop('sum_count')


def set_count(count):
    redis_conn.delete('sum_count')
    redis_conn.sadd('sum_count', count)


def reset():
    import os
    import shutil
    folder = './sumdata/train/split'
    set_count(1)
    set_offset(0)
    redis_conn.delete('sum_ASINs')
    print('Database and files cleared.')


if __name__ == "__main__":
    # get_data(file_name='quick-test-2', limit=0, batch=100, is_reset=True) # limit 0 gets all
    #print(lex_sum('this is a test. this is another test. How many tests do I need? I dont know, just keep testing', 2))
    sum_reviews(file_name='sum-stop-test', limit=500, is_reset=True)
    # db_test(10)
    # set_count(9)
    # print(int(get_count()))
