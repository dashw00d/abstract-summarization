import psycopg2
import json
import os.path
import sqlite3

from db import create_db as createdb

import nltk
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize


# initialize/create local database
sqlite_file = './db/make_samples.sqlite'

lconn = sqlite3.connect(sqlite_file)
lcur = lconn.cursor()

if not os.path.exists(sqlite_file):
    createdb.create_database()

settings = {
'max_comment': 650,
'max_title': 275
}

with open('./config.json') as f:
    config = json.load(f)  # returns a dictionary


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
        data['title'] = word_tokenize(' '.join(title))
        data['comment'] = word_tokenize(' '.join(comment))
        return data


def rare_word_filter(title, comment):
    rand_text = nltk.word_tokenize("They I'm peres ewofij ryan refuse to,  permit us to obtain the refuse permit!")
    english_vocab = set(w.lower() for w in nltk.corpus.words.words())
    punc = ['.',',','?','!','\'','"',':',';',]
    english_vocab |= set(punc)

    return [x if x.lower() in english_vocab else '<UNK>' for x in rand_text]


def gather_reviews(file_name, amount=1000, batch=5000, get_all=False):
    que_offset = int(get_offset())
    count = int(get_count())
    try:
        conn = psycopg2.connect(**config)
        # similar to psycopg2.connect(host="localhost",database="db", user="postgres", password="postgres")
        cur = conn.cursor()

        # Set amount to DB row length
        if get_all:
            cur.execute("""
                SELECT COUNT(*) FROM reviews;
                """)
            amount = cur.fetchone()
            amount = int(amount[0]) - (batch * count)
            gen_message = 'Generating {} rows'.format(str(amount))
        else:
            gen_message = 'Generating {} rows'.format(str(amount))

        # Print current settings once
        print('-' * 15)
        print('Total: ', amount)
        print('Batch Amount: ', batch)
        print('Starting Batch: ', count)        
        print('-' * 15)
        print(' ')
        print(gen_message)
        print('queoffset = {}'.format(que_offset))

        while que_offset <= amount:
            cur.execute("SELECT review_title, review_body FROM reviews WHERE length(review_title) > 30 LIMIT %s OFFSET %s", (batch, que_offset))
            rows = cur.fetchall()
            data = []
            for row in rows:
                filtered = length_filter(row[0], row[1])
                if filtered:
                    data.append((filtered['title'], filtered['comment']))
            count += 1

            # Save file and set new offset/count
            save_text(file_name, data, count)
            data = []
            que_offset += batch
            set_count(count)
            set_offset(que_offset)

            # Print current batch info
            print('{batch} lines added to review-titles-{count}.txt & review-comments-{count}.txt'.format(batch=batch, count=count))
                
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


def save_text(name, data, count):
    with open('./sumdata/train/split/{name}-titles-{count}.txt'.format(name=name, count=str(count)), 'w') as t, open('./sumdata/train/split/{name}-comments-{count}.txt'.format(name=name, count=str(count)), 'w') as r:
        for title, review in data:
            t.write(' '.join(title) + '\n')
            r.write(' '.join(review) + '\n')


def get_offset():
    with lconn:
        lcur.execute("SELECT setting_value FROM sample_gen_settings WHERE setting_name='offset'")
        offset_value = lcur.fetchone()
        return offset_value[0]


def set_offset(offset_value):
    with lconn:
        lcur.execute("UPDATE sample_gen_settings SET setting_value={offset_value} WHERE setting_name='offset'".\
            format(offset_value=offset_value))


def get_count():
    with lconn:
        lcur.execute("SELECT setting_value FROM sample_gen_settings WHERE setting_name='count'")
        count = lcur.fetchone()
        return count[0]


def set_count(count):
    with lconn:
        lcur.execute("UPDATE sample_gen_settings SET setting_value={count} WHERE setting_name='count'".\
            format(count=count))


def reset():
    import os, shutil
    folder = './sumdata/train/split'
    set_count(0)
    set_offset(0)
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)
    print('Database and files cleared.')


if __name__ == "__main__":
    #reset()
    # run with defaults
    # gather_reviews(name, amount=1000, batch=5000, get_all=False)
    gather_reviews(file_name='reviews', batch=50000, get_all=True)
