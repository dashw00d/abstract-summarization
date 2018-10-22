import psycopg2
import json
import pandas as pd
import os.path

import sqlite3

import nltk
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize


settings = {
'max_comment': 650,
'max_title': 275
}

# initialize/create local database
sqlite_file = './db/make_samples.sqlite'

lconn = sqlite3.connect(sqlite_file)
lcur = lconn.cursor()

with open('./config.json') as f:
    config = json.load(f)  # returns a dictionary

conn = psycopg2.connect(**config)
# similar to psycopg2.connect(host="localhost",database="db", user="postgres", password="postgres")
cur = conn.cursor()


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
        return data


def rare_word_filter(title, comment):
    rand_text = nltk.word_tokenize("They I'm peres ewofij ryan refuse to,  permit us to obtain the refuse permit!")
    english_vocab = set(w.lower() for w in nltk.corpus.words.words())
    punc = ['.',',','?','!','\'','"',':',';',]
    english_vocab |= set(punc)

    return [x if x.lower() in english_vocab else '<UNK>' for x in rand_text]


def get_data(file_name, amount=1000, batch=5000, get_all=True):
    que_offset = int(get_offset())
    count = int(get_count())
    try:
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
            cur.execute("SELECT review_title, review_body, asin, review_rating FROM reviews LIMIT %s OFFSET %s", (batch, que_offset))
            rows = cur.fetchall()

            titles = []
            comments = []
            asins = []
            ratings = []

            data = {}

            for row in rows:
                filtered = length_filter(row[0], row[1])
                if filtered:
                    titles.append(filtered['title'])
                    comments.append(filtered['comment'])

                    asins.append(row[2])
                    ratings.append(row[3])

            data['title'] = titles
            data['comment'] = comments
            data['asin'] = asins
            data['rating'] = ratings

            # Save file and set new offset/count
            save_df(file_name, data, count)
            count += 1
            data = {}
            que_offset += batch
            set_count(count)
            set_offset(que_offset)

            # Print current batch info
            print('{batch} lines added to review-titles-{count}.txt & review-comments-{count}.txt'.format(batch=batch, count=count))            

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


def save_df(file_name, data, count):
    if int(count) == 1:
        df = pd.DataFrame(data, columns = ['title', 'comment', 'asin', 'rating'])
        df.to_csv('{}.csv'.format(file_name))
    else:
        with open('{}.csv'.format(file_name), 'a') as f:
            df.to_csv(f, header=False)


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
    # Default: gather_reviews(name, amount=1000, batch=5000, get_all=True)
    gather_reviews(file_name='reviews', batch=50000, get_all=True)