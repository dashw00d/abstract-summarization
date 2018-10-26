import sqlite3
import psycopg2
import json

settings = {
'max_comment': 2500,
'max_title': 500
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