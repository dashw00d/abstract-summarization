import psycopg2
import json
import pickle


with open('./config.json') as f:
    config = json.load(f)  # returns a dictionary


def pickle_reviews():
    try:
        conn = psycopg2.connect(**config)
        # similar to psycopg2.connect(host="localhost",database="db", user="postgres", password="postgres")
        cur = conn.cursor()

        cur.execute("SELECT review_title, review_body FROM reviews WHERE length(review_title) > 30")
        rows = cur.fetchmany(2000000)
        print("The number of parts: ", cur.rowcount)

        titles = []
        reviews = []
        keywords = ['review', 'amazon']
        for row in rows:
            #f.write('{}\n'.format(row[0]))
            titles.append(row[0])
            reviews.append(row[1])

        cur.close()
        save_obj((titles, reviews, keywords), 'reviews')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

def save_obj(obj, name):
    with open('data/' + name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, protocol=2)


def load_obj(name):
    with open('data/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)


if __name__ == "__main__":
    pickle_reviews()