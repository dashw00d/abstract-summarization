import sqlite3

sqlite_file = 'make_samples.sqlite'    # name of the sqlite database file
sample_gen_settings = 'sample_gen_settings'  # name of the table to be created
setting_name = 'setting_name' # name of the column
column_type = 'TEXT'
setting_value = 'setting_value'
field_type = 'VARCHAR'  # column data type


def create_database():
    # Connecting to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()


    try:
        c.execute('''
            CREATE TABLE sample_gen_settings (
             setting_name TEXT NOT NULL UNIQUE,
             setting_value TEXT NOT NULL
    );
    ''')

    except Exception as ex:
        print('ERROR: Failed to add {} with error {}'.format(sample_settings, ex))

    # B) Tries to insert an ID (if it does not exist yet)
    # with a specific value in a second column
    c.execute("INSERT OR IGNORE INTO {tn} ({idf}, {cn}) VALUES ('offset', 0)".\
            format(tn=sample_gen_settings, idf=setting_name, cn=setting_value))

    c.execute("INSERT OR IGNORE INTO {tn} ({idf}, {cn}) VALUES ('count', 1)".\
            format(tn=sample_gen_settings, idf=setting_name, cn=setting_value))



    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()

if __name__=='__main__':
    create_database()