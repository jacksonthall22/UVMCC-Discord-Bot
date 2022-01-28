from ast import literal_eval as make_tuple
from collections import defaultdict
import datetime
import random
import re
from os.path import exists, splitext
import sqlite3


DEFAULT_LOG_FILE = '_action_log.txt'
RANDOM_STR = "".join(random.sample("1234567890", 5))
DEFAULT_NEW_LOG_FILE = f'new_action_log_{RANDOM_STR}.db'
while exists(DEFAULT_NEW_LOG_FILE):
    DEFAULT_NEW_LOG_FILE = f'new_action_log_{RANDOM_STR}.db'


def log(s, filename):
    with open(filename, 'a') as f:
        f.write(f'[RECONSTRUCTED@{datetime.datetime.now()}] {s}'.strip() + '\n')

# TODO May want to paste most updated db_query() straight from bot.py
def db_query(db_name: str,
             query: str,
             params=None,
             do_log=True,
             log_filename=None,
             debug=None,
             verbose=None):
    """
    Connect with the given sqlite3 database and execute a query. Return a
    custom exit code and cur.fetchall() for the command.
    """

    # Open connection
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # Track success of query execution
    exit_code = 0
    query_result = None
    err = None

    # Try executing query with parameters
    try:
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)

        con.commit()
        query_result = cur.fetchall()
    except sqlite3.IntegrityError as e:
        # Integrity error can happen if inserting duplicate primary key
        err = e
        exit_code = 2
    except Exception as e:
        # Fall-through to default error code of 1
        err = e
        exit_code = 1
    finally:
        con.close()

    # Print error message and/or build log_str
    log_str = f'db_query(db_name={db_name},query={query},params={params}) called'
    if exit_code == 0:
        log_str += '\nQuery succeeded'
    else:
        log_str += f'\nQuery failed with exit code 1. Stack trace:\n{err}'

    if do_log:
        log(log_str, log_filename)

    return exit_code, query_result

def main():
    while True:
        log_file = input(f'Enter log filename (default {DEFAULT_LOG_FILE}):\n>>> ')
        if not log_file:
            log_file = DEFAULT_LOG_FILE
        if exists(log_file):
            break
        print(f'Log file "{log_file}" doesn\'t exist, please enter a new one.')

    while True:
        new_log_file = input(F'Enter new log filename (default {DEFAULT_NEW_LOG_FILE}):\n>>> ')
        if not new_log_file:
            new_log_file = DEFAULT_NEW_LOG_FILE
        if not exists(new_log_file):
            break
        print(f'Database file "{new_log_file}" already exists, please choose a new one.')

    print(f'Reading from {log_file}...', end='')
    with open(log_file, 'r') as f:
        file_text = f.read()
    print('done.')

    if not file_text:
        print(f'File was empty.\n'
              f'file_text: {file_text}')

    # Reconstruct all listed past queries
    # https://regex101.com/r/mXknml/1
    past_queries = re.findall('(.* db_query\(db_name=(.*),query=(.*),params=(.*)\) called\nQuery succeeded\n)', file_text)
    assert all(len(e) == 4 for e in past_queries)

    # Make a defaultdict where keys are db filenames to see how many database files were used in the log file.
    # An equal number will be reconstructed using all the successful queries from the log file.
    queries_by_db = defaultdict(list)
    for past_query in past_queries:
        assert isinstance(past_query, tuple)
        assert len(past_query) == 4

        for (full_string, db_file, query, params) in past_queries:
            queries_by_db[db_file].extend((full_string, query, params))

    print()
    print(f'There are queries into {len(queries_by_db)} database file(s) in the log file. New names will be '
          f'generated for their new reconstructions.')

    old_db_files = list(queries_by_db)
    new_db_files = {}
    for f in old_db_files:
        fn, ext = splitext(f)
        new_db_files[f] = f'new_{fn}_{RANDOM_STR}{ext}'

    print('Database files queried in old log:')
    for f in old_db_files:
        print(f'\t{f}')

    print('New database files that will be created:')
    for f in old_db_files:
        print(f'\t{new_db_files[f]}')

    resp = ''
    while resp.lower() not in ('y', 'yes'):
        resp = input('Continue? (y/n)\n>>> ')
        if resp.lower() in ('n', 'no'):
            print('Aborting.')
            exit()

    # Execute all commands into new db file
    print(f'Reconstructing database into new database files...')
    for full_string, db_name, query, params in past_queries:
        db_name = new_db_files[db_name]
        params = make_tuple(params)

        db_query(db_name,
                 query,
                 params,
                 do_log=True,
                 log_filename=new_log_file,
                 verbose=True)
    print(f'Done. New log file is "{new_log_file}"')

    # Test out the new db files
    # print('====================')
    # print(F'SELECT * FROM ChessUsernames ({new_db_files["users.db"]}):')
    # _, result = db_query(new_db_files['users.db'], 'SELECT * FROM ChessUsernames', params=None, do_log=False)
    # print(result)
    #
    # print('====================')
    # print(F'SELECT * FROM ChessUsernames ({new_db_files["users_new.db"]}):')
    # _, result = db_query(new_db_files['users_new.db'], 'SELECT * FROM ChessUsernames', params=None, do_log=False)
    # print(result)


if __name__ == '__main__':
    main()

