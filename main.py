import requests
import json
from typing import *
import sqlite3
import discord


DB_NAME = 'users.db'
VALID_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-'


def exec_db_cmd(con: sqlite3.Connection, cmd_str: str):

    ''' Database Setup '''

    cur = con.cursor()

    ''' Command Functions '''

    def help_() -> Tuple[int, str]:
        return 0, '\n'.join([f"\t{'/'.join(d['name'])}: {d['desc']}" for d in COMMANDS])

    def add_(name: str) -> Tuple[int, str]:
        for c in name:
            if c not in VALID_CHARS:
                return 1, f'\tError: Invalid characters in {name}.'

        # Try inserting - will fail if primary key not unique (user already exists)
        try:
            cur.execute('INSERT INTO tblUsers VALUES (?)', (name,))
            con.commit()
        except sqlite3.IntegrityError:
            return 1, f'\t{name} is already in the database.'

        return 0, f'\tAdded {name} to the database.'

    def show() -> Tuple[int, str]:
        cur.execute('SELECT * FROM tblUsers ORDER BY pmkUsername')
        users = cur.fetchall()

        if not users:
            return 0, f'\tThere are no users in the database.'

        return 0, '\tSaved users:' + ''.join(['\n\t\t'+ e[0] for e in users])

    def remove(name: str) -> Tuple[int, str]:
        for c in name:
            if c not in VALID_CHARS:
                return 1, f'\tError: Invalid characters in {name}.'

        # Select names, case insensitive (will always return 0 or 1 result)
        cur.execute('SELECT * FROM tblUsers WHERE pmkUsername=? COLLATE nocase', (name,))
        users = cur.fetchall()

        if not users:
            # Else return an error msg
            return 1, f'\t{name} wasn\'t in the database.'
        else:
            name = users[0][0]

        cur.execute('DELETE FROM tblUsers WHERE pmkUsername=?', (name,))
        con.commit()

        return 0, f'\tRemoved {name} from the database.'

    def exit_() -> Tuple[int, int]:
        # -1 for result (normally a str) is break condition in main()
        # Code still 0 because this is intended behavior
        return 0, -1

    COMMANDS = [
        {
            'name': ['add'],
            'desc': 'Add a user to the database (ex. `add Cubigami`).',
            'func': add_,
        },
        {
            'name': ['remove'],
            'desc': 'Remove a user from the database (ex. `remove Cubigami`).',
            'func': remove,
        },
        {
            'name': ['show'],
            'desc': 'Show current usernames in the database.',
            'func': show,
        },
        {
            'name': ['exit', 'done'],
            'desc': 'Done adding and removing names.',
            'func': exit_,
        },
        {
            'name': ['help'],
            'desc': 'Print this help message.',
            'func': help_,
        },
    ]

    tokens = cmd_str.strip().split()

    found = False
    for e in COMMANDS:
        if tokens[0] in e['name']:
            try:
                # Call the function in the dict. This function should validate
                # tokens[1:] and print its own error msg describing if they 
                # cannot be parsed
                code, result = e['func'](*tokens[1:])
            except TypeError as e_:
                return 0, f'\tError: Couldn\'t parse arguments for {tokens[0]}().'

            # Successful execution
            return code, result

    # Error when executing
    return 1, None


def main():
    welcome()

    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS tblUsers (pmkUsername TEXT PRIMARY KEY)')

    while True:
        usr_input = input('Please enter a command:\n>>> ')

        code, result = exec_db_cmd(con, usr_input)

        if code != 0:
            # Error occurred: print error message (specific or general)
            if result:
                print(result)
                continue

            print('\tError: Command not recognized. Type "help" for more info.')
        elif result == -1:
            # Exit loop condition
            break
        elif result:
            print(result)

    print()
    print('----')
    print()

    # Show all items in the database
    cur.execute('SELECT * FROM tblUsers ORDER BY pmkUsername')
    usernames = [e[0] for e in cur.fetchall()]
    invalid_usernames = [u.lower() for u in usernames]
    print(f'Users in the database: {", ".join(usernames)}')

    # Build request
    query = {
        'ids': ','.join(usernames)
    }
    response = requests.get("https://lichess.org/api/users/status", params=query)

    online_usernames = []
    offline_usernames = []

    for user in json.loads(response.text):
        if 'online' in user and user['online']:
            online_usernames.append(user['name'])
            invalid_usernames.remove(user['name'].lower())
        else:
            offline_usernames.append(user['name'])
            invalid_usernames.remove(user['name'].lower())

    print('Players online:', ', '.join(online_usernames))
    print('Players offline:', ', '.join(offline_usernames))
    print('Invalid usernames:', ', '.join(invalid_usernames))

def welcome():
    print('==================')
    print('Lichess API Tester')
    print('==================')
    print('by Jackson Hall')
    print('4/1/21')
    print()
    print('''This program uses a sqlite3 database to store Lichess.org usernames, allowing \
the user to add, remove, and view them, and then calls a Lichess API endpoint to see which \
players in the database are online. This is the start of a Discord bot for the UVM Chess Club \
server that will be able to post chat messages when server members go online on Lichess.''')
    print()
    print('----')
    print()


query = {
    'ids': 'Cubigami, GA-BlindfoldBot'
}



main()