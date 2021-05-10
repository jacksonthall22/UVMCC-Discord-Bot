from typing import *
import os
import io
import re
import requests
import json
import datetime
# from collections import defaultdict
import sqlite3
import discord
from discord.ext import commands
import chess.pgn
from keep_alive import keep_alive

bot = commands.Bot(command_prefix='/')

LINK_TO_CODE = 'https://replit.com/@jackson_hall/UVMCC-Discord-Bot'
USERS_DB_NAME = 'users.db'
LICHESS_TABLE_NAME = 'tblUsers'
CHESSCOM_TABLE_NAME = 'tblUsersChesscom'
LOG_FILENAME = '_action_log.txt'
EMBED_FOOTER = '♟  v1.0  ♟  I\'m a bot, beep boop  ♟  Code  ♟'

DEBUG = True
VERBOSE = False  # Shows more debug info, ex. for successful DB queries
LOG = True

''' ========== Extra Functions ========== '''

def test(*args):
    new_args = []
    for a in args:
        if isinstance(a, str):
            new_args.append(a.rstrip(':') + ':')
        else:
            new_args.append(a)
    return 'test: ' + ' '.join([str(a) for a in new_args]).rstrip(':')

def ptest(*args):

    print(test(*args))

async def mtest(ctx, *args):
    await ctx.channel.send(test(*args))

def log(s, filename = LOG_FILENAME):
    with open(filename, 'a') as f:
        f.write(f'{str(datetime.datetime.now())} {s}'.strip() + '\n')

def db_query(db_name: str, query: str, params: Tuple = None, debug=DEBUG, verbose=VERBOSE):
    """ Connect with the given sqlite3 database and execute a query. Return an exit code and cur.fetchall() for the command. """
    
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
        query_result = None
    except Exception as e:
        # Fall-through to default error code of 1
        err = e
        exit_code = 1
        query_result = None
    finally:
        con.close()
    
    # Print error message and/or build log_str
    log_str = f'db_query(db_name={db_name},query={query},params={params}) called'
    if exit_code == 0:
        if DEBUG and VERBOSE:
            print(f'Query `{query}` in database `{db_name}` {("with params " + str(params) + " ", "")[params is None]}succeeded')
        log_str += '\nQuery succeeded'
    else:
        if DEBUG:
            print(f'Error: couldn\'t  execute query `{query}` in database {db_name}')
            print('Stack trace:\n', err)
        log_str += f'\nQuery failed with exit code 1. Stack trace:\n{err}'
    
    if LOG:
        log(log_str)

    return exit_code, query_result

def pgn_to_dict(pgn_str: str, site: str = 'lichess', get_fen=False):
    """
        NOTE: This shouldn't be necessary given the `chess.pgn` module.

        Take a PGN-formatted string and return a dict d that looks like this:
        d = {
            'tags': {
                'White': 'Cubigami',
                'Black': 'OtherPlayer',
                etc...
            },
            'moves': ['e4', 'e5', 'Ke2'],
            # If get_fen
            'fen': [rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPPKPPP/RNBQ1BNR b kq - 1 2]
        }
    """
    # PGN output might be different for chess.com - TODO
    if site == 'lichess':
        pass
    elif site == 'chesscom':
        pass
    else:
        raise ValueError(f'Error: Invalid `site` "{site}" in pgn_to_dict()')
    
    pgn_tags = {k: v for k, v in re.findall('\[(.*?) "(.*?)"\]', pgn_str)}
    # Take out tags, annotations, and result or * at end of moves list
    moves_str = re.sub('\{.*?\} |\[(.*?) "(.*?)"\]\n*|1-0|0-1|1\/2-1\/2|\d\.\.\. |\*', '', pgn_str).strip()

    moves = re.findall('\d+\. (.*?) (.*?) |\d+\. (.*)', moves_str)
    
    pgn_tags.update({'moves': moves})
    return pgn_tags


''' ========== Commands ========== '''

@bot.event
async def on_ready():
    logged_in = f'Logged in as {bot.user}'
    print(logged_in)
    if LOG:
        log(logged_in)
    db_query(USERS_DB_NAME, 'CREATE TABLE IF NOT EXISTS tblUsers (pmkUsername TEXT PRIMARY KEY)', )

@bot.command(brief='Says hello')
async def hello(ctx):
    await ctx.channel.send('Hello!')
    if VERBOSE:
        print('`hello()` run!')

@bot.command(brief='Add a username to names in /show')
async def add(ctx, *args):
    if len(args) == 1:
        args = [args[0], 'lichess']

    if len(args) != 2 or args[1].lower() not in ['lichess', 'chess.com']:
        await ctx.channel.send('Usage: `/add <username> <lichess / chess.com>`')
        return
    
    # Get given username and site the username applies to
    username, site = [a.lower() for a in args]
    
    # Handle differently depending on site
    if site == 'lichess':
        response = json.loads(requests.get("https://lichess.org/api/users/status", params={'ids': username}).text)

        if not response:
            # No users in response -> not a valid username
            await ctx.channel.send(f'`{username}` wasn\'t found on Lichess.')
            return
        
        # Response will give correct capitalization of username
        username_proper_caps = response[0]['name']

        # Execute insertion
        exit_code, response = db_query(USERS_DB_NAME, 'INSERT INTO tblUsers VALUES (?)', (username_proper_caps,))

        # Respond accordingly given nonzero exit code
        if exit_code == 2:
            await ctx.channel.send('That name is already in the Lichess database!')
            return
        elif exit_code != 0:
            await ctx.channel.send('There was an error inserting the username into the database. DM @Cubigami and it can be added manually.')
            return
        
        # Send success message given exit code 0
        await ctx.channel.send(f'Added `{username_proper_caps}` to the Lichess database. Use /show to see who\'s online!')
    elif site == 'chess.com':
        await ctx.channel.send('Chess.com is not currently supported, but it will be soon!')

@bot.command(brief='Remove a username from names in /show')
async def remove(ctx, *args):
    if len(args) == 2 and args[1].lower() not in ['lichess', 'chess.com']:
        await ctx.channel.send('Usage: `/add <username> <lichess / chess.com>`')
        return
    elif len(args) not in (1, 2):
        await ctx.channel.send('Usage: `/remove <username>[ <lichess / chess.com>]`')
        return
    
    # Get given username and site the username applies to, if given
    args = [a.lower() for a in args]

    # If site is None, remove username from both Lichess and Chess.com DBs
    site = None
    if len(args) == 2:
        username, site = args
    else:
        username = args[0]
    
    # Handle differently depending on site
    invalid_lichess_uname = False
    invalid_chesscom_uname = False
    not_in_lichess_db = False
    not_in_chesscom_db = False

    if site in ('lichess', None):
        response = json.loads(requests.get("https://lichess.org/api/users/status", params={'ids': username}).text)

        if not response:
            # No users in response -> not a valid username
            invalid_lichess_uname = True
        else:
            # Response will give correct capitalization of username
            username_proper_caps = response[0]['name']

            # Execute insertion
            exit_code, response = db_query(USERS_DB_NAME, 'DELETE FROM tblUsers WHERE LOWER(pmkUsername) LIKE LOWER(?)', (username_proper_caps,))

            # Respond accordingly given nonzero exit code
            if exit_code == 2:
                raise AssertionError('Didn\'t expect exit_code == 2 in remove()')
            elif exit_code != 0:
                await ctx.channel.send(f'There was an error removing {username_proper_caps} from the database. DM @Cubigami and it can be removed manually.')
                return
            
            # Send success message given exit code 0
            await ctx.channel.send(f'Removed `{username_proper_caps}` from the Lichess database.')
    
    if site in ('chess.com', None):
        invalid_chesscom_uname = True
        not_in_chesscom_db = True

    if invalid_lichess_uname and invalid_chesscom_uname:
        raise ValueError('error: placeholder')

@bot.command(brief='Shows Lichess player statuses (Chess.com coming soon)')
async def show(ctx):
    """ 
        Shows a formatted list of all users in the database and their 
        current Lichess/Chess.com status (playing/active/offline)
    """

    async with ctx.channel.typing():
        # Build embedded message
        e = discord.Embed(title='Lichess Player Statuses')
        e.set_author(name=bot.user.name,
                    url=LINK_TO_CODE,
                    icon_url=bot.user.avatar_url)
        e.set_footer(text=EMBED_FOOTER)

        # Get all users in the database
        exit_code, db_response = db_query(USERS_DB_NAME, 'SELECT * FROM tblUsers ORDER BY pmkUsername')
        usernames = [e[0] for e in list(db_response)]

        # Send request for all usernames - only valid usernames will be returned
        # (and valid ones will be returned with proper capitalization)
        response = requests.get("https://lichess.org/api/users/status", params={'ids': ','.join(usernames)})
        response_items = json.loads(response.text)

        # Don't build full embed if no users were returned
        if not response_items:
            e.add_field(name='No Players', value='There aren\'t any players in the Lichess database. Use /add to add your username!', inline=False)
            await ctx.channel.send(embed=e)
            return

        # Sort by username statuses
        lichess_playing = {}
        lichess_active = []
        lichess_offline = []
        for user in response_items:
            if 'playing' in user and user['playing']:
                # Get current game's PGN
                game_pgn_str = requests.get(f'https://lichess.org/api/user/{user["name"]}/current-game', params={'username': user['name']}).text
                
                # Create a Game object to parse
                game_obj = chess.pgn.read_game(io.StringIO(game_pgn_str))
                board = game_obj.board()
                
                # Make moves to setup board to current state, and get last move
                # (Will be 3 moves behind for anti-cheating according to Lichess API docs)
                last_move = None
                for move in game_obj.mainline_moves():
                    board.push(move)
                    last_move = move.uci()
                game_fen = board.fen()

                # Get current game's URL
                game_url = game_obj.headers['Site']
                
                # Get URL to image of board
                # Source: https://github.com/niklasf/web-boardimage
                # First get orientation
                if game_obj.headers['White'] == user['name']:
                    orientation = 'white'
                elif game_obj.headers['Black'] == user['name']:
                    orientation = 'black'
                else:
                    raise ValueError(f'Error: Neither white nor black are use "{user["name"]}". White is {game_obj.headers["White"]}, Black is {game_obj.headers["Black"]}')
                # Truncate FEN after first space to insert in URL
                game_fen_trunc = game_fen[:game_fen.find(' ')]
                # Build the URL
                img_url = f'https://backscattering.de/web-boardimage/board.png?fen={game_fen_trunc}&lastMove={last_move}&orientation={orientation}'
                
                # Finally update the dict
                lichess_playing[user['name']] = {
                    'fen': game_fen,
                    'url': game_url,
                    'img': img_url
                }
            elif 'online' in user and user['online']:
                lichess_active.append(user['name'])
            else:
                lichess_offline.append(user['name'])

        # Lichess - Playing Now section
        if lichess_playing:
            lines = []
            has_image = False
            for u, dct in lichess_playing.items():
                if not has_image:
                    img = dct['img']
                    has_image = True
                    shown_below = 'shown below - '
                else:
                    shown_below = ''

                lines.append(f'**`{u}`**: Playing now on Lichess ({shown_below}[watch game]({dct["url"]}))')

            if has_image:
                e.set_image(url=img)
            
            e.add_field(name='In Game  ⚔', value='\n'.join(lines), inline=False)
        
        # Lichess - Active section
        if lichess_active:
            lines = []
            for u in lichess_active:
                lines.append(f'**`{u}`**: Active on Lichess')
            e.add_field(name='Active  ⚡', value='\n'.join(lines), inline=False)
        
        # Lichess - Offline section
        if lichess_offline:
            lines = []
            for u in lichess_offline:
                lines.append(f'**`{u}`**')
            e.add_field(name='Offline  💤', value='\n'.join(lines), inline=False)
        

        # Send the final message
        await ctx.channel.send(embed=e)
        
        # Include so bot typing animation does not continue long after await ^
        ptest('pass')

keep_alive()
ptest(os.getenv('TOKEN'))
bot.run(os.getenv('TOKEN'))
