from typing import *
import os
import io
import re
import requests
import json
import datetime
import sqlite3
import discord
from discord.ext import commands
import chess.pgn
from dotenv import load_dotenv

from icecream import ic

load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')

bot = commands.Bot(command_prefix='/')

LINK_TO_CODE = 'https://replit.com/@jackson_hall/UVMCC-Discord-Bot'
USERS_DB_NAME = 'users.db'
LICHESS_TABLE_NAME = 'tblUsers'
CHESSCOM_TABLE_NAME = 'tblUsersChesscom'
LOG_FILENAME = '_action_log.txt'
EMBED_FOOTER = '♟  I\'m a bot, beep boop  ♟  Click my icon for the code  ♟  v1.0  ♟'

DEBUG = True
VERBOSE = True  # Shows more debug info, ex. for successful DB queries
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
        # await ctx.channel.send('Usage: `/add <username> <lichess / chess.com>`')
        await ctx.channel.send('Usage: `/add <username>`')
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
        response = json.loads(requests.get('https://lichess.org/api/users/status', params={'ids': username}).text)

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
        raise ValueError('error: TODO - must handle once Chess.com usernames implemented')

@bot.command(brief='Shows Lichess player statuses (Chess.com coming soon)')
async def show(ctx, *args):
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

        # Get all users in the database
        if len(args) == 1:
            exit_code, db_response = db_query(USERS_DB_NAME, 'SELECT pmkUsername FROM tblUsers WHERE pmkUsername LIKE ?', params=args)
            usernames = list(args)
        elif len(args) == 2:
            await ctx.channel.send('Usage: `/show [<username>]`')
            return
        else:
            exit_code, db_response = db_query(USERS_DB_NAME, 'SELECT pmkUsername FROM tblUsers ORDER BY pmkUsername')
            usernames = [e[0] for e in list(db_response)]

        # Send request for all usernames - only valid usernames will be returned
        # (and valid ones will be returned with proper capitalization)
        response = requests.get("https://lichess.org/api/users/status", params={'ids': ','.join(usernames)})
        response_items = json.loads(response.text)

        # Don't build full embed if no users were returned
        if not response_items:
            if args:
                e.add_field(name='Invalid username', value=f'`{args[0]}` isn\'t on Lichess.', inline=False)
            else:
                e.add_field(name='No Players', value='There aren\'t any players in the database. Use `/add` to add yourself!', inline=False)
            await ctx.channel.send(embed=e)
            return

        # Sort by username statuses
        lichess_playing = {}  # {username: {fen: "", game_url: ""}, ...}
        lichess_active = []   # ["username", ...]
        lichess_offline = []  # ["username", ...]
        featured_game_desc: str = ''  # Will get prepended to embed footer
        featured_game_was_set: bool = False
        for user in response_items:
            if 'playing' in user and user['playing']:
                # Add user to "playing" list
                game_pgn_str = requests.get(f'https://lichess.org/api/user/{user["name"]}/current-game', params={'username': user['name']}).text
                game_obj = chess.pgn.read_game(io.StringIO(game_pgn_str))
                game_url = game_obj.headers['Site']
                lichess_playing[user['name']] = {'url': game_url}

                # Choose first "playing" game found for the featured image: compose an img URL
                # based on the FEN, user orientation, and last move for web-boardimage
                # Source: https://github.com/niklasf/web-boardimage
                if not featured_game_was_set:
                    # Get FEN
                    # Create a Game object to parse
                    board = game_obj.board()
                    # Make moves to setup board to current state, and save last move
                    # (will be 3 moves behind for anti-cheating according to Lichess API docs)
                    last_move = None
                    for move in game_obj.mainline_moves():
                        board.push(move)
                        last_move = move.uci()
                    game_fen = board.fen()

                    # Get orientation
                    if game_obj.headers['White'] == user['name']:
                        orientation = 'white'
                    else:
                        orientation = 'black'
                    
                    # String that will be prepended to embed's footer (at end of function)
                    featured_game_desc = f'{game_obj.headers["White"]} ({game_obj.headers["WhiteElo"]}) - {game_obj.headers["Black"]} ({game_obj.headers["BlackElo"]}) on Lichess\n\n'
                    
                    # Truncate FEN to just the board layout part
                    game_fen_trunc = game_fen[:game_fen.find(' ')]
                    
                    # Set the URL
                    img_url = f'https://backscattering.de/web-boardimage/board.png?fen={game_fen_trunc}&orientation={orientation}{"&lastMove="+last_move if last_move else ""}'
                    lichess_playing[user['name']]['img'] = img_url
                    featured_game_was_set = True

            elif 'online' in user and user['online']:
                lichess_active.append(user['name'])
            else:
                lichess_offline.append(user['name'])

        ''' Build the embed message '''

        # Playing Now section
        if lichess_playing:
            lines = []
            has_image = False
            for u, dct in lichess_playing.items():
                if 'img' in dct:
                    img = dct['img']
                    has_image = True
                    shown_below = 'shown below - '
                else:
                    shown_below = ''

                lines.append(f'**`{u}`**: Playing now on Lichess ({shown_below}[watch game]({dct["url"]}))')

            if has_image:
                e.set_image(url=img)
            
            e.add_field(name='In Game  ⚔', value='\n'.join(lines), inline=False)
        
        # Active section
        if lichess_active:
            lines = []
            for u in lichess_active:
                lines.append(f'**`{u}`**: Active on Lichess')
            e.add_field(name='Active  ⚡', value='\n'.join(lines), inline=False)
        
        # Offline section
        if lichess_offline:
            lines = []
            for u in lichess_offline:
                lines.append(f'**`{u}`**')
            e.add_field(name='Offline  💤', value='\n'.join(lines), inline=False)
        
        # Footer
        e.set_footer(text=featured_game_desc+EMBED_FOOTER)

        # Send the final message
        await ctx.channel.send(embed=e)


@bot.command(brief='Generate a Lichess game link that any two players can join (default 10+5 casual)')
async def play(ctx, *args):
    async with ctx.channel.typing():
        # Sent when weeding out badly formatted commands
        USAGE = 'Usage: `/play [<min>+<sec>] [rated]`'
        
        # Set default time format if none is specified
        DEFAULT_TIME_FORMAT = '10+5'
        DEFAULT_RATED = 'casual'
        if not args:
            time_format = DEFAULT_TIME_FORMAT
            rated = DEFAULT_RATED
        elif len(args) == 1:
            if args[0] in ('rated', 'casual'):
                time_format = DEFAULT_TIME_FORMAT
                rated = args[0].strip()
            else:
                time_format = args[0].strip()
                rated = DEFAULT_RATED
        elif len(args) == 2:
            time_format, rated = args
        else:
            await ctx.channel.send(USAGE)
            return
        
        if rated.lower() not in ('rated', 'casual'):
            await ctx.channel.send(USAGE)
            return
        
        if '+' not in time_format or time_format == '0+0':
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE}')
            return
        
        minutes, seconds = time_format.split('+')
        fraction_formats = {
            '1/4': 15,
            '1/2': 30,
            '3/4': 45
        }
        if '/' in seconds \
                or ('/' in minutes and minutes not in fraction_formats) \
                or ('/' not in minutes and not re.match('^[0-9]{0,3}\+[0-9]{1,3}$', time_format.strip())):
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE}')
            return

        # Time format parts: clock_limit+clock_inc
        clock_limit = None
        clock_inc = None
        # Convert minutes to clock_limit
        if '/' in minutes:
            clock_limit = fraction_formats[minutes]
        else:
            try:
                clock_limit = int(minutes) * 60
            except ValueError:
                await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE}')
                return
        # Convert seconds to clock_inc
        try:
            clock_inc = int(seconds)
        except ValueError:
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE}')
            return

        # Validate clock_limit, clock_inc
        # https://lichess.org/api#operation/challengeOpen
        if clock_limit > 10800 or clock_inc > 180:
            await ctx.channel.send('Error: maximum time format is 180+180')
            return
        # elif clock_limit == 0 and clock_inc < 3:
        #     # clock_inc = 3
        #     ...  # TODO
        
        is_rated = rated.lower() == 'rated'

        # Make POST request
        post_data = {
            'rated': ('false', 'true')[is_rated],
            'clock.increment': clock_inc,
            'clock.limit': clock_limit
        }
        response = json.loads(requests.post('https://lichess.org/api/challenge/open',
                                            data=post_data).text)
        
        # Catch API errors
        if 'error' in response:
            await ctx.channel.send('Error: Lichess API could not generate a game link :(')
            return

        # Send embed message in chat
        game_id = response['challenge']['id']
        main_link = response['challenge']['url']
        white_link = response['urlWhite']
        black_link = response['urlBlack']
        time_format_shown = response['challenge']['timeControl']['show']
        e = discord.Embed(title=f'Created game `{game_id}`: a {time_format_shown} {["casual", "rated"][is_rated]} challenge')
        e.set_author(name=bot.user.name,
                     url=LINK_TO_CODE,
                     icon_url=bot.user.avatar_url)
        lines = [
            f'[As White]({white_link})',
            f'[As Black]({black_link})',
            f'[As Random]({main_link})'
        ]
        e.add_field(name='Play  ⚔', value='\n'.join(lines), inline=False)
        e.set_footer(text=EMBED_FOOTER)
        
        await ctx.channel.send(embed=e)


bot.run(TOKEN)
