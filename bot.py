import math
from typing import Tuple, Union, Literal, List, Any
import chess
import chess.pgn
import os
import io
import re
import requests
import json
import datetime
import sqlite3
import discord
from discord.ext import commands
from dotenv import load_dotenv
import random
import time
from collections import defaultdict, OrderedDict, Counter

from icecream import ic

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
bot = commands.Bot(command_prefix='/')

LINK_TO_CODE = 'https://github.com/jacksonthall22/UVMCC-Discord-Bot'
DB_FILENAME = 'users.db'
LOG_FILENAME = '_action_log.txt'
EMBED_FOOTER = '♟  I\'m a bot, beep boop  ♟  Click my icon for the code  ♟  v2.0  ♟'

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


def log(s, filename=LOG_FILENAME):
    with open(filename, 'a') as f:
        f.write(f'{str(datetime.datetime.now())} {s}'.strip() + '\n')


def db_query(db_name: str,
             query: str,
             params: Tuple = None,
             do_log=LOG,
             log_filename=LOG_FILENAME,
             debug=DEBUG,
             verbose=VERBOSE) \
        -> Tuple[int, List[Any]]:
    """
    Connect with the given sqlite3 database and execute a query. Return a
    custom exit code and cur.fetchall() for the command.
    """

    # Remove large spaces in the query
    query = ' '.join(query.split())

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
        if DEBUG and VERBOSE:
            print(f'Query succeeded: `{query}` in database `{db_name}` with params {params}')
        log_str += '\nQuery succeeded'
    else:
        if DEBUG:
            print(f'-----> QUERY FAILED: `{query}` with params {params} in database {db_name}')
            print(f'Stack trace:\n{err}')
        log_str += f'\nQuery failed with exit code 1. Stack trace:\n{err}'

    if do_log:
        log(log_str, log_filename)

    return exit_code, query_result


def get_board_image(fen: str,
                    orientation: chess.COLOR_NAMES,
                    last_move_uci: str = None) -> str:
    """ Get a URL for a PNG image of a chess board with the given FEN highlighting the last move. """
    # Validate fen
    chess.Board(fen)

    # Truncate FEN to just the board layout part
    fen_trunc = fen[:fen.find(' ')]

    return f'https://backscattering.de/web-boardimage/board.png' \
           f'?fen={fen_trunc}' \
           f'&orientation={orientation}' \
           f'{"&lastMove=" + last_move_uci if last_move_uci else ""}'


def get_current_board(*, pgn: str = None, game: chess.pgn.Game = None) -> chess.Board():
    """ Get the Board after pushing all moves in the pgn's/Game's mainline. """
    assert (pgn, game).count(None) == 1

    if game is None:
        game = chess.pgn.read_game(io.StringIO(pgn))

    b = game.board()
    for move in game.mainline_moves():
        b.push(move)
    return b


# noinspection PyShadowingBuiltins
def get_last_move(*,
                  pgn: str = None,
                  game: chess.pgn.Game = None,
                  format: Literal['san', 'uci', 'move'] = 'san') \
        -> Union[str, chess.Move, None]:
    """ Get the last move played in the pgn/Game, or None if the mainline has no moves. """
    assert (pgn, game).count(None) == 1
    if game is None:
        game = chess.pgn.read_game(io.StringIO(pgn))

    assert format in ("san", "uci", "move"), f'error: invalid format "{format}"'

    b = game.board()
    last_move = None
    last_uci = None
    last_san = None
    for move in game.mainline_moves():
        last_move = move
        last_uci = move.uci()
        last_san = b.san(move)
        b.push(move)

    if format == 'san':
        return last_san
    if format == 'uci':
        return last_uci
    if format == 'move':
        return last_move


def get_last_node(*,
                  pgn: str = None,
                  game: chess.pgn.Game = None) \
        -> chess.pgn.ChildNode:
    """ Get the last node in the pgn/Game mainline, or None if the mainline has no moves. """
    assert (pgn, game).count(None) == 1
    if game is None:
        game = chess.pgn.read_game(io.StringIO(pgn))

    last_node = game.root()
    for node in game.mainline():
        last_node = node

    return last_node


def get_ply(*, fen: str = None, board: chess.Board = None) -> int:
    """ Get the (half-move) plies of the position. """
    assert (fen, board).count(None) == 1
    if board is None:
        board = chess.Board(fen)

    return board.ply()


def get_turn(*, fen: str = None, board: chess.Board = None, as_str: bool = False) \
        -> Union[bool, Literal['white', 'black']]:
    """ Get the turn of the board either as a string or a bool (where white = True). """
    assert (fen, board).count(None) == 1

    if board is None:
        board = chess.Board(fen)

    turn = board.turn
    if as_str:
        return chess.COLOR_NAMES[turn]
    return turn


def to_fullmoves(*, plies: int) -> int:
    """ Convert from plies to fullmoves. """
    return plies // 2 + 1


def format_move_number(*, ply: int) -> str:
    """ Get a string to prefix the next move at a given plies, like "1. " or "1...". """
    next_fullmove_num = to_fullmoves(plies=ply)
    if ply % 2 == 0:
        return f'{next_fullmove_num}. '
    return f'{next_fullmove_num}... '


''' ========== Commands ========== '''


@bot.event
async def on_ready():
    logged_in = f'========================================\n' \
                f'Logged in as {bot.user}\n' \
                f'Current time: {datetime.datetime.now()}\n' \
                f'========================================'
    print(logged_in)
    if LOG:
        log(logged_in)

    RESET_VOTE_CHESS_TABLES = False
    if RESET_VOTE_CHESS_TABLES:
        print('==================')
        print('DELETING & RESETTING VOTE CHESS TABLES')

        DROP_QUERIES = [
            'DROP TABLE MatchStatuses',
            'DROP TABLE MatchSides',
            'DROP TABLE VoteMatches',
            'DROP TABLE VoteMatchPairings',
            'DROP TABLE VoteMatchVotes',
        ]
        for q in DROP_QUERIES:
            db_query(DB_FILENAME, q)
        print('DONE')
        print('==================')

    # User info
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS DiscordUsers (discord_id TEXT PRIMARY KEY)')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS ChessSites (site TEXT PRIMARY KEY COLLATE NOCASE)')
    db_query(DB_FILENAME, 'INSERT OR IGNORE INTO ChessSites(site) VALUES ("lichess.org"), ("chess.com")')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS ChessUsernames '
                          '(username TEXT PRIMARY KEY, '
                          'discord_id TEXT, '
                          'site TEXT, '
                          'FOREIGN KEY(discord_id) REFERENCES DiscordUsers(discord_id), '
                          'FOREIGN KEY(site) REFERENCES ChessSites(site))')
    # Vote Chess tables
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS MatchStatuses '
                          '(status TEXT PRIMARY KEY COLLATE NOCASE)')
    db_query(DB_FILENAME, 'INSERT OR IGNORE INTO MatchStatuses(status) VALUES '
                          '("Not Started"), '
                          '("Aborted"), '
                          '("In Progress"), '
                          '("Abandoned"), '
                          '("Complete")')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS MatchSides '
                          '(side TEXT PRIMARY KEY COLLATE NOCASE)')
    db_query(DB_FILENAME, 'INSERT OR IGNORE INTO MatchSides(side) VALUES '
                          '("Black"), '
                          '("White"), '
                          '("Both"), '
                          '("random")')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS MatchResults '
                          '(result TEXT PRIMARY KEY COLLATE NOCASE)')
    db_query(DB_FILENAME, 'INSERT OR IGNORE INTO MatchResults(result) VALUES '
                          '("checkmate"), '
                          '("resignation"), '
                          '("abandonment"), '
                          '("stalemate"), '
                          '("repetition"), '
                          '("mutual agreement"), '
                          '("50-move rule"), '
                          '("unknown")')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS MatchTerminations '
                          '(termination TEXT PRIMARY KEY COLLATE NOCASE)')
    db_query(DB_FILENAME, 'INSERT OR IGNORE INTO MatchTerminations VALUES '
                          '("1-0"), '
                          '("0-1"), '
                          '("1/2-1/2"), '
                          '("*")')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS VoteMatches '
                          '(match_code TEXT PRIMARY KEY, '
                          'match_name TEXT, '
                          'pgn TEXT, '
                          'starting_fen TEXT NOT NULL '
                          '             DEFAULT "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",'
                          'status TEXT NOT NULL, '
                          'hours_between_moves INTEGER DEFAULT 1 NOT NULL, '
                          'last_move_unix_time INTEGER, '
                          'unix_time_created INTEGER NOT NULL, '
                          'unix_time_started INTEGER, '
                          'unix_time_ended INTEGER, '
                          'result TEXT DEFAULT NULL, '
                          'termination TEXT NOT NULL DEFAULT "*", '
                          'hide_votes INTEGER NOT NULL DEFAULT 1, '  # 1 == TRUE
                          'FOREIGN KEY(status) REFERENCES MatchStatuses(status), '
                          'FOREIGN KEY(result) REFERENCES MatchResults(result), '
                          'FOREIGN KEY(termination) REFERENCES MatchTerminations(termination))')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS VoteMatchPairings '
                          '(match_code TEXT NOT NULL, '
                          'discord_id TEXT NOT NULL, '
                          'side TEXT NOT NULL, '
                          'votes_cast INTEGER DEFAULT 0, '
                          'top_move_votes_cast INTEGER DEFAULT 0, '
                          'FOREIGN KEY(match_code) REFERENCES VoteMatches(match_code), '
                          'FOREIGN KEY(discord_id) REFERENCES DiscordUsers(discord_id), '
                          'PRIMARY KEY(match_code, discord_id), '
                          'FOREIGN KEY(side) REFERENCES MatchSides(side))')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS VoteMatchVotes '
                          '(match_code TEXT NOT NULL, '
                          'discord_id TEXT NOT NULL, '
                          'ply_count INTEGER NOT NULL DEFAULT 0, '
                          'vote TEXT, '
                          'voted_resign INTEGER NOT NULL DEFAULT 0, '
                          'voted_draw INTEGER NOT NULL DEFAULT 0, '
                          'FOREIGN KEY(match_code, discord_id)'
                          ' REFERENCES VoteMatchPlies(match_code, discord_id),'
                          'PRIMARY KEY(match_code, discord_id, ply_count))')
    db_query(DB_FILENAME, 'CREATE TABLE IF NOT EXISTS VoteMatchDrawOffers '
                          '(match_code TEXT NOT NULL, '
                          'ply_count INTEGER NOT NULL, '
                          'voted_draw INTEGER NOT NULL, '
                          'FOREIGN KEY(match_code) REFERENCES VoteMatches(match_code), '
                          'PRIMARY KEY(match_code, ply_count))')
    print('Finished on_ready()')
    print('==================')


@bot.command(brief='Says hello')
async def hello(ctx):
    # import time
    msg = await ctx.channel.send('Hello!')
    # embed = discord.Embed(title="Sample Embed", url="https://realdrewdata.medium.com/",
    #                       description="This is an embed that will show how to build an embed and the different components",
    #                       color=0xFF5733)
    # await ctx.send(embed=embed)
    if VERBOSE:
        print('`hello()` run!')
    # time.sleep(5)
    # await msg.edit(content='Hello! (test)')


@bot.command(brief='Add a username to names in /show')
async def add(ctx, *args):
    if len(args) == 1:
        args = [args[0], 'lichess']

    if len(args) != 2 or args[1].lower() not in ('lichess', 'chess.com'):
        await ctx.channel.send('Usage: `/add <username> [lichess|chess.com]`')
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
        db_query(DB_FILENAME,
                 'INSERT INTO DiscordUsers(discord_id) VALUES (?)',
                 params=(str(ctx.message.author),))
        code, _ = db_query(DB_FILENAME,
                           'INSERT INTO ChessUsernames(username, site) VALUES (?, "lichess.org")',
                           params=(username_proper_caps,))
        if code == 0:
            await ctx.channel.send(f'Added `{username_proper_caps}` (Lichess) to the database. '
                                   f'Use `/show` to see who\'s online!')
        elif code == 2:
            await ctx.channel.send('That name is already in the Lichess database!')
        else:
            await ctx.channel.send('There was an error inserting your username into the database. '
                                   'DM @Cubigami and it can be added manually.')
    elif site == 'chess.com':
        await ctx.channel.send('Chess.com is not currently supported, but it will be soon!')


@bot.command(brief='Remove a username from names in /show')
async def remove(ctx, *args):
    if len(args) == 2 and args[1].lower() not in ('lichess', 'chess.com'):
        await ctx.channel.send('Usage: `/remove <username> [lichess|chess.com]`')
        return
    elif len(args) not in (1, 2):
        await ctx.channel.send('Usage: `/remove <username> [lichess|chess.com]`')
        return

    # Get username and site the username applies to (if given)
    args = [a.lower() for a in args]

    if len(args) == 2:
        username, site = args
    else:
        username = args[0]
        site = None

    # Handle differently depending on site
    invalid_lichess_uname = False
    invalid_chesscom_uname = False
    if site is None:
        # If site not given, remove all instances of username in DB
        await remove(ctx, username, 'lichess')
        await remove(ctx, username, 'chess.com')
        return

    if site.lower() == 'lichess':
        queried_site = site
        if site == 'lichess':
            queried_site = 'lichess.org'
        code, result = db_query(DB_FILENAME,
                                'SELECT username FROM ChessUsernames WHERE username LIKE ? AND site LIKE ?',
                                params=(username, queried_site))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return

        if not result:
            # No users in DB result -> not a valid username
            invalid_lichess_uname = True
            await ctx.channel.send(f'`{username}` was not in the database.')
        else:
            # Remove the username
            username_proper_caps, = result[0]
            code, result = db_query(DB_FILENAME, 'DELETE FROM ChessUsernames WHERE username = ? AND site = ?',
                                    params=(username_proper_caps, queried_site))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return

            await ctx.channel.send(f'Removed `{username_proper_caps}` (Lichess) from the database.')
            return

    if site.lower() == 'chess.com':
        invalid_chesscom_uname = True  # TODO
        not_in_chesscom_db = True

    if invalid_lichess_uname and invalid_chesscom_uname:
        raise ValueError('error: TODO - must handle once Chess.com usernames implemented')


@bot.command(brief='Connect your Discord ID to a Lichess username so you can use `/show me`')
async def iam(ctx, *args):
    """ Let a user associate a Lichess or Chess.com username with their Discord ID. """
    # Note: Not fully functional if 2 users have the same username on different sites.

    async with ctx.channel.typing():
        if len(args) != 1:
            await ctx.channel.send('Usage: `/iam <username>`')
            return
        username = args[0]

        # Make sure username is in the database
        code, result = db_query(DB_FILENAME, 'SELECT username FROM ChessUsernames '
                                             'WHERE username LIKE ?',
                                params=(username,))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return
        if not result:
            await ctx.channel.send(f'`{username}` isn\'t in the database. Use `/add {username}` to add them first.')
            return

        # Update the discord_id for the username
        discord_id = str(ctx.message.author)
        username, = result[0]  # Has proper caps
        code, _ = db_query(DB_FILENAME, 'UPDATE ChessUsernames SET discord_id = ? '
                                        'WHERE username = ?',
                           params=(discord_id, username))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return

        await ctx.channel.send(f'Linked `{username}` to `{discord_id}`. Use `/show` to see your status!')


@bot.command(brief='Unlink your Discord account from a Lichess username.')
async def iamnot(ctx, *args):
    """ Remove association between a lichess or chess.com username and the user's Discord ID. """
    # Note: Not fully functional if 2 users have the same username on different sites.

    async with ctx.channel.typing():
        if len(args) != 1:
            await ctx.channel.send('Usage: `/iamnot <username>`')
            return
        username = args[0]

        # Make sure username is in the database
        code, result = db_query(DB_FILENAME, 'SELECT username FROM ChessUsernames '
                                             'WHERE username LIKE ?',
                                params=(username,))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return
        if not result:
            await ctx.channel.send(f'`{username}` isn\'t in the database.')
            return

        # Update the discord_id for the username
        discord_id = str(ctx.message.author)
        username, = result[0]  # Has proper caps
        code, _ = db_query(DB_FILENAME, 'UPDATE ChessUsernames SET discord_id = NULL '
                                        'WHERE username LIKE ?',
                           params=(username,))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return

        await ctx.channel.send(f'Removed link from `{username}` to `{discord_id}`.')


@bot.command(brief='Show all chess accounts linked to the specified Discord account, '
                   'or show the Discord ID linked to a chess account')
async def whois(ctx, *args):
    """ Show linked chess account(s) of the specified Discord ID or vice versa. """

    async with ctx.channel.typing():
        if len(args) != 1:
            await ctx.channel.send('Usages:\n'
                                   ' - `/whois <Discord username>#<XXXX>`, ex. `/whois cubigami#3114`\n'
                                   ' - `/whois <Lichess/Chess.com username>`, ex. `/whois cubigami`')
            return

        # Determine what to search for
        if '#' in args[0]:
            discord_id = args[0]

            code, result = db_query(DB_FILENAME, 'SELECT discord_id, username FROM ChessUsernames '
                                                 'WHERE discord_id LIKE ?',
                                    params=(discord_id,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'`{discord_id}` isn\'t a Discord username in the database.')
                return

            discord_id_proper_caps, _ = result[0]
            response = f'`{discord_id_proper_caps}` has linked the following chess accounts:\n'
            for _, username in result:
                response += f'\n - `{username}` (Lichess)'

            await ctx.channel.send(response)
            return
        else:
            username = args[0]

            code, result = db_query(DB_FILENAME,
                                    'SELECT discord_id, username FROM ChessUsernames '
                                    'WHERE username LIKE ?',
                                    params=(username,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'`{username}` isn\'t a chess username in the database.')
                return

            discord_id_proper_caps, username_proper_caps = result[0]
            if discord_id_proper_caps is None:
                await ctx.channel.send(f'`{username_proper_caps}` is not linked to any Discord account.')
                return

            await ctx.channel.send(f'`{username_proper_caps}` is `{discord_id_proper_caps}`.')


@bot.command(brief='Show accounts you have connected to your Discord ID')
async def whoami(ctx, *args):
    """ Show connected account(s) of the specified player. """
    async with ctx.channel.typing():
        if len(args) != 0:
            await ctx.channel.send('Usage: `/whoami`')
            return

        discord_id = str(ctx.message.author)
        code, result = db_query(DB_FILENAME, 'SELECT username FROM ChessUsernames '
                                             'WHERE discord_id LIKE ?',
                                params=(discord_id,))
        if code != 0:
            await ctx.channel.send('There was a database error :(')
            return
        if not result:
            await ctx.channel.send(f'You haven\'t linked any chess accounts to `{discord_id}`. Use `/iam <username>` '
                                   f'to add one!')
            return

        response = f'You have linked the following chess accounts to `{discord_id}`:'
        for username in result:
            response += f'\n - `{username}` (Lichess)'

        await ctx.channel.send(response)


@bot.command(brief='Shows Lichess player statuses (Chess.com coming soon)')
async def show(ctx, *args):
    """
    Shows a formatted list of all users in the database and their
    current Lichess/Chess.com status (playing/active/offline)
    """
    async with ctx.channel.typing():
        # Build embedded message
        e = discord.Embed(title='Lichess Player Statuses',
                          color=0xB58863)
        e.set_author(name=bot.user.name,
                     url=LINK_TO_CODE,
                     icon_url=bot.user.avatar_url)

        # Get all users in the database
        if len(args) == 1:
            username = args[0].lower()
            if username == 'me':
                code, result = db_query(DB_FILENAME,
                                        'SELECT username FROM ChessUsernames '
                                        'WHERE discord_id = ?',
                                        params=(str(ctx.message.author),))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send('You have no Lichess or Chess.com usernames linked to '
                                           'your Discord account. Use `/iam <username>` to link some.')
                    return
                else:
                    usernames = [e for e, in result]
            else:
                code, _ = db_query(DB_FILENAME, 'SELECT * FROM ChessUsernames '
                                                'WHERE username LIKE ?',
                                   params=(username,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                usernames = [username]
        elif len(args) >= 2:
            await ctx.channel.send('Usage: `/show [<username>]`')
            return
        else:
            code, result = db_query(DB_FILENAME, 'SELECT username FROM ChessUsernames '
                                                 'ORDER BY username')
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            usernames = [e for e, in result]

        # Send request for all usernames - only valid usernames will be returned
        # (and valid ones will be returned with proper capitalization)
        response = json.loads(requests.get("https://lichess.org/api/users/status",
                                           params={'ids': ','.join(usernames)}).text)

        # Don't build full embed if no users were returned
        if not response:
            if args:
                e.add_field(name='Invalid username',
                            value=f'`{args[0]}` isn\'t on Lichess.',
                            inline=False)
            else:
                e.add_field(name='No Players',
                            value='There aren\'t any players in the database. Use `/add <username>` to add yourself!',
                            inline=False)
            await ctx.channel.send(embed=e)
            return

        # Sort by username statuses
        lichess_playing = {}  # {username: {fen: "", game_url: ""}, ...}
        lichess_active = []   # ["username", ...]
        lichess_offline = []  # ["username", ...]
        featured_game_desc: str = ''  # Will get prepended to embed footer
        featured_game_was_set: bool = False
        for user in response:
            if 'playing' in user and user['playing']:
                # Add user to "playing" list
                game_pgn_str = requests.get(f'https://lichess.org/api/user/{user["name"]}/current-game',
                                            params={'username': user['name']}).text
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

                    # TODO add IM/GM/etc. title before username if there is one
                    # String that will be prepended to embed's footer (at end of function)
                    featured_game_desc = f'{game_obj.headers["White"]} ({game_obj.headers["WhiteElo"]}) ' \
                                         f'- {game_obj.headers["Black"]} ({game_obj.headers["BlackElo"]}) on Lichess\n\n'

                    lichess_playing[user['name']]['img'] = get_board_image(game_fen, orientation, last_move)
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
        e.set_footer(text=featured_game_desc + EMBED_FOOTER)

        # Send the final message
        await ctx.channel.send(embed=e)


@bot.command(brief='Generate a Lichess game link that any two players can join (default 10+5 casual)')
async def play(ctx, *args):
    async with ctx.channel.typing():
        # Sent when weeding out badly formatted commands
        USAGE_MSG = 'Usage: `/play [<min>+<sec>] [rated]`'

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
            await ctx.channel.send(USAGE_MSG)
            return

        if rated.lower() not in ('rated', 'casual'):
            await ctx.channel.send(USAGE_MSG)
            return

        if '+' not in time_format or time_format == '0+0':
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE_MSG}')
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
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE_MSG}')
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
                await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE_MSG}')
                return
        # Convert seconds to clock_inc
        try:
            clock_inc = int(seconds)
        except ValueError:
            await ctx.channel.send(f'Error: invalid time format `{time_format}`. {USAGE_MSG}')
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
        e = discord.Embed(title=f'Created game `{game_id}`: a {time_format_shown} '
                                f'{["casual", "rated"][is_rated]} challenge',
                          color=discord.colour.Color.blue())
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


@bot.command(brief='Play in a Vote Chess match!')
async def vc(ctx, *args):
    # Conditions that must be met for a move to be made:
    #  - given time elapsed since last move (opponent move), unless everyone has voted
    #  - 50%+ players voted
    #  - Not a tie, unless everyone has voted
    #    - If everyone votes differently, reset the timer and recast the vote

    SUB_CMD_USAGE_MSGS = OrderedDict({
        'create': '`/vc create "<name for the match>" ["<starting FEN>"]`\n'
                  '> Creates a new match (uses starting position FEN by default). Give the match a fun name!',
        'abort': '`/vc abort <match code>`\n'
                 '> Aborts a match if it hasn\'t started. In-progress matches can\'t be aborted, they must be '
                 'resigned by majority vote using `/vc vote resign`.',
        'join': '`/vc join <match code> [white|black|both|`**`random`**`]`\n'
                '> Joins the match. Players joined as "random" will balance the teams randomly when the '
                'match starts.',
        'leave': '`/vc leave <match code>/all`\n'
                 '> Leaves the match if a match code is provided, or leaves all joined matches (only those that are '
                 'not in progress).',
        'start': '`/vc start <match code>`\n'
                 '> Starts the match. Use this command only after all players that want to have joined.',
        'vote': '`/vc vote <move>|resign|draw [<match code>]`\n'
                '> Casts a vote! If the move is legal in only one of your active matches, the vote will be cast '
                'to that match automatically. Otherwise, the game\'s match code must be provided. Votes are final '
                'unless there is a tie at the end of the voting period, in which case all votes will be recast. Move '
                'must be in algebraic notation, like `Nf3`, or name two squares, like `g1f3`.',
        'status': '`/vc status [<match code>]`\n'
                  '> Shows info about the current position of the given match. If no match code is provided, '
                  'shows info about all of your active matches.',
        'show': '`/vc show`\n'
                '> Shows the status of active and recently-completed matches.',
        'rematch': '`/vc rematch <match code>`\n'
                   '> Starts a new match with the same teams as the other match, but as the opposite colors.',
        'remind': '`/vc remind <match code>`\n'
                  '> Tags players who haven\'t voted to remind them to vote.',
        'settings': '`/vc settings <match code> [votes=`**`hide`**`|show] [majority=<0–`**`100`**`>%]`\n'
                    '> Update match settings:\n'
                    '>   - `votes` sets whether to delete user messages after `/vote` is called\n'
                    '>   - `majority` sets the percent of team members that must vote for a move to be played at the '
                    'next count',
        'help': '`/vc help <sub-command>|all`\n'
                '> Show a help message explaining the sub-command, or all sub-commands using `/vc help all`'
    })
    USAGE_MSG_SHORT = f'Usage: `/vc {"|".join(cmd for cmd in SUB_CMD_USAGE_MSGS)}`\n' \
                      f'Call a sub-command without anything after (ex. just `/vc create`) to get a help message for ' \
                      f'that command, or use `/vc help all` to show a message describing all of `/vc`\'s sub-commands ' \
                      f'and their usages.'

    async with ctx.channel.typing():
        # Validate command usage
        if len(args) == 0:
            await ctx.channel.send(USAGE_MSG_SHORT)
            return
        sub_cmd = args[0].lower()

        ''' Handle sub-commands '''
        args = args[1:]

        if sub_cmd == 'create':
            if len(args) == 0:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return
            match_name = args[0]

            # Make a new game and set it up with the right FEN
            game = chess.pgn.Game()
            starting_fen = chess.STARTING_FEN
            if len(args) >= 2:
                # Validate that second arg is a FEN
                starting_fen = args[1]
                try:
                    game.setup(chess.Board(starting_fen))
                except ValueError:
                    await ctx.channel.send(
                        f'Error: invalid FEN "{starting_fen}". Make sure FEN is in quotes.\n' +
                        f'Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                    return

            # Get a unique match code
            while True:
                match_code = ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 4))
                code, result = db_query(DB_FILENAME, 'SELECT match_code FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    break
                print(f'test: skipping match code {match_code} (already existed)')

            # Add match to database
            db_query(DB_FILENAME,
                     'INSERT INTO VoteMatches(match_code, match_name, pgn, starting_fen, unix_time_created, status, '
                     'hours_between_moves, last_move_unix_time) VALUES (?, ?, ?, ?, ?, "Not Started", 1, NULL)',
                     params=(match_code, match_name, str(game), starting_fen, int(time.time())))

            # Create embed message
            e = discord.Embed(title=f'Created Vote Chess Match `{match_code}`: "**{match_name}**"',
                              color=discord.colour.Color.blue())
            e.set_author(name=bot.user.name,
                         url=LINK_TO_CODE,
                         icon_url=bot.user.avatar_url)
            orientation = get_turn(fen=starting_fen, as_str=True)
            e.set_thumbnail(url=get_board_image(starting_fen, orientation))
            e.add_field(name='Join', value=f'`/vc join {match_code} white`\n`/vc join {match_code} black`',
                        inline=False)
            e.add_field(name='Start Match', value=f'Once everyone has joined:\n`/vc start {match_code}`', inline=False)
            e.add_field(name='Vote', value='Vote for legal moves in [algebraic notation]('
                                           'https://en.wikipedia.org/wiki/Algebraic_notation_(chess)) or by specifying '
                                           'two squares, ex.:\n'
                                           '`/vc vote Nf3` or\n'
                                           '`/vc vote g1f3`', inline=True)
            e.set_footer(text=EMBED_FOOTER)
            await ctx.channel.send(embed=e)
        elif sub_cmd == 'abort':
            if len(args) != 1:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return

            match_code = args[0].upper()

            code, result = db_query(DB_FILENAME, 'SELECT status FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return

            if not result:
                await ctx.channel.send(f'There are no matches with code `{match_code}`. Check the code and try again.')
                return

            match_status, = result[0]
            if match_status == 'Not Started':
                # Abort the match
                code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches SET status = "Aborted" '
                                                'WHERE match_code LIKE ?',
                                   params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                await ctx.channel.send(f'Aborted match `{match_code}`.')
            elif match_status == 'In Progress':
                await ctx.channel.send(f'Cannot abort match `{match_code}`: match is in progress and must be '
                                       f'completed or abandoned by majority vote using `/vc vote abandon'
                                       f' {match_code}`. Abandoning a match loses the game.')
            elif match_status == 'Aborted':
                await ctx.channel.send(f'Cannot abort match `{match_code}`: match was aborted.')
            elif match_status == 'Abandoned':
                await ctx.channel.send(f'Cannot abort match `{match_code}`: match was abandoned.')
            elif match_status == 'Complete':
                await ctx.channel.send(f'Cannot abort match `{match_code}`: match has already finished.')
            else:
                await ctx.channel.send(f'There was a database integrity error :(. Match `{match_code}` has unknown '
                                       f'status "{match_status}" and could not be aborted. DM @Cubigami and the issue '
                                       f'can be resolved manually.')
        elif sub_cmd == 'join':
            if len(args) not in (1, 2):
                code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name FROM VoteMatches '
                                                     'WHERE status LIKE "Not Started" '
                                                     'ORDER BY unix_time_created')
                msg = '\n\n**Open Vote Chess Matches**'
                if result:
                    for match_code, match_name in result:
                        code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                             'WHERE match_code LIKE ?',
                                                params=(match_code,))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        num_players_joined = len(result)
                        msg += f'\n> `{match_code}`: "**{match_name}**" ({num_players_joined} joined)'
                else:
                    if code != 0:
                        msg += '\nCan\'t get open matches, there was a database error :('
                    else:
                        msg += '\nThere are no open matches. Start one with `/vc create`!'

                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd] + msg)
                return

            # Make sure match exists
            match_code = args[0].upper()
            code, result = db_query(DB_FILENAME, 'SELECT match_code, pgn, status FROM VoteMatches '
                                                 'WHERE match_code = ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error while joining the match :(')
                return

            if not result:
                await ctx.channel.send(f'There are no matches with code `{match_code}`. Check the code and try again.')
                return

            match_code, pgn, match_status = result[0]

            if match_status == 'Not Started':
                # Validate `side`
                if len(args) == 2:
                    side = args[1].lower()
                    code, result = db_query(DB_FILENAME, 'SELECT side FROM MatchSides '
                                                         'WHERE side LIKE ?',
                                            params=(side,))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return
                    if not result:
                        await ctx.channel.send(f'Invalid option "{side}". Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                        return
                else:
                    side = 'random'

                code, _ = db_query(DB_FILENAME, 'REPLACE INTO VoteMatchPairings(match_code, discord_id, side) '
                                                'VALUES (?, ?, (SELECT side FROM MatchSides '
                                                '               WHERE side LIKE ?))',
                                   params=(match_code, str(ctx.message.author), side))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                ''' Create embed message '''
                e = discord.Embed(title=f'Joined Vote Chess Match `{match_code}`',
                                  color=discord.colour.Color.blue())
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                ''' Set thumbnail '''
                game = chess.pgn.read_game(io.StringIO(pgn))
                starting_fen = game.headers['FEN'] if 'FEN' in game.headers else chess.STARTING_FEN
                orientation = get_turn(fen=starting_fen, as_str=True)
                e.set_thumbnail(url=get_board_image(starting_fen, orientation))

                ''' Group players by their side (white/black/both/random) '''
                code, result = db_query(DB_FILENAME, 'SELECT discord_id, side FROM VoteMatchPairings '
                                                     'WHERE match_code = ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                players_by_side = defaultdict(list)
                for discord_id, side in result:
                    side = side.lower()
                    if side in ('white', 'black'):
                        players_by_side[side].append(discord_id)
                    elif side == 'both':
                        players_by_side['white'].append(discord_id)
                        players_by_side['black'].append(discord_id)
                    elif side == 'random':
                        players_by_side['random'].append(discord_id)
                    else:
                        await ctx.channel.send(f'There was a database integrity error :(. Unknown `side` "{side}" for '
                                               f'match pairing (match_code=`{match_code}`, discord_id={discord_id})')
                # White
                lines = [f'> {discord_id}' for discord_id in players_by_side['white']]
                e.add_field(name='⬜  White Team  ⬜   ', value='\n'.join(lines) if lines else '> None yet', inline=True)
                # Black
                lines = [f'> {discord_id}' for discord_id in players_by_side['black']]
                e.add_field(name='⬛  Black Team  ⬛   ', value='\n'.join(lines) if lines else '> None yet', inline=True)
                # Random
                msg = 'These players will randomly balance teams when the match starts.\n'
                lines = [f'> {discord_id}' for discord_id in players_by_side['random']]
                e.add_field(name='🎯  Random Team  🎯   ', value=(msg + ('\n'.join(lines) if lines else '> None yet')),
                            inline=False)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
            elif match_status == 'In Progress':
                await ctx.channel.send(f'Cannot join match `{match_code}`: match has already started.')
            elif match_status == 'Aborted':
                await ctx.channel.send(f'Cannot join match `{match_code}`: match was aborted.')
            elif match_status == 'Abandoned':
                await ctx.channel.send(f'Cannot join match `{match_code}`: match was abandoned.')
            elif match_status == 'Complete':
                await ctx.channel.send(f'Cannot join match `{match_code}`: match has finished.')
            else:
                await ctx.channel.send(f'Cannot join match `{match_code}`: match has unknown status "{match_status}". '
                                       f'DM @Cubigami and the issue can be resolved manually.')
        elif sub_cmd == 'leave':
            if len(args) != 1:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return

            match_code = args[0].upper()
            if match_code == 'all':
                # Get all "Not Started" matches user has joined
                code, result = db_query(DB_FILENAME, 'SELECT match_code FROM VoteMatchPairings '
                                                     'WHERE discord_id = ? '
                                                     '      AND match_code LIKE ? '
                                                     '      AND match_code IN (SELECT match_code FROM VoteMatches '
                                                     '                         WHERE status = "Not Started")',
                                        params=(str(ctx.message.author), match_code))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                if not result:
                    # Send msg in case user has no "Not Started" matches but thinks they can leave "In Progress" ones
                    code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                         'WHERE discord_id = ? '
                                                         '      AND match_code LIKE ? '
                                                         '      AND match_code IN (SELECT match_code FROM VoteMatches '
                                                         '                         WHERE status = "In Progress")',
                                            params=(str(ctx.message.author), match_code))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                    if result:
                        await ctx.channel.send(f'You are playing in {len(result)} active Vote Chess matches. You may '
                                               f'not leave a match after it has started.')
                    else:
                        await ctx.channel.send('You have not joined any Vote Chess matches.')
                    return
                match_codes_to_leave = [match_code for match_code, in result]
                num_matches_to_leave = len(match_codes_to_leave)

                # User has "Not Started" matches - leave them all
                code, _ = db_query(DB_FILENAME, 'DELETE FROM VoteMatchPairings '
                                                'WHERE discord_id = ? '
                                                '      AND match_code IN (SELECT match_code FROM VoteMatches '
                                                '                         WHERE status = "Not Started")',
                                   params=(str(ctx.message.author),))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                # Get match names
                code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name FROM VoteMatches '
                                                     'WHERE match_code IN ?',
                                        params=(match_codes_to_leave,))

                msg = f'Left {num_matches_to_leave} Vote Chess matches:'
                for match_code, match_name in result:
                    msg += f'\n> `{match_code}`: "**{match_name}**"'
                await ctx.channel.send(msg)
            else:
                # Just leave the match using the user-provided match_code
                code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                     'WHERE discord_id = ? '
                                                     '      AND match_code LIKE ?',
                                        params=(str(ctx.message.author), match_code))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'There are no matches with code `{match_code}`. Check the code and '
                                           f'try again.')
                    return
                assert len(result) == 1, await ctx.channel.send(f'There was a database integrity error :(. '
                                                                f'Multiple ({len(result)}) match pairings with '
                                                                f'match_code=`{match_code}`, '
                                                                f'discord_id=`{str(ctx.message.author)}`')

                # TODO only allow user to leave "Not Started" matches

                code, _ = db_query(DB_FILENAME, 'DELETE FROM VoteMatchPairings '
                                                'WHERE discord_id = ? '
                                                '      AND match_code LIKE ?',
                                   params=(str(ctx.message.author), match_code))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                # Get match name
                code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                if not result:
                    await ctx.channel.send(f'There was a database error :(. Something weird is going on, match code '
                                           f'`{match_code}` was just found a second ago, but now it\'s not there.')
                    return

                match_code, match_name = result[0]
                await ctx.channel.send(f'Left Vote Chess match:\n'
                                       f'> `{match_code}`: "**{match_name}**"')
        elif sub_cmd == 'start':
            if len(args) != 1:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return
            match_code = args[0].upper()

            # Get status of given match
            code, result = db_query(DB_FILENAME, 'SELECT status FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'There are no matches with code `{match_code}`. Check the code and try again.')
                return
            if len(result) != 1:
                await ctx.channel.send(f'There was a database integrity error :(. Multiple '
                                       f'matches with match_code `{match_code}`')
                return
            status, = result[0]

            if status == 'Not Started':
                ''' Pair players who joined randomly '''
                # First get all pairings and the side(s) they joined
                code, result = db_query(DB_FILENAME, 'SELECT discord_id, side FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'Cannot start match `{match_code}`: no players have joined either side.')
                    return

                # Split by "side"
                players_by_side = defaultdict(list)
                for discord_id, side in result:
                    players_by_side[side.lower()].append(discord_id)

                ''' Make sure there are enough players '''
                random_players = players_by_side['random']
                white_players = players_by_side['white']
                black_players = players_by_side['black']
                both_players = players_by_side['both']

                num_white_players = len(white_players) + len(both_players)
                num_black_players = len(black_players) + len(both_players)
                num_random_players = len(random_players)

                if (num_white_players == 0 or num_black_players == 0) \
                        and num_random_players == 0:
                    side = ('black', 'white')[num_white_players == 0]
                    await ctx.channel.send(f'Cannot start match `{match_code}`: no players have joined as {side}.')
                    return
                elif num_white_players == num_black_players == 0 and num_random_players < 2:
                    await ctx.channel.send(f'Cannot start match `{match_code}`: not enough players.')
                    return

                ''' Transfer "random" players to a random team '''
                if random_players:
                    # If num_total_players is odd, this gives preference randomly to white if True, black if False
                    rand_preference = bool(random.getrandbits(1))

                    # Note that num_total_players might double-count some players playing as "both" - this is intended
                    num_total_players = num_white_players + num_black_players + num_random_players

                    # Number of players by team after random placement
                    future_num_white = (math.floor, math.ceil)[rand_preference](num_total_players / 2)
                    future_num_black = (math.floor, math.ceil)[not rand_preference](num_total_players / 2)

                    # Number to add to existing teams to reach these numbers
                    num_transfer_to_white = future_num_white - num_white_players
                    num_transfer_to_black = future_num_black - num_black_players

                    # If number to transfer to either side is negative, it means that balancing the teams
                    # (make them equal size or within 1 if total is odd) would require removing players
                    # from the larger team. In this case just add all random players to the smaller team.
                    if num_transfer_to_white < 0 or num_transfer_to_black < 0:
                        # Balancing black would mean removing players from white.
                        # In this case add all random players to the smaller team
                        if num_white_players < num_black_players:
                            num_transfer_to_white = num_random_players
                            num_transfer_to_black = 0
                        elif num_black_players < num_white_players:
                            num_transfer_to_white = 0
                            num_transfer_to_black = num_random_players
                        else:
                            # Team aren't equal if either number above is < 0, something would be wrong
                            # setting num_transfer_to_white/black
                            assert False

                    transfer_to_white = random.sample(random_players, num_transfer_to_white)
                    transfer_to_black = list(set(random_players) - set(transfer_to_white))
                    assert len(transfer_to_black) == num_transfer_to_black

                    # Update player sides to "white"/"black" for this match
                    for discord_id in transfer_to_white:
                        code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchPairings SET side = "White" '
                                                        'WHERE match_code LIKE ? '
                                                        'AND discord_id = ?',
                                           params=(match_code, discord_id))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                    for discord_id in transfer_to_black:
                        code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchPairings SET side = "Black" '
                                                        'WHERE match_code LIKE ? '
                                                        'AND discord_id = ?',
                                           params=(match_code, discord_id))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return

                ''' Set match status to "In Progress" '''
                current_time = int(time.time())
                code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                'SET status = "In Progress", unix_time_started = ? '
                                                'WHERE match_code LIKE ?',
                                   params=(current_time, match_code))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                ''' Get match name '''
                code, result = db_query(DB_FILENAME, 'SELECT match_name, starting_fen FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'There was a database integrity error :(. There should be a match with '
                                           f'match_code `{match_code}`, but none was found.')
                    return
                elif len(result) != 1:
                    await ctx.channel.send(f'There was a database integrity error :(. Multiple matches exist with '
                                           f'match code `{match_code}`.')
                    return
                match_name, starting_fen = result[0]
                orientation = get_turn(fen=starting_fen, as_str=True)

                ''' Send Embed message '''
                e = discord.Embed(title=f'Starting Match `{match_code}`: "**{match_name}**"',
                                  color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                e.set_image(url=get_board_image(starting_fen, orientation))
                msg = 'Use `/vc vote <move>` to cast your votes! A move will be played once all players on a team ' \
                      'have voted. If it\'s a tie, votes will be recast. You may vote to offer/accept a draw or ' \
                      'resign like this (TODO: not yet implemented):\n' \
                      '> `/vc vote draw`\n' \
                      '> `/vc vote resign`\n' \
                      'If the majority does the same, an draw offer is offered/accepted or the game is resigned.'
                e.add_field(name=f'It\'s {orientation} to move', value=msg, inline=False)
                msg = 'You can use `/vc status` to check the status on your active matches and see ' \
                      'who still needs to vote, and use `/vc remind` to tag all players on your team who haven\'t ' \
                      'voted on the current move.'
                e.add_field(name='Tips', value=msg, inline=False)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
                return
            elif status == 'Aborted':
                await ctx.channel.send(f'Cannot start match `{match_code}`: match was aborted.')
                return
            elif status == 'In Progress':
                await ctx.channel.send(f'Cannot start match `{match_code}`: match is in progress.')
                return
            elif status == 'Abandoned':
                await ctx.channel.send(f'Cannot start match `{match_code}`: match was abandoned.')
                return
            elif status == 'Complete':
                await ctx.channel.send(f'Cannot start match `{match_code}`: match has already finished.')
                return
            else:
                await ctx.channel.send(f'Match `{match_code}` has unknown status "{status}" and could not be '
                                       f'aborted. DM @Cubigami and the issue can be resolved manually.')
                return
        elif sub_cmd == 'vote':
            if len(args) not in (1, 2):
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return
            move_vote = args[0]

            if len(args) == 1:
                ''' No match code provided: try to infer it based on current board positions, and validate SAN move '''
                # Get all user's current matches - this includes ones that can't be voted on
                code, results = db_query(DB_FILENAME, 'SELECT match_code, match_name, pgn FROM VoteMatches '
                                                      'WHERE status = "In Progress" '
                                                      '      AND match_code IN (SELECT match_code '
                                                      '                         FROM VoteMatchPairings '
                                                      '                         WHERE discord_id = ?)',
                                         params=(str(ctx.message.author),))
                if code != 0:
                    await ctx.channel.send('A database error occurred :(')
                    return
                if not results:
                    code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name FROM VoteMatches '
                                                         'WHERE status LIKE "Not Started" '
                                                         'ORDER BY unix_time_created')
                    if code != 0:
                        await ctx.channel.send('A database error occurred :(')
                        return
                    msg = 'You are not participating in any active Vote Chess matches. Join one with ' \
                          '`/vc join`!\n\n' \
                          '**Open Vote Chess Matches**'
                    if result:
                        for match_code, match_name in result:
                            code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                                 'WHERE match_code LIKE ?',
                                                    params=(match_code,))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                            num_players_joined = len(result)
                            msg += f'\n> `{match_code}`: "**{match_name}**" ({num_players_joined} joined)'
                    else:
                        msg += '\nThere are no open matches. Start one with `/vc create`!'
                    await ctx.channel.send(msg)

                if move_vote.lower() in ('resign', 'draw'):
                    # If move is "resign" or "draw", this can be a vote in all active games
                    valid_match_codes = [e for e, _, _ in results]

                    if len(valid_match_codes) >= 2:
                        msg = f'Please use one of these commands:'
                        for valid_match_code in valid_match_codes:
                            msg += f'\n`/vc vote {move_vote} {valid_match_code}`'
                        msg += '\nRemember you can use `/vc status [<match code>]` to see which game is which.'

                        await ctx.channel.send(msg)
                        return
                else:
                    # Get list of match_codes from the user's active matches for which
                    # the entered move is legal in the match's current position. List should
                    # have length 1, or else send an error msg asking user to disambiguate
                    # with the match code. Match should have status "In Progress" or else
                    # send an error msg saying vote could not be cast.

                    # Check in which games the move is valid
                    valid_match_codes = []
                    for match_code, _, pgn in results:
                        # Get Board for this game
                        b = get_current_board(pgn=pgn)

                        # Make sure it's user's turn in this game
                        code, result = db_query(DB_FILENAME, 'SELECT side FROM VoteMatchPairings '
                                                             'WHERE match_code LIKE ? '
                                                             'AND discord_id = ?',
                                                params=(match_code, str(ctx.message.author)))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return

                        side, = result[0]
                        if side.lower() not in (chess.COLOR_NAMES[b.turn].lower(), 'both'):
                            continue

                        # Make sure move is legal in the position
                        try:
                            b.push_san(move_vote)
                        except (ValueError, AssertionError):
                            try:
                                b.push_uci(move_vote)
                            except (ValueError, AssertionError):
                                continue
                        valid_match_codes.append(match_code)

                    # Ask for match code if the same move is legal in another active match
                    if not valid_match_codes:
                        await ctx.channel.send(f'`{move_vote}` is not a valid move in any of your active games.')
                        return
                    if len(valid_match_codes) >= 2:
                        msg = f'`{move_vote}` is a valid move in more than one of your active games. ' \
                              f'Please use one of these commands:'
                        for valid_match_code in valid_match_codes:
                            msg += f'\n`/vc vote {move_vote} {valid_match_code}`'
                        msg += '\nRemember you can use `/vc status [<match code>]` to see which game is which.'

                        await ctx.channel.send(msg)
                        return

                match_code = valid_match_codes[0]
            else:
                ''' Match code is provided as second argument, no need to query to infer it. '''
                match_code = args[1].upper()

            ''' Verify that match_code exists '''
            # Get match even if its status is not "In Progress" to send appropriate error msgs in other cases
            code, result = db_query(DB_FILENAME, 'SELECT match_name, pgn, status, hide_votes FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'There are no matches with code `{match_code}`. Check the code and try again.')
                return
            assert len(result) == 1, await ctx.channel.send(f'There was a database integrity error :(. Multiple '
                                                            f'matches found with match code `{match_code}`')

            ''' Validate that status for given match_code is "In Progress" '''
            match_name, pgn, status, hide_votes = result[0]
            hide_votes = bool(hide_votes)

            current_board = get_current_board(pgn=pgn)
            current_fen = current_board.fen()
            orientation = get_turn(board=current_board, as_str=True)
            last_move = get_last_move(pgn=pgn, format='san')
            ply_count = current_board.ply()

            ''' Make sure status is "In Progress" '''
            if status == 'Not Started':
                await ctx.channel.send(f'Cannot cast vote: match `{match_code}` hasn\'t started yet. Use '
                                       f'`/vc start {match_code}` once all players have joined.')
                return
            elif status == 'Aborted':
                await ctx.channel.send(f'Cannot cast vote: match `{match_code}` was aborted.')
                return
            elif status == 'Abandoned':
                await ctx.channel.send(f'Cannot cast vote: match `{match_code}` was abandoned.')
                return
            elif status == 'Complete':
                await ctx.channel.send(f'Cannot cast vote: match `{match_code}` has already finished.')
                return
            elif status != 'In Progress':
                await ctx.channel.send(f'Match `{match_code}` has unknown status "{status}" and could not be '
                                       f'aborted. DM @Cubigami and the issue can be resolved manually.')
                return

            ''' Validate that user's "side" is the current side to move '''
            code, result = db_query(DB_FILENAME, 'SELECT side FROM VoteMatchPairings '
                                                 'WHERE match_code LIKE ? '
                                                 '      AND discord_id = ? '
                                                 '      AND side IN (?, "Both")',
                                    params=(match_code, str(ctx.message.author), orientation.capitalize()))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'It is {orientation}\'s turn in match `{match_code}`: "**{match_name}**". '
                                       f'You cannot vote for the other team!')
                return

            ''' Handle draw/resign offers first (and return to skip move validation later) '''
            if move_vote.lower() in ('draw', 'resign'):
                move_vote = move_vote.lower()

                ''' See if user already voted to draw/resign, if so tell them they already voted '''
                code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchVotes '
                                                     'WHERE match_code LIKE ? '
                                                     '      AND discord_id = ? '
                                                     '      AND ply_count = ? '
                                                     f'      AND voted_{move_vote} = 1',
                                        params=(match_code, str(ctx.message.author), ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if result:
                    # Player already voted to resign/draw
                    optional_str = f'to **{("resign", "offer a draw")[move_vote == "draw"]}** '
                    await ctx.channel.send(f'You already voted '
                                           f'{optional_str if not hide_votes else ""}'
                                           f'in match `{match_code}`.')
                    # Delete message
                    if hide_votes:
                        try:
                            await ctx.message.delete()
                        except discord.errors.Forbidden:
                            # Can't delete messages in a DM channel, just skip it
                            pass
                    return

                ''' Now we know here user didn't already vote to draw/resign, so cast the vote '''
                code, result = db_query(DB_FILENAME, f'SELECT voted_{move_vote} FROM VoteMatchVotes '
                                                     'WHERE match_code LIKE ?'
                                                     '      AND discord_id = ? '
                                                     '      AND ply_count = ?',
                                        params=(match_code, str(ctx.message.author), ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                ''' Either insert or update depending on whether user has already voted for a legal move '''
                if not result:
                    # User hasn't cast any vote yet for this ply - create a record with vote=NULL
                    code, _ = db_query(DB_FILENAME, 'INSERT INTO VoteMatchVotes(match_code, discord_id, '
                                                    f'                           ply_count, vote, voted_{move_vote}) '
                                                    'VALUES (?, ?, ?, NULL, 1)',
                                       params=(match_code, str(ctx.message.author), ply_count))
                    if code != 0:
                        await ctx.channel.send(f'There was a database error :(')
                        return
                else:
                    code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchVotes '
                                                    f'SET voted_{move_vote} = 1 '
                                                    'WHERE match_code LIKE ? '
                                                    '      AND discord_id = ? '
                                                    '      AND ply_count = ?',
                                       params=(match_code, str(ctx.message.author), ply_count))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                ''' Vote to resign/draw successfully inserted/updated - build Embed message '''
                # Get users that still need to vote in this match
                code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ? '
                                                     '      AND side IN (?, "Both") '
                                                     '      AND discord_id NOT IN (SELECT discord_id '
                                                     '                             FROM VoteMatchVotes '
                                                     '                             WHERE match_code LIKE ? '
                                                     '                                   AND ply_count = ? '
                                                     '                                   AND vote IS NOT NULL)',
                                        params=(match_code, orientation.capitalize(), match_code, ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                # If all players vote, tally votes immediately after sending embed
                all_players_voted = not result
                assert not all_players_voted
                # This^ should not happen here when voting to resign/draw since if all players have voted (vote!=NULL),
                # the votes would have been tallied immediately and player would not have had time to vote resign/draw

                players_not_voted = [player for player, in result]

                if not hide_votes:
                    title = f'`{str(ctx.message.author)}` voted ' \
                            f'to **{("resign", "offer a draw")[move_vote == "draw"]}** ' \
                            f'in match `{match_code}`: "**{match_name}**"'
                else:
                    title = f'A player cast their vote in match `{match_code}`: "**{match_name}**"'
                e = discord.Embed(title=title,
                                  color=(0x000000, 0xFFFFFF)[current_board.turn])
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                e.set_thumbnail(url=get_board_image(current_fen, orientation))
                if not all_players_voted:
                    e.add_field(name=f'Waiting on ({len(players_not_voted)})',
                                value='\n'.join(f'> {p}' for p in players_not_voted))
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)

                ''' Check whether majority has voted to draw/resign this ply '''
                # Step 1: get number of players on side to move's team
                code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ? '
                                                     '      AND side IN (?, "Both")',
                                        params=(match_code, orientation.capitalize()))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'There was a database integrity error :(. No players found on this '
                                           f'{orientation} for match `{match_code}`')
                    return
                num_players_on_side = len(result)
                num_votes_for_majority = num_players_on_side // 2 + 1

                # Step 2: Get number of players that voted to draw/resign
                assert move_vote in ('draw', 'resign')
                code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ? '
                                                     '      AND side IN (?, "Both") '
                                                     '      AND discord_id IN (SELECT discord_id FROM VoteMatchVotes '
                                                     '                         WHERE match_code LIKE ? '
                                                    f'                               AND voted_{move_vote} = 1 '
                                                    f'                               AND ply_count = ?)',
                                        params=(match_code, orientation.capitalize(), match_code, ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return

                num_players_voted = len(result)
                if num_players_voted == 0:
                    await ctx.channel.send(f'There was a database integrity error :(. At least one player has voted '
                                           f'to {move_vote} (just now) but it was not updated in the database')
                    return

                is_majority = num_players_voted >= num_votes_for_majority
                if is_majority:
                    if move_vote == 'draw':
                        ''' Set a record that a draw was offered at this ply '''
                        code, _ = db_query(DB_FILENAME, 'INSERT OR IGNORE '
                                                        'INTO VoteMatchDrawOffers(match_code, '
                                                        '                         ply_count, '
                                                        '                         voted_draw) '
                                                        'VALUES (?, ?, 1)',
                                           params=(match_code, ply_count))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return

                        ''' Check whether previous ply voted to draw (see whether "draw" is being offered/accepted) '''
                        code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchDrawOffers '
                                                             'WHERE match_code LIKE ? '
                                                             '      AND ply_count = ? '
                                                             '      AND voted_draw = 1',
                                                params=(match_code, ply_count - 1))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        if result:
                            ''' Draw is being accepted - end the game and update match details '''
                            game = chess.pgn.read_game(io.StringIO(pgn))
                            game.headers['Result'] = '1/2-1/2'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "mutual agreement", '
                                                            '    termination = "1/2-1/2", '
                                                            '    unix_time_ended = ?, '
                                                            '    pgn = ? '
                                                            'WHERE match_code LIKE ?',
                                               params=(int(time.time()), str(game), match_code))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return

                            # Make list telling how players voted (even though game was resigned/drawn)
                            code, result = db_query(DB_FILENAME,
                                                    'SELECT discord_id, vote, voted_draw, voted_resign '
                                                    'FROM VoteMatchVotes '
                                                    'WHERE match_code LIKE ? '
                                                    '      AND ply_count = ?',
                                                    params=(match_code, ply_count))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                            if not result:
                                await ctx.channel.send(
                                    'There was a database integrity error :(. Tried to tally votes, '
                                    'but there were none recorded for this ply in the database.')
                                return

                            # Group players by the moves they voted on
                            players_by_vote = defaultdict(list)
                            players_voted_draw = []
                            players_voted_resign = []
                            for discord_id, vote, voted_draw, voted_resign in result:
                                # ic(voted_draw)
                                # ic(voted_resign)

                                if vote is not None:
                                    players_by_vote[vote].append(discord_id)
                                if voted_draw:
                                    players_voted_draw.append(discord_id)
                                if voted_resign:
                                    players_voted_resign.append(discord_id)

                            e = discord.Embed(title=f'Game Over: Draw by agreement '
                                                    f'in Match `{match_code}`: "**{match_name}**"',
                                              color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                            e.set_author(name=bot.user.name,
                                         url=LINK_TO_CODE,
                                         icon_url=bot.user.avatar_url)
                            e.set_image(url=get_board_image(current_fen, orientation, last_move))
                            e.add_field(name=f'Match is over after {to_fullmoves(plies=current_board.ply())} moves',
                                        value=f'The game is a draw by mutual agreement.',
                                        inline=False)
                            if players_by_vote:
                                # msg = f'This is how {orientation}\'s team voted:\n'
                                msg = ''
                                groups = []
                                for move, players in sorted(players_by_vote.items(), key=lambda e: len(e[1]),
                                                            reverse=True):
                                    s = f'**`{move}`**:'
                                    for player in players:
                                        s += f'\n> {player}'
                                    groups.append(s)
                                e.add_field(name='Votes', value=msg + '\n'.join(groups), inline=False)
                            if players_voted_draw:
                                lines = []
                                for discord_id in players_voted_draw:
                                    lines.append(f'> {discord_id}')
                                e.add_field(name='Voted to Draw', value='\n'.join(lines), inline=True)
                            if players_voted_resign:
                                lines = []
                                for discord_id in players_voted_resign:
                                    lines.append(f'> {discord_id}')
                                e.add_field(name='Voted to Resign', value='\n'.join(lines), inline=True)
                            e.add_field(name='Want a rematch?',
                                        value=f'Use `/rematch {match_code}` to play another '
                                              f'game with the same teams, but with colors '
                                              f'reversed! (TODO)',
                                        inline=False)
                            e.set_footer(text=EMBED_FOOTER)
                            await ctx.channel.send(embed=e)
                            return
                        else:
                            ''' Draw is only being offered - already updated database table, nothing to do here '''
                            await ctx.channel.send('Draw was offered (TODO)')
                            return
                    else:
                        assert move_vote == 'resign'

                        ''' Players voted to resign - end the game and update match details '''
                        game = chess.pgn.read_game(io.StringIO(pgn))
                        termination = ('1-0', '0-1')[orientation == 'white']
                        game.headers['Result'] = termination

                        code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                        'SET status = "Complete", '
                                                        '    result = "resignation", '
                                                        '    termination = ?, '
                                                        '    unix_time_ended = ?,'
                                                        '    pgn = ? '
                                                        'WHERE match_code LIKE ?',
                                           params=(termination, int(time.time()), str(game), match_code))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return

                        ''' Build Embed message '''
                        # Make list telling how players voted (even though game was resigned/drawn)
                        code, result = db_query(DB_FILENAME, 'SELECT discord_id, vote, voted_draw, voted_resign '
                                                             'FROM VoteMatchVotes '
                                                             'WHERE match_code LIKE ? '
                                                             '      AND ply_count = ?',
                                                params=(match_code, ply_count))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        if not result:
                            await ctx.channel.send('There was a database integrity error :(. Tried to tally votes, '
                                                   'but there were none recorded for this ply in the database.')
                            return
                        # Group players by the moves they voted on
                        players_by_vote = defaultdict(list)
                        players_voted_draw = []
                        players_voted_resign = []
                        for discord_id, vote, voted_draw, voted_resign in result:
                            # ic(voted_draw)
                            # ic(voted_resign)

                            if vote is not None:
                                players_by_vote[vote].append(discord_id)
                            if voted_draw:
                                players_voted_draw.append(discord_id)
                            if voted_resign:
                                players_voted_resign.append(discord_id)

                        # Create the embed message
                        e = discord.Embed(title=f'Game Over: {orientation.capitalize()} Resigned '
                                                f'in Match `{match_code}`: "**{match_name}**"',
                                          color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                        e.set_author(name=bot.user.name,
                                     url=LINK_TO_CODE,
                                     icon_url=bot.user.avatar_url)
                        e.set_image(url=get_board_image(current_fen, orientation, last_move))
                        e.add_field(name=f'Match is over after {to_fullmoves(plies=current_board.ply())} moves',
                                    value=f'{orientation.capitalize()} wins by Resignation.',
                                    inline=False)
                        if players_by_vote:
                            # msg = f'This is how {orientation}\'s team voted:\n'
                            msg = ''
                            groups = []
                            for move, players in sorted(players_by_vote.items(), key=lambda e: len(e[1]), reverse=True):
                                s = f'**`{move}`**:'
                                for player in players:
                                    s += f'\n> {player}'
                                groups.append(s)
                            e.add_field(name='Votes', value=msg + '\n'.join(groups), inline=False)
                        if players_voted_draw:
                            lines = []
                            for discord_id in players_voted_draw:
                                lines.append(f'> {discord_id}')
                            e.add_field(name='Voted to Draw', value='\n'.join(lines), inline=True)
                        if players_voted_resign:
                            lines = []
                            for discord_id in players_voted_resign:
                                lines.append(f'> {discord_id}')
                            e.add_field(name='Voted to Resign', value='\n'.join(lines), inline=True)
                        e.add_field(name='Want a rematch?',
                                    value=f'Use `/rematch {match_code}` to play another '
                                          f'game with the same teams, but with colors '
                                          f'reversed! (TODO)',
                                    inline=False)
                        e.set_footer(text=EMBED_FOOTER)
                        await ctx.channel.send(embed=e)
                        return

                await ctx.channel.send('Resign/draw offers are not yet implemented.')
                return

            ''' Validate that move is legal in this position '''
            b = current_board.copy()
            try:
                move = b.push_san(move_vote)
            except ValueError:
                try:
                    move = b.push_uci(move_vote)
                except ValueError:
                    await ctx.channel.send(f'`{move_vote}` is not a valid move '
                                           f'in match `{match_code}`: "**{match_name}**"')
                    return
            # Turns ex. "Qh4" into "Qh4#" if necessary
            move_vote = current_board.san(move)

            # Here, move_vote is not "draw"/"resign"
            ''' Validate that user hasn't already voted '''
            code, result = db_query(DB_FILENAME, 'SELECT vote FROM VoteMatchVotes '
                                                 'WHERE match_code LIKE ? '
                                                 '      AND discord_id LIKE ? '
                                                 '      AND ply_count LIKE ? '
                                                 '      AND vote IS NOT NULL',
                                    params=(match_code, str(ctx.message.author), ply_count))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if result:
                if len(result) != 1:
                    await ctx.channel.send(f'There was a database integrity error :(. '
                                           f'`{str(ctx.message.author)}` has already '
                                           f'voted more than once ({len(result)} times)')
                    return
                vote, = result[0]
                code, result = db_query(DB_FILENAME, 'SELECT hide_votes FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                assert len(result) == 1
                hide_votes, = result[0]

                await ctx.channel.send(f'You already voted '
                                       f'{f"for **{format_move_number(ply=ply_count)}{vote}** " if not hide_votes else ""}'
                                       f'in match `{match_code}`.')
                return

            ''' Cast a vote for the move '''
            code, _ = db_query(DB_FILENAME, 'INSERT INTO VoteMatchVotes(match_code, discord_id, ply_count, vote) '
                                            'VALUES (?, ?, ?, ?)',
                               params=(match_code, str(ctx.message.author), ply_count, move_vote))
            if code == 2:
                # Code 2 means unique constraint failed. This means user already voted to draw/resign
                # so there is already a record in the database with conflicting primary key. Instead update it.
                code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchVotes '
                                                'SET vote = ? '
                                                'WHERE match_code LIKE ? '
                                                '      AND discord_id = ? '
                                                '      AND ply_count = ?',
                                   params=(move_vote, match_code, str(ctx.message.author), ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
            elif code != 0:
                await ctx.channel.send(f'There was a database error :(. Error code: {code}')
                return

            ''' Delete user message that casted the vote '''
            author_discord_id = str(ctx.message.author)
            if hide_votes:
                try:
                    await ctx.message.delete()
                except discord.errors.Forbidden:
                    # Can't delete messages in a DM channel, just skip it
                    pass

            ''' Create embed message '''
            # Get users that still need to vote in this match
            code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                 'WHERE match_code LIKE ? '
                                                 '      AND side IN (?, "Both") '
                                                 '      AND discord_id NOT IN (SELECT discord_id FROM VoteMatchVotes '
                                                 '                             WHERE match_code LIKE ? '
                                                 '                                   AND ply_count = ? '
                                                 '                                   AND vote IS NOT NULL)',
                                    params=(match_code, orientation.capitalize(), match_code, ply_count))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return

            # If all players vote, tally votes immediately after sending embed
            all_players_voted = not result
            players_not_voted = [player for player, in result]

            ''' Send embed message '''
            if not hide_votes:
                if move_vote.lower() in ('draw', 'resign'):
                    vote_str = f'to **{("draw", "resign")[move_vote.lower() == "resign"]}**'
                else:
                    vote_str = f'for **{format_move_number(ply=ply_count)}{move_vote}**'
                title = f'`{author_discord_id}` cast their vote {vote_str} ' \
                        f'in match `{match_code}`: "**{match_name}**"'
            else:
                title = f'A player cast their vote in match `{match_code}`: "**{match_name}**"'
            e = discord.Embed(title=title,
                              color=(0x000000, 0xFFFFFF)[current_board.turn])
            e.set_author(name=bot.user.name,
                         url=LINK_TO_CODE,
                         icon_url=bot.user.avatar_url)
            e.set_thumbnail(url=get_board_image(current_fen, orientation))
            if not all_players_voted:
                e.add_field(name='Waiting on votes from:', value='\n'.join('> ' + p for p in players_not_voted))
            e.set_footer(text=EMBED_FOOTER)
            await ctx.channel.send(embed=e)

            if all_players_voted:
                code, result = db_query(DB_FILENAME, 'SELECT discord_id, vote, voted_draw, voted_resign '
                                                     'FROM VoteMatchVotes '
                                                     'WHERE match_code LIKE ? '
                                                     'AND ply_count = ?',
                                        params=(match_code, ply_count))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send('There was a database integrity error :(. Tried to tally votes, '
                                           'but there were none recorded for this ply in the database.')
                    return

                # Group players by the moves they voted on
                players_by_vote = defaultdict(list)
                players_voted_draw = []
                players_voted_resign = []
                for discord_id, vote, voted_draw, voted_resign in result:
                    # ic(voted_draw)
                    # ic(voted_resign)

                    if vote is not None:
                        players_by_vote[vote].append(discord_id)
                    if voted_draw:
                        players_voted_draw.append(discord_id)
                    if voted_resign:
                        players_voted_resign.append(discord_id)

                # Get list of (move_san, players_that_voted_for_this) tuples sorted by
                # how many voted for each move
                vote_counts = sorted(players_by_vote.items(), key=lambda e: len(e[1]), reverse=True)

                if len(vote_counts) == 1 or vote_counts[0][1] > vote_counts[1][1]:
                    # Everyone voted the same, or first vote has more votes than the second. In
                    # both cases it's fine to play the top-voted move

                    game = chess.pgn.read_game(io.StringIO(pgn))
                    current_board = get_current_board(pgn=pgn)
                    # This turns ex. "Qf7" to "Qf7#" if necessary
                    top_voted_move_san = current_board.san(current_board.parse_san(vote_counts[0][0]))

                    # Increment count of top_move_votes_cast for all players who voted for the top move
                    code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchPairings '
                                                    'SET top_move_votes_cast = top_move_votes_cast + 1 '
                                                    'WHERE match_code LIKE ? '
                                                    '      AND discord_id IN (SELECT discord_id FROM VoteMatchVotes '
                                                    '                         WHERE match_code LIKE ? '
                                                    '                               AND ply_count = ? '
                                                    '                               AND vote = ?)',
                                       params=(match_code, match_code, ply_count, top_voted_move_san))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return
                    # Increment count of votes_cast for all players
                    code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatchPairings '
                                                    'SET votes_cast = votes_cast + 1 '
                                                    'WHERE match_code LIKE ? '
                                                    '      AND discord_id IN (SELECT discord_id FROM VoteMatchVotes '
                                                    '                         WHERE match_code LIKE ? '
                                                    '                               AND ply_count = ?'
                                                    '                               AND vote IS NOT NULL)',
                                       params=(match_code, match_code, ply_count))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                    ''' Add variation to the match's PGN and update it in the database '''
                    current_board = get_current_board(game=game)
                    get_last_node(game=game).add_variation(current_board.parse_san(top_voted_move_san))
                    orientation = get_turn(board=current_board, as_str=True)
                    current_board.push_san(top_voted_move_san)
                    current_fen = current_board.fen()
                    last_move = get_last_move(game=game, format='uci')

                    ''' Handle current_board being in checkmate/stalemate etc. (end the match) '''
                    game_ended = False
                    if current_board.is_game_over():
                        # Update status/result/termination in database
                        if current_board.is_checkmate():
                            # Set termination
                            termination = ('1-0', '0-1')[current_board.turn]
                            result_str = 'checkmate'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "checkmate", '
                                                            '    termination = ?, '
                                                            '    unix_time_ended = ?',
                                               params=(termination, int(time.time())))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                        elif current_board.is_stalemate():
                            termination = '1/2-1/2'
                            result_str = 'stalemate'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "stalemate", '
                                                            '    termination = "1/2-1/2", '
                                                            '    unix_time_ended = ?',
                                               params=(int(time.time()),))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                        elif current_board.is_repetition():
                            termination = '1/2-1/2'
                            result_str = 'repetition'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "repetition", '
                                                            '    termination = "1/2-1/2", '
                                                            '    unix_time_ended = ?',
                                               params=(int(time.time()),))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                        elif current_board.is_fifty_moves():
                            termination = '1/2-1/2'
                            result_str = 'the fifty move rule'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "50-move rule", '
                                                            '    termination = "1/2-1/2", '
                                                            '    unix_time_ended = ?',
                                               params=(int(time.time()),))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                        else:
                            termination = '*'
                            result_str = 'an unknown reason'
                            code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches '
                                                            'SET status = "Complete", '
                                                            '    result = "unknown", '
                                                            '    termination = "*", '
                                                            '    unix_time_ended = ?',
                                               params=(int(time.time()),))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                        game_ended = True

                    code, _ = db_query(DB_FILENAME, 'UPDATE VoteMatches SET pgn = ? '
                                                    'WHERE match_code LIKE ?',
                                       params=(str(game), match_code))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                    ''' See whether draw was offered at this ply (but not on the previous ply) '''
                    code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchDrawOffers '
                                                         'WHERE match_code LIKE ? '
                                                         '      AND ply_count = ? '
                                                         '      AND voted_draw = 1 '
                                                         '      AND match_code NOT IN (SELECT match_code '
                                                         '                             FROM VoteMatchDrawOffers '
                                                         '                             WHERE ply_count = ? '
                                                         '                                   AND voted_draw = 1)',
                                            params=(match_code, ply_count, ply_count-1))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return
                    draw_offered = False
                    if result:
                        draw_offered = True
                        code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchVotes '
                                                             'WHERE match_code LIKE ?'
                                                             '      AND ply_count = ?'
                                                             '      AND voted_draw = 1',
                                                params=(match_code, ply_count))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        players_voted_draw = [p for p, in result]

                    ''' Send Embed message '''
                    if game_ended:
                        title = f'Game Over: ' \
                                f'{format_move_number(ply=ply_count)}{top_voted_move_san} Played ' \
                                f'in Match `{match_code}`: "**{match_name}**"'
                    else:
                        title = f'{format_move_number(ply=ply_count)}{top_voted_move_san} Played ' \
                                f'in Match `{match_code}`: "**{match_name}**"'
                    e = discord.Embed(title=title,
                                      color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                    e.set_author(name=bot.user.name,
                                 url=LINK_TO_CODE,
                                 icon_url=bot.user.avatar_url)
                    e.set_image(url=get_board_image(current_fen, orientation, last_move))
                    if game_ended:
                        if termination in ('1-0', '0-1'):
                            msg = f'{orientation.capitalize()} wins by {result_str}.'
                        elif termination == '1/2-1/2':
                            msg = f'Both teams draw by {result_str}.'
                        else:
                            msg = f'The game ended with an unknown result. DM @Cubigami and this issue can be ' \
                                  f'resolved manually.'
                        e.add_field(name=f'Match is over after {to_fullmoves(plies=current_board.ply())} moves',
                                    value=msg,
                                    inline=False)
                    if draw_offered:
                        assert not game_ended
                        lines = []
                        for discord_id in players_voted_draw:
                            lines.append(f'> {discord_id}')
                        msg = 'These players voted to draw:\n'
                        e.add_field(name=f'**{orientation.capitalize()} offers a draw.**', value=msg + '\n'.join(lines))
                    # msg = f'This is how {orientation}\'s team voted:\n'
                    msg = ''
                    groups = []
                    for move, players in sorted(players_by_vote.items(), key=lambda e: len(e[1]), reverse=True):
                        s = f'**`{move}`**:'
                        for player in players:
                            s += f'\n> {player}'
                        groups.append(s)
                    e.add_field(name='Votes', value=msg + '\n'.join(groups), inline=False)
                    if players_voted_draw:
                        lines = []
                        for discord_id in players_voted_draw:
                            lines.append(f'> {discord_id}')
                        e.add_field(name='Voted to Draw', value='\n'.join(lines), inline=True)
                    if players_voted_resign:
                        lines = []
                        for discord_id in players_voted_resign:
                            lines.append(f'> {discord_id}')
                        e.add_field(name='Voted to Resign', value='\n'.join(lines), inline=True)
                    if game_ended:
                        e.add_field(name='Want a rematch?',
                                    value=f'Use `/rematch {match_code}` to play another '
                                          f'game with the same teams, but with colors '
                                          f'reversed! (TODO)')
                    e.set_footer(text=EMBED_FOOTER)
                    await ctx.channel.send(embed=e)
                    return
                else:
                    # There's a tie for the first place votes, remove all votes and make players recast votes
                    # TODO add field in VoteMatchVotes to differentiate voting sessions and show history of ties
                    code, _ = db_query(DB_FILENAME, 'DELETE FROM VoteMatchVotes '
                                                    'WHERE match_code LIKE ? '
                                                    '      AND ply_count = ?',
                                       params=(match_code, ply_count))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                    current_board = get_current_board(pgn=pgn)
                    orientation = get_turn(board=current_board, as_str=True)
                    last_move = get_last_move(pgn=pgn, format='uci')

                    ''' Send Embed message '''
                    e = discord.Embed(title=f'Voting Tie for Move {format_move_number(ply=ply_count)} in Match '
                                            f'`{match_code}`: "**{match_name}**"',
                                      color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                    e.set_author(name=bot.user.name,
                                 url=LINK_TO_CODE,
                                 icon_url=bot.user.avatar_url)
                    e.set_image(url=get_board_image(current_fen, orientation, last_move))
                    # msg = f'This is how {orientation}\'s team voted:\n'
                    msg = ''
                    groups = []
                    for move, players in sorted(players_by_vote.items(), key=lambda e: len(e[1]), reverse=True):
                        s = f'**{move}**:'
                        for player in players:
                            s += f'\n> {player}'
                    e.add_field(name='Votes', value=msg + '\n'.join(groups), inline=False)
                    e.set_footer(text=EMBED_FOOTER)
                    await ctx.channel.send(embed=e)
                    return
        elif sub_cmd == 'status':
            if len(args) not in (0, 1):
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sum_cmd])
                return

            # Either infer the match_code if none is given, or use the user-inputted arg
            if len(args) == 0:
                # Get all active matches for this user, past & present
                code, result = db_query(DB_FILENAME,
                                        'SELECT match_code, match_name FROM VoteMatches '
                                        'WHERE match_code IN (SELECT match_code FROM VoteMatchPairings '
                                        '                     WHERE discord_id LIKE ?)'
                                        '      AND status IN ("Not Started", "In Progress")',
                                        params=(str(ctx.message.author),))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name FROM VoteMatches '
                                                         'WHERE status LIKE "Not Started" '
                                                         'ORDER BY unix_time_created')
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return

                    msg = f'Usage: {SUB_CMD_USAGE_MSGS["status"]}\n' \
                          f'You have not joined any Vote Chess matches, so you must specify a match code.\n\n' \
                          '**Open Vote Chess Matches**'
                    if result:
                        for match_code, match_name in result:
                            code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                                 'WHERE match_code LIKE ?',
                                                    params=(match_code,))
                            if code != 0:
                                await ctx.channel.send('There was a database error :(')
                                return
                            num_players_joined = len(result)
                            msg += f'\n> `{match_code}`: "**{match_name}**" ({num_players_joined} joined)'
                    else:
                        msg += '\nThere are no open matches. Start one with `/vc create`!'

                    await ctx.channel.send(msg)
                    return
                elif len(result) >= 2:
                    msg = f'Usage: {SUB_CMD_USAGE_MSGS[sub_cmd]}\n' \
                          'You are playing in more than one Vote Chess match, so a match code must be specified.' \
                          '\n\n**Your active Vote Chess matches**'
                    for match_code, match_name in result:
                        code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                             'WHERE match_code LIKE ?',
                                                params=(match_code,))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        num_players_joined = len(result)
                        msg += f'\n> `{match_code}`: "**{match_name}**" ({num_players_joined} players)'
                    await ctx.channel.send(msg)
                    return

                # User is only in one match - use this as the one to get status for
                match_code, match_name = result[0]
            else:
                match_code = args[0].upper()

                # Make sure match code exists
                code, result = db_query(DB_FILENAME, 'SELECT match_name FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'There are no matches with code `{match_code}`. '
                                           f'Check the code and try again.')
                    return
                assert len(result) == 1, await ctx.channel.send(f'There was a database integrity error :(. Multiple '
                                                                f'matches with match code `{match_code}`')
                match_name, = result[0]

            code, result = db_query(DB_FILENAME, 'SELECT pgn, starting_fen, status, hours_between_moves, '
                                                 'last_move_unix_time, unix_time_created, unix_time_started, '
                                                 'unix_time_ended, result, termination, hide_votes '
                                                 'FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                raise ValueError('error: bad input validation - match_code should exist here')
            assert len(result) == 1, await ctx.channel.send(f'There was a database integrity error :(. Multiple '
                                                            f'matches with match_code `{match_code}`')

            pgn, starting_fen, status, hours_between_moves, last_move_unix_time, unix_time_created, \
            unix_time_started, unix_time_ended, match_result, termination, hide_votes = result[0]

            if status == 'In Progress':
                ''' Get details about the game '''
                game = chess.pgn.read_game(io.StringIO(pgn))
                current_board = get_current_board(pgn=pgn)
                game.setup(current_board)
                current_fen = current_board.fen()
                current_ply = current_board.ply()
                orientation = get_turn(fen=current_fen, as_str=True)

                ''' Get a list of Discord usernames of players who HAVE NOT voted '''
                code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ? '
                                                     '      AND side IN (?, "Both") '
                                                     '      AND discord_id NOT IN (SELECT discord_id '
                                                     '                             FROM VoteMatchVotes '
                                                     '                             WHERE match_code LIKE ? '
                                                     '                                   AND ply_count = ? '
                                                     '                                   AND vote IS NOT NULL)',
                                        params=(match_code, orientation.capitalize(), match_code, current_ply))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if result:
                    waiting_on_str = '\n'.join([f'> {plr}' for plr, in result])
                    num_waiting_on = len(result)
                else:
                    waiting_on_str = 'Everyone has voted.\n' + '\n'.join([f'> {plr}' for plr, in result])
                    num_waiting_on = 0

                # Get a list of Discord usernames of players who HAVE voted
                code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                     'WHERE match_code = ? '
                                                     'AND side IN (?, "Both") '
                                                     'AND discord_id IN (SELECT discord_id FROM VoteMatchVotes '
                                                     '                   WHERE match_code LIKE ? '
                                                     '                         AND ply_count = ? '
                                                     '                         AND vote IS NOT NULL)',
                                        params=(match_code, orientation.capitalize(), match_code, current_ply))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if result:
                    already_voted_str = '\n'.join([f'> {plr}' for plr, in result])
                    num_already_voted = len(result)
                else:
                    already_voted_str = 'No votes yet'
                    num_already_voted = 0

                ''' Get a string showing the 3 most recent moves (6 plies) '''
                # ex. `10... Ne4 11. cxd5 cxd5 12. Qc2 O-O 13. O-O`
                recent_moves = ''
                mainline_moves = list(game.mainline_moves())
                temp_board = chess.Board(starting_fen)
                for i, move in enumerate(mainline_moves):
                    if i >= len(mainline_moves) - 6:
                        # This is one of the most recent 6 moves
                        san = temp_board.san(move)
                        move_num_str = format_move_number(ply=temp_board.ply())
                        if temp_board.turn == chess.BLACK:
                            if not recent_moves:
                                # Add number before black move if it's the first move added
                                recent_moves += f'{move_num_str}{san} '
                            else:
                                # Don't add move number for black, there's one for white's move before
                                recent_moves += f'{san} '
                        else:
                            # Always add move num for white
                            recent_moves += f'{move_num_str}{san} '
                    temp_board.push(move)
                recent_moves = recent_moves.strip()

                ''' Get lists of team members '''
                code, result = db_query(DB_FILENAME, 'SELECT discord_id, side FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                assert result, await ctx.channel.send('There was a database integrity error :(. Game status is "In '
                                                      'Progress" but has no joined players')
                players_by_side = defaultdict(list)
                for discord_id, side in result:
                    side = side.lower()
                    assert side != 'random', await ctx.channel.send('There was a database integrity error :(. Player '
                                                                    'side was "Random" even though it should have '
                                                                    'changed when the match started')
                    if side in ('white', 'black'):
                        players_by_side[side].append(discord_id)
                    elif side == 'both':
                        players_by_side['white'].append(discord_id)
                        players_by_side['black'].append(discord_id)
                    else:
                        await ctx.channel.send(f'There was a database integrity error :(. Unknown `side` "{side}" for '
                                               f'match pairing (match_code=`{match_code}`, discord_id={discord_id})')
                        return
                white_players = players_by_side['white']
                black_players = players_by_side['black']

                ''' Build final Embed message '''
                e = discord.Embed(title=f'Match `{match_code}`: "**{match_name}**"',
                                  color=(0x000000, 0xFFFFFF)[orientation == 'white'])
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                e.set_image(url=get_board_image(current_fen, orientation))
                e.add_field(name=f'Waiting on ({num_waiting_on})', value=waiting_on_str, inline=True)
                e.add_field(name=f'Already voted ({num_already_voted})', value=already_voted_str, inline=True)
                e.add_field(name=f'**Move {to_fullmoves(plies=current_ply)}**: {orientation.capitalize()} to move',
                            value='Recent moves:\n' + (f'> {recent_moves}' if recent_moves else '> No moves yet.'),
                            inline=False)
                e.add_field(name='⬜  White Team  ⬜   ', value='\n'.join([f'> {p}' for p in white_players]), inline=True)
                e.add_field(name='⬛  Black Team  ⬛   ', value='\n'.join([f'> {p}' for p in black_players]), inline=True)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
            elif status == 'Not Started':
                game = chess.pgn.read_game(io.StringIO(pgn))
                current_board = get_current_board(pgn=pgn)
                game.setup(current_board)
                current_fen = current_board.fen()
                orientation = get_turn(fen=current_fen, as_str=True)

                ''' Get lists of all players in this match '''
                code, result = db_query(DB_FILENAME, 'SELECT discord_id, side FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                assert result, await ctx.channel.send('There was a database integrity error :(. Game status is "In '
                                                      'Progress" but has no joined players')
                players_by_side = defaultdict(list)
                for discord_id, side in result:
                    side = side.lower()
                    if side in ('white', 'black', 'random'):
                        players_by_side[side].append(discord_id)
                    elif side == 'both':
                        players_by_side['white'].append(discord_id)
                        players_by_side['black'].append(discord_id)
                    else:
                        await ctx.channel.send(f'There was a database integrity error :(. Unknown `side` "{side}" for '
                                               f'match pairing (match_code=`{match_code}`, discord_id={discord_id})')
                        return
                white_players = players_by_side['white']
                black_players = players_by_side['black']
                random_players = players_by_side['random']

                ''' Build final Embed message '''
                e = discord.Embed(title=f'Match `{match_code}`: "**{match_name}**"',
                                  color=discord.colour.Color.blue())
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                e.set_thumbnail(url=get_board_image(current_fen, orientation))
                e.add_field(name='Status: Not Started',
                            value=f'When all players have joined, use `/vc start {match_code}`.',
                            inline=False)
                # White
                lines = [f'> {discord_id}' for discord_id in white_players]
                e.add_field(name='⬜  White Team  ⬜   ', value='\n'.join(lines) if lines else '> None yet', inline=True)
                # Black
                lines = [f'> {discord_id}' for discord_id in black_players]
                e.add_field(name='⬛  Black Team  ⬛   ', value='\n'.join(lines) if lines else '> None yet', inline=True)
                # Random
                msg = 'These players will randomly balance teams when the match starts.\n'
                lines = [f'> {discord_id}' for discord_id in random_players]
                e.add_field(name='🎯  Random Team  🎯   ', value=(msg + ('\n'.join(lines) if lines else '> None yet')),
                            inline=False)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
            elif status == 'Aborted':
                game = chess.pgn.read_game(io.StringIO(pgn))
                current_board = get_current_board(pgn=pgn)
                game.setup(current_board)
                current_fen = current_board.fen()
                orientation = get_turn(fen=current_fen, as_str=True)

                ''' Build final Embed message '''
                e = discord.Embed(title=f'Status Report: Match `{match_code}`: "**{match_name}**"',
                                  color=discord.colour.Color.blue())
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                e.set_image(url=get_board_image(current_fen, orientation))
                e.add_field(name='Status: Aborted', value='No moves were played.', inline=False)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
            elif status == 'Abandoned':
                await ctx.channel.send('Not yet implemented for Abandoned matches. DM @Cubigami and the issue can be '
                                       'resolved manually.')
                return
            elif status == 'Complete':
                await ctx.channel.send('Not yet implemented for Completed matches. DM @Cubigami and the issue can be '
                                       'resolved manually.')
                return
            else:
                await ctx.channel.send(f'Cannot join match `{match_code}`: match has unknown status "{status}". '
                                       f'DM @Cubigami and the issue can be resolved manually.')
                return
        elif sub_cmd == 'show':
            if len(args) != 0:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return

            # Get details about all active matches
            code, result = db_query(DB_FILENAME, 'SELECT match_code, match_name, status, pgn, '
                                                 '       unix_time_ended, result, termination '
                                                 'FROM VoteMatches '
                                                 'WHERE status IN ("Not Started", "In Progress", "Complete") '
                                                 'ORDER BY last_move_unix_time')
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                e = discord.Embed(title='Active Vote Chess Matches',
                                  color=discord.colour.Color.blue())
                e.add_field(name='No active matches  😥', value='Create one with `/vc create`!')
                await ctx.channel.send(embed=e)
                return

            ''' Split matches by their status '''
            matches_by_status = defaultdict(list)
            for r in result:
                matches_by_status[r[2]].append((*r[:2], *r[3:]))

            e = discord.Embed(title='Active Vote Chess Matches',
                              color=discord.colour.Color.blue())
            e.set_footer(text=EMBED_FOOTER)
            e.set_author(name=bot.user.name,
                         url=LINK_TO_CODE,
                         icon_url=bot.user.avatar_url)
            # Not Started
            lines = []
            for match_code, match_name, _, _, _, _ in matches_by_status['Not Started']:
                code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                num_players_joined = len(result)
                lines.append(f'> Match `{match_code}`: "**{match_name}**" ({num_players_joined} joined)')
            if lines:
                e.add_field(name='Not Started  🔎', value='\n'.join(lines))
            # In Progress
            lines = []
            for match_code, match_name, _, _, _, _ in matches_by_status['In Progress']:
                code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                num_players_joined = len(result)
                lines.append(f'> Match `{match_code}`: "**{match_name}**" ({num_players_joined} players)')
            if lines:
                e.add_field(name='In Progress  🤺', value='\n'.join(lines))
            # Recently completed
            current_unix_time = time.time()
            SECONDS_IN_WEEK = 604800
            lines = []
            for match_code, match_name, pgn, unix_time_ended, match_result, termination \
                    in matches_by_status['Complete']:
                if current_unix_time - SECONDS_IN_WEEK < unix_time_ended:
                    # Match finished less than 1 week ago
                    code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                         'WHERE match_code LIKE ?',
                                            params=(match_code,))
                    if code != 0:
                        await ctx.channel.send('There was a database error :(')
                        return
                    num_players_joined = len(result)

                    line = f'Match `{match_code}`: "**{match_name}**" ({num_players_joined} players)\n' \
                           f'> '
                    total_moves = to_fullmoves(plies=get_current_board(pgn=pgn).ply())
                    if termination == '1-0':
                        line += f'White won by {match_result} after {total_moves} moves'
                    elif termination == '0-1':
                        line += f'Black won by {match_result} after {total_moves} moves'
                    elif termination == '1/2-1/2':
                        line += f'Draw by {match_result} after {total_moves} moves'
                    elif termination == '*':
                        line += f'Unknown result after {total_moves} moves'
                    else:
                        await ctx.channel.send(f'There was a database integrity error :(. Unknown match termination "'
                                               f'{termination}" for match `{match_code}`')
                        return
                    lines.append(line)
            if lines:
                e.add_field(name='Recently Completed 🏳️', value='\n'.join(lines))

            await ctx.channel.send(embed=e)
        elif sub_cmd == 'rematch':
            ...
            await ctx.channel.send('Not yet implemented!')
        elif sub_cmd == 'remind':
            if len(args) not in (0, 1):
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
                return

            # Either infer the match_code if none is given, or use the user-inputted arg and make sure it exists
            if len(args) == 0:
                # Get all active matches for this user, past & present
                code, result = db_query(DB_FILENAME,
                                        'SELECT match_code, match_name FROM VoteMatches '
                                        'WHERE match_code IN (SELECT match_code FROM VoteMatchPairings '
                                        '                     WHERE discord_id LIKE ?)'
                                        '      AND status IN ("Not Started", "In Progress")',
                                        params=(str(ctx.message.author),))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    msg = f'Usage: {SUB_CMD_USAGE_MSGS[sub_cmd]}\n' \
                          f'You have not joined any Vote Chess matches. You can only use `/vc remind` for your own ' \
                          f'active matches.'

                    await ctx.channel.send(msg)
                    return
                elif len(result) >= 2:
                    msg = f'Usage: {SUB_CMD_USAGE_MSGS[sub_cmd]}\n' \
                          'You are playing in more than one Vote Chess match, so a match code must be specified.' \
                          '\n\n**Your active Vote Chess matches**'
                    for match_code, match_name in result:
                        code, result = db_query(DB_FILENAME, 'SELECT * FROM VoteMatchPairings '
                                                             'WHERE match_code LIKE ?',
                                                params=(match_code,))
                        if code != 0:
                            await ctx.channel.send('There was a database error :(')
                            return
                        num_players_joined = len(result)
                        msg += f'\n> `{match_code}`: "**{match_name}**" ({num_players_joined} players)'
                    await ctx.channel.send(msg)
                    return

                # User is only in one match - use this as the one to get status for
                match_code, match_name = result[0]
            else:
                match_code = args[0].upper()

                # Make sure match code exists
                code, result = db_query(DB_FILENAME, 'SELECT match_name FROM VoteMatches '
                                                     'WHERE match_code LIKE ?',
                                        params=(match_code,))
                if code != 0:
                    await ctx.channel.send('There was a database error :(')
                    return
                if not result:
                    await ctx.channel.send(f'There are no matches with code `{match_code}`. '
                                           f'Check the code and try again.')
                    return
                assert len(result) == 1, await ctx.channel.send(
                    f'There was a database integrity error :(. Multiple matches with '
                    f'match code `{match_code}`')
                match_name, = result[0]

            ''' Make sure match is "In Progress" '''
            code, result = db_query(DB_FILENAME, 'SELECT status FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            assert len(result) == 1
            status, = result[0]
            if status == 'Not Started':
                await ctx.channel.send(f'Cannot use `/vc remind` for match `{match_code}`: match has not started.')
                return
            elif status == 'Aborted':
                await ctx.channel.send(f'Cannot use `/vc remind` for match `{match_code}`: match was aborted.')
                return
            elif status == 'Abandoned':
                await ctx.channel.send(f'Cannot use `/vc remind` for match `{match_code}`: match was abandoned.')
                return
            elif status == 'Complete':
                await ctx.channel.send(f'Cannot use `/vc remind` for match `{match_code}`: match has already finished.')
                return
            elif status != 'In Progress':
                await ctx.channel.send(f'Cannot use `/vc remind` for match `{match_code}`: match has unknown status '
                                       f'"{status}". DM @Cubigami and the issue can be resolved manually.')
                return

            # Get match's side to move
            code, result = db_query(DB_FILENAME, 'SELECT pgn FROM VoteMatches '
                                                 'WHERE match_code LIKE ?',
                                    params=(match_code,))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            assert len(result) == 1
            pgn, = result[0]
            current_board = get_current_board(pgn=pgn)
            orientation = get_turn(board=current_board, as_str=True)
            current_ply = current_board.ply()

            # Get all players on the match's side to move that have not yet voted
            code, result = db_query(DB_FILENAME, 'SELECT discord_id FROM VoteMatchPairings '
                                                 'WHERE match_code LIKE ? '
                                                 'AND side IN (?, "Both") '
                                                 'AND discord_id NOT IN (SELECT discord_id FROM VoteMatchVotes '
                                                 '                       WHERE match_code LIKE ? '
                                                 '                             AND ply_count = ? '
                                                 '                             AND vote IS NOT NULL)',
                                    params=(match_code, orientation.capitalize(), match_code, current_ply))
            if code != 0:
                await ctx.channel.send('There was a database error :(')
                return
            if not result:
                await ctx.channel.send(f'There was a database integrity error :(. Match `{match_code}` is "In '
                                       f'Progress", but there are no players that need to vote for the current turn. '
                                       f'This should never happen since votes are tallied as soon as all players '
                                       f'have voted.')
                return
            discord_ids = [discord_id for discord_id, in result]

            ''' Create Embed message '''
            e = discord.Embed(title=f'Reminder: Cast Your Vote in Match `{match_code}`: "**{match_name}**"',
                              color=discord.colour.Color.blue())
            e.set_author(name=bot.user.name,
                         url=LINK_TO_CODE,
                         icon_url=bot.user.avatar_url)
            lines = [f'@{p}' for p in discord_ids]
            e.add_field(name='Waiting on votes from:', value='\n'.join(lines))
            e.set_footer(text=EMBED_FOOTER)
            await ctx.channel.send(embed=e)
        elif sub_cmd == 'settings':
            ...
            await ctx.channel.send('Not yet implemented!')
        elif sub_cmd == 'help':
            if len(args) != 1:
                await ctx.channel.send('Usage: ' + SUB_CMD_USAGE_MSGS[sub_cmd])
            help_sub_cmd = args[0].lower()

            if help_sub_cmd == 'all':
                e = discord.Embed(title=f'**`/vc {"|".join(cmd for cmd in SUB_CMD_USAGE_MSGS)}`**',
                                  color=discord.colour.Color.dark_gray())
                e.set_author(name=bot.user.name,
                             url=LINK_TO_CODE,
                             icon_url=bot.user.avatar_url)
                for cmd, desc in SUB_CMD_USAGE_MSGS.items():
                    e.add_field(name=cmd, value=desc, inline=True)
                e.set_footer(text=EMBED_FOOTER)
                await ctx.channel.send(embed=e)
            else:
                if help_sub_cmd not in SUB_CMD_USAGE_MSGS:
                    await ctx.channel.send(f'Invalid sub-command "{help_sub_cmd}". Check the spelling and try again.')

bot.run(TOKEN)
