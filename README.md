# University of Vermont Chess Club Discord Bot

This is the source code for UVMCC#6718 on discord. If you are affiliated with the club, feel free to submit issues with feature requests, create your own pull requests, or DM me on Discord with ideas for the bot!

## Commands
`/add <username>` - Add a username to the database players. Your status, and possibly an image of your current game, will be displayed when users types `/show` in chat. Currently only Lichess usernames are supported, but Chess.com support may be coming soon! (Future syntax will be `/add <username> <lichess / chess.com>`.)
`/remove <username>` - Removes a username from the database. (Future syntax will be `/remove <username>[ <lichess / chess.com>]`.)
`/show` - Shows statuses of all users in the Lichess and Chess.com username databases as either Active, Playing Now, or Offline. A player that is in-game may have their game's current position selected as the embedded message's image.
`/help`: Shows a help message.
