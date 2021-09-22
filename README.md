# University of Vermont Chess Club Discord Bot

This is the source code for UVMCC#6718 on Discord. Feel free to submit an issue here on Github with feature requests or DM me on Discord with ideas for the bot!

## Commands
- `/add <username>` - Add a username to the database players. Your status, and possibly an image of your current game, will be displayed when users types `/show` in chat. Currently only Lichess usernames are supported, but Chess.com support may be coming soon! (Future syntax will be `/add <username> <lichess / chess.com>`.)
- `/remove <username>` - Removes a username from the database. (Future syntax will be `/remove <username>[ <lichess / chess.com>]`.)
- `/show` - Shows statuses of all users in the Lichess and Chess.com username databases as either Active, Playing Now, or Offline. A player that is in-game may have their game's current position selected as the embedded message's image.
- `/play [<min>+<sec>] [rated]` - Generate links for an open game challenge on Lichess that any two people can join. Default challenge type is 10+5 casual unless another time format or "rated" is specified.
- `/help`: Shows a help message.

## Contribute
If you'd like to contribute, let me know on Discord first. Adding commands is not too difficult if you are familiar with Python. If you are affiliated with the club, we can talk about getting you the bot's authentication token that will allow you to control the bot via [Discord's API](https://discord.com/developers/docs/intro).

After you've done that, use git to manage your changes. If you've never used git before, here's a rundown:
1. Clone this repo: `git clone https://github.com/jacksonthall22/UVMCC-Discord-Bot.git`
1. Make a new local branch off of `main` and give it some descriptive name for what you will be working on, like `puzzles-command`: `git checkout -b <branch-name>`
1. When you're ready to commit, stage your changes. You can stage specific files to be committed with `git add file.txt` or add all modified files with `git add -A`. If you've made a lot of changes without committing, it's good practice to stage and commit the files separately (unless the new code in some of those files relies on each other - then try to commit them together so code remains functional throughout the branch's commit history).
1. Commit your changes (often and in small chunks) to save the changes in the staged files to your local branch: `git commit -m "A short message on what you changed"`
1. Push your changes when you want to update files on the remote branch. First time: `git push --set-upstream origin <branch-name>`, later times: `git push`. You can list all the branch names using `git branch`.
1. Create a pull request for your branch when the feature is working (here on Github).