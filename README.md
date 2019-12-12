# get_news
Grabs Reuters news stories from their RSS feed.  The original motivation for this was noticing one day that a news story came out on a stock (I think MTCH, something about one of their apps they bought), and the market didn't react for a few hours, after which the price dropped substantially if I remember correctly.  I think it had to do with some lawsuit.  This gave me the idea of information arbitrage -- auto-detecting positive/negative news stories and using that for an auto-trader or alert system.

To run the data scraper,.first set up the postgresql database (details below), then run:
`python scrape_reuters_rss.py`



# Prereqs
Note: this instructions are intended for Linux (Ubuntu).  For Windows, it's probably different.  For Mac, it's probably pretty similar.

You might install the libraries with `pip install -r requirements.txt`.

You need postgresql installed (`sudo apt install postgresql -y`), and you need a password and user set for postgres that can access the DB we are using.  The DB name is `rss_feeds`.  The postgres username and password should also be stored as environment variables since these are loaded in the code.  These environment variables are:

`postgres_uname`
`postgres_pass`

In Ubuntu and many Linux distros, this can be set in the ~/.bashrc file with `export postgres_uname=postgres` and likewise for the password.  In the latest MacOS, they use zsh, and these can be set in ~/.zshrc.  In Windows, you can search for 'enviornment variables' in the search bar and there should be a GUI to add environment variables.

You can use the default postgres user, but you need to set a password. Or you can create a new user and set a password.  This can be done with CREATE USER or ALTER USER commands.  To use the restore db function, your user here should also have privileges to create databases.  The easiest way to do this is to make the user a superuser like the postgres user: `ALTER ROLE user1 WITH SUPERUSER;` (https://stackoverflow.com/a/46575000/4549682) after doing `sudo -iu postgres` and then `psql`.
