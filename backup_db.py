"""
https://www.a2hosting.com/kb/developer-corner/postgresql/import-and-export-a-postgresql-database

pg_dump -U username dbname > dbexport.pgsql

pg_dump -U nate rss_feeds > rss_feeds.pgsql

to load:

psql -U nate rss_feeds < rss_feeds.pgsql
"""



import os


from sqlalchemy import create_engine as ce

def create_engine():
    # create connection
    postgres_uname = os.environ.get('postgres_uname')
    postgres_pass = os.environ.get('postgres_pass')
    db = 'postgresql://{}:{}@localhost:5432/rss_feeds'.format(postgres_uname, postgres_pass)
    engine = ce(db, echo=False)
    return engine



# engine = create_engine()

# need to be superuser to get data director -- see scrape_reuters_rss.py for way to access
# engine.execute("show data_directory;")
