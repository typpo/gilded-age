from scraper.valleyscraper import ValleyScraper
from analyzer.calaisanalyzer import CalaisAnalyzer
from pysqlite2 import dbapi2 as sqlite
import constants
import sys

def createTables(cur):
    cur.execute("""
           CREATE TABLE IF NOT EXISTS articles
                (id INTEGER PRIMARY KEY,
                source TEXT,
                alignment TEXT,
                page INTEGER,
                title TEXT,
                summary TEXT,
                text TEXT,
                url TEXT,
                articleDate DATE,
                timeEnter DATE)
            """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS calais_items
                (id INTEGER PRIMARY KEY,
                type TEXT,
                data TEXT,
                count INTEGER)
            """)

    cur.execute("""
           CREATE TABLE IF NOT EXISTS calais_results
                (id INTEGER PRIMARY KEY,
                article_id INTEGER,
                relation_id INTEGER,
                relevance REAL,
                has_full_text BOOLEAN,
                FOREIGN KEY(article_id) REFERENCES articles(id),
                FOREIGN KEY(relation_id) REFERENCES calais_items(id))
            """)

    conn.commit()

def cleanAnalysis():
    cur.execute('DROP TABLE calais_items')
    cur.execute('DROP TABLE calais_results')
    conn.commit()

def main():
    cur.execute('SELECT * FROM articles')
    print len(cur.fetchall()), 'articles in database.'

    lastfetch = None
    while True:
        input = raw_input('> ')

        if input == 'scrape':
            # Run scrapers...
            if 'VALLEY' in constants.ENABLED_SCRAPERS:
                vs = ValleyScraper(conn)
                vs.execute()
        elif input == 'analyze':
            # Run analyzers on last result
            if 'CALAIS' in constants.ENABLED_ANALYZERS:
                ca = CalaisAnalyzer(conn)
                ca.execute(lastfetch)
        elif input == 'print':
            print lastfetch
        elif input == 'q':
            break
        else:
            try:
                cur.execute(input)
                lastfetch = cur.fetchall()
                print len(lastfetch), 'results.'
            except sqlite.OperationalError:
                lastfetch = None
                print 'Bad query.'

# Database setup
conn = sqlite.connect(constants.DB_FILE)
cur = conn.cursor()

# Main startup
if __name__ == "__main__":
    if '-t' in sys.argv:
        createTables(conn)
        print 'Generated tables, exiting'
    elif '--clean' in sys.argv:
        print 'Cleaning analysis tables'
        cleanAnalysis()
    elif not '-c' in sys.argv:
        main()
    # If we're not calling main, drop to Python console
