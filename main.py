from scraper.valleyscraper import ValleyScraper
from pysqlite2 import dbapi2 as sqlite
import constants
import sys

def createTable(cur):
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

def insertTest(cur):
        cur.execute("""
                INSERT INTO articles VALUES (
                    null, 'src', 'alignment', 1, 'title', 'summary', 'text', 'http://', date('1889-12-26'), datetime('now','localtime')
                )
        """)

def main():
        createTable(cur)
        conn.commit()

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
		elif input == 'print':
			print lastfetch
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
	if not '-c' in sys.argv:
		main()
	# If we're not calling main, drop to Python console
