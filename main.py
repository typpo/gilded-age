from scraper.valleyscraper import ValleyScraper
from pysqlite2 import dbapi2 as sqlite
import constants

def createTable(cur):
        cur.execute("""
               CREATE TABLE IF NOT EXISTS articles
                    (id INTEGER PRIMARY KEY,
                    source TEXT,
                    summary TEXT,
                    data TEXT,
                    articleDate DATE,
                    timeEnter DATE)
                """)

def insertTest(cur):
        cur.execute("""
                INSERT INTO articles VALUES (
                    null, 'src', 'twat', 'twatwatwatwat', date('1989-12-26'), datetime('now','localtime')
                )
        """)

def main():
        conn = sqlite.connect(constants.DB_FILE)
        cur = conn.cursor()

        createTable(cur)
        conn.commit()

        cur.execute('SELECT * FROM articles')
        print len(cur.fetchall()), 'articles in db at', constants.DB_FILE

        # Run scrapers...
	if 'VALLEY' in constants.ENABLED_SCRAPERS:
		vs = ValleyScraper(conn)
		vs.execute()
        

if __name__ == "__main__":
        main()
