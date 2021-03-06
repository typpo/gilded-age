import db.articles
"""Utilty for populating an FTS index for articles"""

def createFts(conn):
    """Creates fts table for articles using porter stemming algorithm"""
    conn.cursor().execute("""
           CREATE VIRTUAL TABLE articles_fts
                USING fts3
                (id INTEGER PRIMARY KEY,
                source TEXT,
                alignment TEXT,
                page INTEGER,
                title TEXT,
                summary TEXT,
                text TEXT,
                url TEXT,
                articleDate DATE,
                timeEnter DATE,
                tokenize=porter)
            """)

def populateFts(conn):
    """Populates FTS table with contents of a normal articles table."""
    cur = conn.cursor()
    cur.execute('SELECT * FROM articles')
    rs = cur.fetchall()
    articles = db.articles.processAll(rs)

    print 'Populating database with', len(articles), 'articles'
    for a in articles:
        stmt = 'INSERT INTO articles_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime(?), datetime(?))'
        args = (a.id, a.source, a.alignment, a.page, a.title, a.summary, \
                a.text, a.url, a.articleDate, a.timeEnter)
        cur.execute(stmt, args)

    conn.commit()
    print 'Done.'

def deleteFts(conn):
    conn.cursor().execute('DROP TABLE articles_fts')
