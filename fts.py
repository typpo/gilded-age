import db.articles

def populateFts(conn):
    """Populates FTS table with contents of articles table."""
    cur = conn.cursor()
    cur.execute('SELECT * FROM articles')
    rs = cur.fetchall()
    articles = db.articles.processAll(rs)

    for a in articles:
        stmt = 'INSERT INTO articles_fts VALUES (null, ?, ?, ?, ?, ?, ?, ?, datetime(?), datetime(?))'
        args = (a.id, a.source, a.alignment, a.page, a.title, a.summary, a.text, a.url, a.articleDate, a.timeEnter)
        cur.execute(stmt, args)
