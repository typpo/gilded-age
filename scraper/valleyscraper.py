from BeautifulSoup import BeautifulSoup
from ConfigParser import ConfigParser
from basescraper import BaseScraper
from xml.dom import minidom
import constants
import abc
import re
import os
import time
import urllib
import urlparse

class ValleyScraper(BaseScraper):
        """Class for scraping Valley of the Shadow Project
        valley.lib.virginia.edu/
        """
        def __init__(self, conn):
		"""Initialize with database connection"""
                super(ValleyScraper, self).__init__(conn)
                print 'Initializing ValleyScraper...'

        def execute(self):
                """Loads urls in config files"""
                sources = super(ValleyScraper, self).parseConfig(constants.VALLEY_CONFIG)
                for source in sources:
			c, extracted = self.extractLinks(source['url'])
			for link, tag in extracted:
				self.parse(link, tag, source)

                print 'Found', len(links), 'newspapers total'

        def extractLinks(self,base):
                """Extract all links that end in .xml"""
                src = urllib.urlopen(base).read()
                soup = BeautifulSoup(src)

                tags = soup.findAll('a', href=re.compile("\.xml$"))
		links = []
                for tag in tags:
                        ref = tag['href']
			fullurl = urlparse.urljoin(base, ref)
			filename = ref[ref.rfind('/')+1:]
                        links.append((fullurl, filename))

                return len(tags), links

        def parse(self,link,tag,source):
                """Parse and write article files.

		link -- full url to newspaper document.
		tag -- name of newspaper document, also the filename that's written.
		source -- dictionary representing the config settings for this source.

		return -- True if successful, False otherwise
		"""

                # Parse.
		print 'Loading', link
                src = urllib.urlopen(link).read()
                soup = BeautifulSoup(src)

		# Parse date
		# Formatted:
		# Newspaper Name: August 5, 1859

		# Search for section-head class
		sectionhead = soup.find('h2', 'section-head')
		if sectionhead is None:
			# Bad page
			return False

		# Extract date
		article_strdate = re.match('^.*: ([^<]+)', str(sectionhead)).group(1)
		article_date = time.strptime(article_strdate, '%B %d, %Y')
		print '\tDated', article_strdate

                # Create XML tree.
                xml = minidom.Document()
                root = xml.createElement('ArticleRoot')
                xml.appendChild(root)

		# Add metadata
                meta = xml.createElement('meta')
                meta.appendChild(super(ValleyScraper, self).createTextNode('newspaper', source['name']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('alignment', source['alignment']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('date', article_strdate))

		# Create articles node
		articles = xml.createElement('articles')

		# Loop through pages
		for page in soup.findAll('p', 'title'):
			pageno, summary, text = self.__parsePage(page)
			if pageno is None:
				# Bad page
				continue

			# Add article to xml tree
			article = xml.createElement('article')
			article.appendChild(super(ValleyScraper, self) \
				.createTextNode('page', str(pageno)))
			article.appendChild(super(ValleyScraper, self) \
				.createTextNode('summary', summary))
			article.appendChild(super(ValleyScraper, self) \
				.createTextNode('text', str(text)))
			articles.appendChild(article)


		# Finish off XML tree
                root.appendChild(meta)
                root.appendChild(articles)

		# And write it to file
                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
		super(ValleyScraper, self).writeXml(path, xml)

		return True

	def __parsePage(self, page):
		"""Parses articles from a page"""

		# TODO parse multiple articles per page

		# Grab page number - (probably not necessary, but sometimes pages are skipped)
		strpageno = re.match('\D+(\d+)', str(page)).group(1)
		pageno = int(strpageno)
		print '\tPage', pageno

		# Walk until we find a summary
		summary = page.findNext('blockquote', text=re.compile('Summary'))
		if summary is None:
			summary = None
		else:
			summary = summary.next
			summary = re.sub('(\<br\s*/?\>|\n|\s{2,})', '', summary)

		if summary is None:
			return None, None, None

		# Look for full text associated with summary
		# TODO do all articles have a summary?
		text = page.findNext('blockquote', text=re.compile('.*(Full Text).*(Summary)?'))
		if text is None:
			text = None
		else:
			# Join the contents as strings
			# TODO remove line breaks here?
			text = ''.join(map(lambda x: str(x), text.findNext('p').contents))
			text = re.sub('(\<br\s*/?\>|\n|\s{2,})', '', text)

		return pageno, summary, text
