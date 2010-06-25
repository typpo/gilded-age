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

		# Extract date
		strdate = re.match('^.*: ([^<]+)', str(sectionhead)).group(1)
		article_date = time.strptime(strdate, '%B %d, %Y')
		print '\tDated', strdate

                # Create XML tree.
                xml = minidom.Document()
                root = xml.createElement('ArticleRoot')
                xml.appendChild(root)

		# Add metadata
                meta = xml.createElement('meta')
                meta.appendChild(super(ValleyScraper, self).__createNode('newspaper', paper))
                meta.appendChild(super(ValleyScraper, self).__createNode('alignment', source['alignment']))
                meta.appendChild(super(ValleyScraper, self).__createNode('date', article_date))

		# Create articles node
		articles = xml.createElement('articles')

		# Loop through pages
		for page in soup.findAll('p', 'title'):
			# Parse article
			# TODO parse multiple articles per page
			strpageno = re.match('\D+(\d+)', str(page)).group(1)
			article_pageno = int(strpageno)
			print '\tPage', article_pageno

			# Walk until we find a summary
			summary = page.findNext('blockquote', text=re.compile('Summary'))
			if summary is None:
				article_summary = None
			else:
				summary = summary.next
				article_summary = re.sub('(\<br\s*/?\>|\n|\s{2,})', '', summary)

			if article_summary is None:
				return False

			# Look for full text associated with summary
			# TODO do all articles have a summary?
			text = page.findNext('blockquote', text=re.compile('.*(Full Text).*(Summary)?'))
			if text is None:
				article_text = None
			else:
				article_text = str(''.join(text.findNext('p').contents))
				article_text = re.sub('(\<br\s*/?\>|\n|\s{2,})', '', article_text)
				print article_text

			# Add article to xml tree
			article = xml.createElement('article')
			article.appendChild(super(ValleyScraper, self).__createNode('page', article_pageno))
			article.appendChild(super(ValleyScraper, self).__createNode('summary', article_summary))
			article.appendChild(super(ValleyScraper, self).__createNode('text', article_text))
			articles.appendChild(article)


		# Finish off XML tree
                root.appendChild(meta)
                root.appendChild(articles)

		# And write it to file
                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
		super(ValleyScraper, self).__writeXml(path, xml)

		return True
