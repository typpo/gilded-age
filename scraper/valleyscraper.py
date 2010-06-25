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
                targets = super(ValleyScraper, self).parseConfig(constants.VALLEY_CONFIG)
                links = []
                for key in targets:
                        c = self.extractLinks(targets[key], links)
                        print '\tExtracted', c, 'newspapers from', key
                        break

                print 'Found', len(links), 'newspapers total'
                
                for link, tag in links:
                        self.parse(link, tag)



        def extractLinks(self,base,links):
                """Extract all links that end in .xml"""
                src = urllib.urlopen(base).read()
                soup = BeautifulSoup(src)

                tags = soup.findAll('a', href=re.compile("\.xml$"))
                for tag in tags:
                        ref = tag['href']
			fullurl = urlparse.urljoin(base, ref)
			filename = ref[ref.rfind('/')+1:]
                        links.append((fullurl, filename))

                return len(tags)

        def parse(self,link,tag):
                """Parse and write article files.

		link -- full url to newspaper document.
		tag -- name of newspaper document, also the filename that's written.

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

		# Loop through pages
		for page in soup.findAll('p', 'title'):
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

                # Create XML tree.

		# TODO don't go if summary or text is null

                xml = minidom.Document()
                root = xml.createElement('ArticleRoot')
                xml.appendChild(root)

                meta = xml.createElement('meta')
                meta.appendChild(self.__createNode('newspaper','...'))
                meta.appendChild(self.__createNode('date','...'))
                meta.appendChild(self.__createNode('alignment','...'))

                data = xml.createElement('data')
                data.appendChild(self.__createNode('summary','...'))
                data.appendChild(self.__createNode('text','...'))

                root.appendChild(meta)
                root.appendChild(data)

                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
		self.__writeXml(path, xml)

		return True

        def __createNode(self,name,text):
		"""Create an XML node with text"""
                xml = minidom.Document()
                e = xml.createElement(name)
                e.appendChild(xml.createTextNode(text))
                return e

	def __writeXml(self, path, xml):
                """Write XML doc to file"""
		print 'Writing to', path
                dirpath = os.path.dirname(path)
                if not os.path.isdir(dirpath):
                        os.makedirs(dirpath)
                f = open(path, 'w')
                xml.writexml(f,'\t','\t','\n','UTF-8')
