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
		total = 0
                for source in sources:
			c, extracted = self.extractLinks(source['url'])
			if c == -1:
				continue
			total += c
			for link, tag in extracted:
				self.parse(link, tag, source)

                print 'Found', total, 'newspapers total'

        def extractLinks(self,base):
                """Extract all links that end in .xml"""
		try:
			src = urllib.urlopen(base).read()
		except:
			print 'Unable to read', base
			return -1, None

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

		# Search for section-head class
		sectionhead = soup.find('h2', 'section-head')
		if sectionhead is None:
			# Bad page
			return False

		# Extract date, formatted: 'Newspaper Name: August 5, 1859'
		article_strdate = re.match('^.*: ([^<]+)', str(sectionhead)).group(1)
		article_date = time.strptime(article_strdate, '%B %d, %Y')

                # Create XML tree.
                xml = minidom.Document()
                root = xml.createElement('ArticleRoot')
                xml.appendChild(root)

		# Add metadata
                meta = xml.createElement('meta')
                meta.appendChild(super(ValleyScraper, self).createTextNode('newspaper', source['name']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('alignment', source['alignment']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('date', article_strdate))
                meta.appendChild(super(ValleyScraper, self).createTextNode('url', link))
		
                # Create articles node
		articles = xml.createElement('articles')

		# Loop through pages
                pageno = 0 
                for page in src.split('<p class="title">')[1:]:
                        pageno += 1
                        print '\tPage', pageno,
                        
			extracted_articles = self.__parsePage(page)
			if pageno is -1 or extracted_articles is None:
				# Bad page
				continue

			# Add article to xml tree
			for title, summary, text in extracted_articles:
				article = xml.createElement('article')
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('page', str(pageno)))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('title', title))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('summary', summary))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('text', str(text)))
				articles.appendChild(article)

                                # Write to db
                                args = (source['name'], \
                                     source['alignment'], \
                                     pageno, \
                                     title, \
                                     summary, \
                                     text, \
                                     link, \
                                     article_strdate,)

                                super(ValleyScraper, self).toDb(args)
                print


		# Finish off XML tree
                root.appendChild(meta)
                root.appendChild(articles)

		# Write to file
                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
		super(ValleyScraper, self).writeXml(path, xml)

		return True

	def __parsePage(self, pagesrc):
		"""Parses articles from a page"""
                # TODO verify that all articles have a summary?

		# Set up return list - holds summary and article text for each article
		returnvals = []

                # Walk until we find a summary
                page = BeautifulSoup(pagesrc)
                summaries = page.findAll('blockquote', text=re.compile('Summary'))
                if summaries is None or len(summaries) < 1:
                        return None

                # Loop through article summaries and record them
                articlecount = 1 
                for summary in summaries:
                        # Get title
                        title = summary.findPrevious('b')
                        title_text = title.contents[0]

                        # Grab summary text
                        summary_next = summary.next
                        summary_text = re.sub('(\<br\s*/?\>|\n|\s{2,})', '', summary_next)

                        # Look for full text associated with summary
                        full = summary_next.findNext('blockquote', text=re.compile('(Summary|Full Text)'))
                        if full is None or str(full).find('Summary') > -1:
                                full_text = None
                        else:
                                # Get paragraphs
                                paras = full.findAllNext('p')

                                # Flatten paragraph list and convert everything to a string
                                # TODO preserve paragraph breaks
                                full_text = ' '.join(
                                        map(lambda part: str(part),
                                            (part for para in paras for part in para)
                                        ))

                                # Clean it up
                                full_text = re.sub('(\<br\s*/?\>|\s{2,})', ' ', full_text)

                        returnvals.append((title_text, summary_text, full_text))
                        articlecount += 1

                return returnvals
