from BeautifulSoup import BeautifulStoneSoup
from BeautifulSoup import BeautifulSoup
from basescraper import BaseScraper
from utils import asciiDamnit
from xml.dom import minidom
import constants
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
                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
                if os.path.exists(path):
                        # Already parsed
                        return False

                # Parse.
		print 'Loading', link
                try:
                        src = urllib.urlopen(link).read()
                except IOError:
                        return False

                soup = BeautifulStoneSoup(src, \
                        convertEntities=BeautifulStoneSoup.HTML_ENTITIES)

		# Search for section-head class
		sectionhead = soup.find('h2', 'section-head')
		if sectionhead is None:
			# Bad page
			return False

		# Extract date, formatted: 'Newspaper Name: August 5, 1859'
                try:
                        strdate = re.match('^.*?:\s+(.*?\s\d{1,2},\s+\d{4})', \
                                str(sectionhead)).group(1)
                        try:
                                date = time.strptime(strdate, '%B %d, %Y')
                        except ValueError:
                                # month # instead of name
                                        date = time.strptime(strdate, '%m %d, %Y')
                except:
                        print 'Couldn\'t parse date'
                        strdate = raw_input('Enter date (mm/dd/yyyy) for: %s\n> ' \
                                % (sectionhead.contents[0]))
                        date = time.strptime(strdate, '%m/%d/%Y')

                        

                strdate = '%d-%02d-%02d' % \
                        (date.tm_year, date.tm_mon, date.tm_mday)

                # Create XML tree.
                xml = minidom.Document()
                root = xml.createElement('ArticleRoot')
                xml.appendChild(root)

		# Add metadata
                meta = xml.createElement('meta')
                meta.appendChild(super(ValleyScraper, self).createTextNode('newspaper', source['name']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('alignment', source['alignment']))
                meta.appendChild(super(ValleyScraper, self).createTextNode('date', strdate))
                meta.appendChild(super(ValleyScraper, self).createTextNode('url', link))
		
                # Create articles node
		articles = xml.createElement('articles')

		# Loop through pages
                pageno = 0 
                for page in src.split('<p class="title">')[1:]:
                        pageno += 1
                        
			extracted_articles = self.__parsePage(page)
			if pageno is -1 or extracted_articles is None:
				# Bad page
				continue

			for title, summary, text in extracted_articles:
                                # Add article to xml tree
				article = xml.createElement('article')
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('page', str(pageno)))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('title', title))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('summary', summary))
				article.appendChild(super(ValleyScraper, self) \
					.createTextNode('text', text))
				articles.appendChild(article)

                                # Write to db
                                args = (source['name'], \
                                     source['alignment'], \
                                     pageno, \
                                     title, \
                                     summary, \
                                     text, \
                                     link, \
                                     strdate,)

                                super(ValleyScraper, self).toDb(args)

		# Finish off XML tree
                root.appendChild(meta)
                root.appendChild(articles)

		# Write to file
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
                        summary_text = ''.join(map(lambda part: unicode(part), summary_next))
                        summary_text = re.sub('(\<br\s*/?\>|\s{2,})', ' ', summary_text)

                        # Look for full text associated with summary
                        full = summary_next.findNext('blockquote', text=re.compile('(Summary|Full Text)'))
                        if full is None or unicode(full).find('Summary') > -1:
                                full_text = None
                        else:
                                # Get paragraphs
                                paras = full.findAllNext('p')

                                # Flatten paragraph list and convert everything to a string
                                # TODO preserve paragraph breaks
                                full_text = ' '.join(
                                        map(lambda part: unicode(part),
                                            (part for para in paras for part in para)
                                        ))

                                # Clean it up
                                full_text = re.sub('(\<br\s*/?\>|\s{2,})', ' ', full_text)

                        # Make sure it's ASCII
                        title_text = asciiDamnit(unicode(title_text))
                        summary_text = asciiDamnit(unicode(summary_text))
                        full_text = 'None' if full_text is None else \
                                asciiDamnit(unicode(full_text))
                        returnvals.append((title_text, summary_text, full_text))
                        articlecount += 1

                return returnvals
