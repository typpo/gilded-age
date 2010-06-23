from BeautifulSoup import BeautifulSoup
from basescraper import BaseScraper
import abc
import re
import urllib
import urlparse

CONFIG_PATH = 'scraper/valley.cfg'

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
                targets = super(ValleyScraper, self).parseConfig(CONFIG_PATH)
                links = []
                for key in targets:
                        c = self.extractLinks(targets[key], links)
                        print '\tExtracted', c, 'newspapers from', key

                print 'Found', len(links), 'newspapers total'

        def extractLinks(self,base,links):
                """Extract all links that end in .xml"""
                src = urllib.urlopen(base).read()
                soup = BeautifulSoup(src)

                c = 0
                for tag in soup.findAll('a', href=re.compile("\.xml$")):
                        ref = tag['href']
                        links.append(urlparse.urljoin(base, ref))
                        c+=1
                return c
