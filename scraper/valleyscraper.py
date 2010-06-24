from BeautifulSoup import BeautifulSoup
from ConfigParser import ConfigParser
from basescraper import BaseScraper
import constants
import abc
import re
import os
import urllib
import urlparse
from xml.dom import minidom

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
                        break



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
		"""

                # Parse.

                # Create XML tree.
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

                # write to file (for testing)
                path = os.path.join(constants.BASE_DIR, \
                        constants.VALLEY_DIR, tag)
                dirpath = os.path.dirname(path)
                if not os.path.isdir(dirpath):
                        os.makedirs(dirpath)
                f = open(path, 'w')
                xml.writexml(f,'\t','\t','\n','UTF-8')

        def __createNode(self,name,text):
		"""Create an XML node with text"""
                xml = minidom.Document()
                e = xml.createElement(name)
                e.appendChild(xml.createTextNode(text))
                return e
