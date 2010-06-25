import abc
from ConfigParser import ConfigParser

class BaseScraper(object):
        __metaclass__ = abc.ABCMeta

        def __init__(self,conn):
                """Initializes with a database connection"""
                self.conn = conn
                self.cur = conn.cursor()

        def addArticle(self,date,title,data):
                """Adds article to db"""
                return False

        def parseConfig(self,path):
                """Loads configuration file containing data source"""
                config = ConfigParser()
                config.readfp(open(path))
                config.read

                sources = []
                for section in config.sections():
                        sectiondict = {}
			for key, val in config.items(section):
				sectiondict[key] = val
			sources.append(sectiondict)
                return sources


        @abc.abstractmethod
        def execute(self):
                """Run scraper"""
                return

        def __createTextNode(self,name,text):
		"""Create an XML node containing text"""
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
