import os
import abc
from xml.dom import minidom
from ConfigParser import ConfigParser

class BaseScraper():
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

        def toDb(self, args):
                execute = "INSERT INTO articles VALUES (null, ?, ?, ?, ?, ?, ?, ?, datetime(?), datetime('now','localtime'))"

                self.cur.execute(execute, args)
                self.conn.commit()

        def createTextNode(self,name,text):
		"""Create an XML node containing text"""
                xml = minidom.Document()
                e = xml.createElement(name)
                e.appendChild(xml.createTextNode(unicode(text)))
                return e

	def writeXml(self, path, xml):
                """Write XML doc to file"""
                dirpath = os.path.dirname(path)
                if not os.path.isdir(dirpath):
                        os.makedirs(dirpath)
                f = open(path, 'w')
                xml.writexml(f,'\t','\t','\n', 'utf-8')
