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

                sourcedict = {}
                for section in config.sections():
                        url = config.get(section, 'url', 0)
                        sourcedict[section] = url
                return sourcedict


        @abc.abstractmethod
        def execute(self):
                """Run scraper"""
                return

