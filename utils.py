import unicodedata

def asciiDamnit(text):
    """Forces conversion to ascii"""
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')
