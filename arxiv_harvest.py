import urllib2
import time
import xml.etree.ElementTree as ET
from datetime import datetime

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"

def harvest(initial_date, end_date):
    results = []
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = "{0}from={1}&until={2}&metadataPrefix=arXiv".format(base_url, initial_date, end_date)
    
    while True:

        print "fetching: {0}".format(url)
        try:
            response = urllib2.urlopen(url)
            
        except urllib2.HTTPError, e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print "Got 503. Retrying after {0:d} seconds.".format(to)

                time.sleep(to)
                continue
                
            else:
                raise
            
        xml = response.read()
        root = ET.fromstring(xml)
        
        for record in root.find(OAI+'ListRecords').findall(OAI+"record"):
            arxiv_id = record.find(OAI+'header').find(OAI+'identifier')
            meta = record.find(OAI+'metadata')
            info = meta.find(ARXIV+"arXiv")
            created = info.find(ARXIV+"created").text
            created = datetime.strptime(created, "%Y-%m-%d")
            categories = info.find(ARXIV+"categories").text
            doi = info.find(ARXIV+"doi")
            if doi is not None:
                doi = doi.text.split()[0]
                
            contents = {'title': info.find(ARXIV+"title").text,
                        'id': info.find(ARXIV+"id").text,
                        'abstract': info.find(ARXIV+"abstract").text.strip(),
                        'created': created,
                        'categories': categories.split(),
                        'doi': doi,
                        }
            results.append(contents)

        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            break

        else:
            url = base_url + "resumptionToken=%s"%(token.text)
                   
    return results

records = harvest('2015-01-01', '2015-01-10')
print records
print "Got {0} records".format(len(records))
