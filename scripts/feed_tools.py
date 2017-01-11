# Copyright 2015 Altova GmbH
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#	  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__copyright__ = 'Copyright 2015 Altova GmbH'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# This module provides commonly used functionality to work with EDGAR RSS feeds.

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import re,datetime,os.path,urllib.request,urllib.error,glob,logging

# create logger
logger = logging.getLogger('default')

rss_schema = None
edgar_ns = 'http://www.sec.gov/Archives/edgar'

"""Returns the local directory representing the root directory."""
root_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-2])

"""Returns the local directory where all downloaded feeds will be stored."""
feed_dir = os.path.join(root_dir,'feeds')

"""Returns the local directory where all downloaded filings will be stored."""
filings_dir = os.path.join(root_dir,'filings')

class Feed:
	"""This class represents an EDGAR RSS feed."""

	def __init__(self,year,month):
		self._year = year
		self._month = month

	def __eq__(self, other):
		return self._year == other._year and self._month == other._month

	def __lt__(self, other):
		return self._year < other._year or (self._year == other._year and self._month < other._month)
	def __le__(self, other):
		return self._year < other._year or (self._year == other._year and self._month <= other._month)

	def copy(self):
		return Feed(self._year,self._month)

	def inc(self):
		self._month += 1
		if self._month > 12:
			self._month = 1
			self._year += 1
		
	def dec(self):
		self._month -= 1
		if self._month < 1:
			self._month = 12
			self._year -= 1
		
	@property
	def year(self):
		return self._year

	@property
	def month(self):
		return self._month
		
	@property
	def filename(self):
		"""Returns the filename of the EDGAR RSS feed."""
		return 'xbrlrss-%d-%02d.xml'%(self.year,self.month)
		
	@property
	def url(self):
		"""Returns the URL representing the EDGAR RSS feed."""
		return 'http://www.sec.gov/Archives/edgar/monthly/'+self.filename

	def download(self,dir):
		"""Downloads the EDGAR RSS feed from SEC and stores it into the given dir. Returns the full path to the downloaded file."""
		logger.info('Downloading RSS feed %s',self.url)
		filepath = os.path.join(dir,self.filename)
		urllib.request.urlretrieve(self.url,filepath)
		return filepath
		
def load_rss_schema():
	"""Returns an XML schema object of the RSS schema."""
	global rss_schema
	if rss_schema:
		return rss_schema

	filepath = os.path.join(root_dir,'xsd','rss.xsd')
	logger.info('Loading RSS schema %s',filepath)

	url = 'file:'+urllib.request.pathname2url(filepath)
	rss_schema, log = xsd.Schema.create_from_url(url)
	if not rss_schema:
		error = 'Failed loading RSS schema: %s' % '\n'.join([error.text for error in log])
		logger.critical(error)
		raise RuntimeError(error)		
	return rss_schema
	
def load_feed(filepath):
	"""Returns an XML instance object of the RSS feed."""
	rss_schema = load_rss_schema()	
	
	logger.info('Loading RSS feed %s',filepath)

	url = 'file:'+urllib.request.pathname2url(filepath)
	feed_instance, log = xml.Instance.create_from_url(url,schema=rss_schema)
	if not feed_instance:
		error = 'Failed loading RSS feed %s: %s' % (url,'\n'.join([error.text for error in log]))
		logger.error(error)
		raise RuntimeError(error)
	return feed_instance

def child_elem_as_str(elem,name):
	"""Returns the content of the child element as str."""
	child = elem.find_child_element((name,edgar_ns))
	if child:
		return str(child.schema_actual_value)
	return None

def child_elem_as_int(elem,name):
	"""Returns the content of the child element as int."""
	child = elem.find_child_element((name,edgar_ns))
	if child:
		return int(child.schema_actual_value)
	return None

def child_elem_as_date(elem,name,format):
	"""Returns the content of the child element as date."""
	child = elem.find_child_element((name,edgar_ns))
	if child:
		return datetime.datetime.strptime(str(child.schema_actual_value),format).date()
	return None
	
def child_elem_as_datetime(elem,name,format):
	"""Returns the content of the child element as datetime."""
	child = elem.find_child_element((name,edgar_ns))
	if child:
		return datetime.datetime.strptime(str(child.schema_actual_value),format)
	return None
	
def parse_feed(feed_instance):
	"""Parse the EDGAR meta information for each item/filing in the RSS feed and return a list of dict objects."""
	dir = 'filings/'+re.fullmatch(r'file:///.*/xbrlrss-(\d{4}-\d{2})\.xml',feed_instance.uri).group(1)

	filings = []
	rss = feed_instance.document_element
	for channel in rss.element_children():
		for item in channel.element_children():
			if item.local_name == 'item':
				filing = {}
			
				enclosure = item.find_child_element('enclosure')
				if enclosure:
					filing['enclosureUrl'] = enclosure.find_attribute('url').schema_normalized_value
					filing['enclosureLength'] = int(enclosure.find_attribute('length').schema_normalized_value)
				else:
					# fallback to value of <link> s/index.htm/xbrl.zip/
					link = item.find_child_element('link')
					if link:
						composed_enclosure = link.schema_normalized_value.replace('index.htm', 'xbrl.zip')
						filing['enclosureUrl'] = composed_enclosure
						filing['enclosureLength'] = None

				xbrlFiling = item.find_child_element(('xbrlFiling',edgar_ns))
				if xbrlFiling:
					filing['companyName'] = child_elem_as_str(xbrlFiling,'companyName')
					filing['formType'] = child_elem_as_str(xbrlFiling,'formType')
					filing['filingDate'] = child_elem_as_date(xbrlFiling,'filingDate','%m/%d/%Y')
					filing['cikNumber'] = child_elem_as_int(xbrlFiling,'cikNumber')
					filing['accessionNumber'] = child_elem_as_str(xbrlFiling,'accessionNumber')
					filing['fileNumber'] = child_elem_as_str(xbrlFiling,'fileNumber')
					filing['acceptanceDatetime'] = child_elem_as_datetime(xbrlFiling,'acceptanceDatetime','%Y%m%d%H%M%S')
					filing['period'] = child_elem_as_date(xbrlFiling,'period','%Y%m%d')
					filing['assistantDirector'] = child_elem_as_str(xbrlFiling,'assistantDirector')
					filing['assignedSic'] = child_elem_as_int(xbrlFiling,'assignedSic')
					filing['otherCikNumbers'] = child_elem_as_str(xbrlFiling,'otherCikNumbers')
					filing['fiscalYearEnd'] = child_elem_as_int(xbrlFiling,'fiscalYearEnd')
					instanceUrl = None
					xbrlFiles = xbrlFiling.find_child_element(('xbrlFiles',edgar_ns))
					for xbrlFile in xbrlFiles.element_children():
						if xbrlFile.find_attribute(('type',edgar_ns)).normalized_value == 'EX-101.INS':
							url = xbrlFile.find_attribute(('url',edgar_ns)).normalized_value
							instanceUrl = dir+'/'+filing['accessionNumber']+'-xbrl.zip%7Czip/'+url.split('/')[-1]
							break
					filing['instanceUrl'] = instanceUrl
				filings.append(filing)
	return filings
	
def read_feed(filepath):
	"""Return a list of dict objects with EDGAR meta information for each filing in the RSS feed."""
	feed = load_feed(filepath)
	return parse_feed(feed)
	
def read_feeds(filepaths):
	"""Return a list of dict objects with EDGAR meta information for each filing in the RSS feeds."""
	filings = []
	for filepath in filepaths:
		filings.append(read_feed(filepath))
	return filings
