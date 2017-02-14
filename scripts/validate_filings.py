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

# Validates all SEC filings in the given RSS feed
#
# Usage:
# 	raptorxmlxbrl script scripts/validate_filings.py feeds/xbrlrss-2015-04.xml

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl
import re,sys,os.path,time,concurrent.futures,urllib,glob,logging,argparse

gsRootDir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-2])
gsRootURL = 'file://'+urllib.request.pathname2url(gsRootDir)+'/'

def load_rss_schema():
	rss_schema, log = xsd.Schema.create_from_url(urllib.parse.urljoin(gsRootURL,'xsd/rss.xsd'))
	if not rss_schema:
		raise Exception('\n'.join([error.text for error in log]))
	return rss_schema
	
def load_rss_feed(url,schema):
	rss_feed, log = xml.Instance.create_from_url(urllib.parse.urljoin(gsRootURL,url),schema=schema)
	if not rss_feed:
		raise Exception('\n'.join([error.text for error in log]))
	return rss_feed

def child_as_str(elem,name):
	child = elem.find_child_element((name,'http://www.sec.gov/Archives/edgar'))
	if child:
		return str(child.schema_actual_value)
	return None

def child_as_int(elem,name):
	child = elem.find_child_element((name,'http://www.sec.gov/Archives/edgar'))
	if child:
		return int(child.schema_actual_value)
	return None
	
def parse_rss_feed(rss_feed,args):
	dir = 'filings/'+re.fullmatch(r'file:///.*/xbrlrss-(\d{4}-\d{2})\.xml',rss_feed.uri).group(1)

	filings = []
	rss = rss_feed.document_element
	for channel in rss.element_children():
		for item in channel.element_children():
			if item.local_name == 'item':
				xbrlFiling = item.find_child_element(('xbrlFiling','http://www.sec.gov/Archives/edgar'))
				if xbrlFiling:
					if args.company_re and not bool(args.company_re.match(child_as_str(xbrlFiling,'companyName'))):
						continue
					if args.form_type and args.form_type != child_as_str(xbrlFiling,'formType'):
						continue
					if args.cik and args.cik != child_as_int(xbrlFiling,'cikNumber'):
						continue
					if args.sic and args.sic != child_as_int(xbrlFiling,'assignedSic'):
						continue
				
					accessionNumber = xbrlFiling.find_child_element(('accessionNumber','http://www.sec.gov/Archives/edgar')).schema_normalized_value
					xbrlFiles = xbrlFiling.find_child_element(('xbrlFiles','http://www.sec.gov/Archives/edgar'))
					for xbrlFile in xbrlFiles.element_children():
						if xbrlFile.find_attribute(('type','http://www.sec.gov/Archives/edgar')).schema_normalized_value == 'EX-101.INS':
							url = xbrlFile.find_attribute(('url','http://www.sec.gov/Archives/edgar')).schema_normalized_value
							filings.append(dir+'/'+accessionNumber+'-xbrl.zip%7Czip/'+url.split('/')[-1])
	return filings

def validate(url):
	instance, log = xbrl.Instance.create_from_url(urllib.parse.urljoin(gsRootURL,url))
	if not instance or log.has_errors():		
		errors = list(log.errors)
		logger.error('Filing %s has %d ERRORS!',url,len(errors))
		if logging.getLogger().isEnabledFor(logging.DEBUG):
			logger.log(logging.DEBUG,'\n'.join([error.text for error in log]))
		return False
	if log.has_inconsistencies():
		inconsistencies = list(log.inconsistencies)
		logger.warning('Filing %s has %d INCONSISTENCIES!',url,len(inconsistencies))
		if logging.getLogger().isEnabledFor(logging.DEBUG):
			logger.log(logging.DEBUG,'\n'.join([error.text for error in inconsistencies]))
	else:
		logger.info('Filing %s is VALID!',url)
	return True
	
def parse_args():
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Validates all filings contained in the given EDGAR RSS feed from the SEC archive.')
	parser.add_argument('rss_feeds', metavar='RSS', nargs='+', help='EDGAR RSS feed file')
	parser.add_argument('--log', metavar='LOGFILE', dest='log_file', help='specify output log file')
	parser.add_argument('--log-level', type=int, default=logging.INFO, help='specify min. log level (use 10 to enable detailed error messages)')
	parser.add_argument('--cik', help='CIK number')
	parser.add_argument('--sic', help='SIC number')
	parser.add_argument('--form-type', help='Form type (10-K,10-Q,...)')
	parser.add_argument('--company', help='Company name')
	parser.add_argument('--threads', type=int, default=8, dest='max_threads', help='specify max number of threads')
	args = parser.parse_args()
	args.company_re = re.compile(args.company, re.I) if args.company else None
	if args.cik:
		args.cik = int(args.cik)
	if args.sic:
		args.sic = int(args.sic)
	return args
	
def setup_logging(args):
	"""Setup the Python logging infrastructure."""
	global logger
	if args.log_file:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=args.log_file,filemode='w',level=args.log_level)
	else:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=args.log_level)
	logger = logging.getLogger('default')
	
def main():
	# Parse script arguments
	args = parse_args()	
	# Setup python logging framework
	setup_logging(args)

	rss_schema = load_rss_schema()
	for arg in sys.argv[1:]:
		for file in glob.glob(arg):
			logger.info('Loading rss file %s',file)
			rss_feed = load_rss_feed(urllib.request.pathname2url(file),rss_schema)
			filings = parse_rss_feed(rss_feed,args)
			
			logger.info('Processing %d filings...',len(filings))
			with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads) as executor:
				futures = [executor.submit(validate,url) for url in filings]
				for future in concurrent.futures.as_completed(futures):
					future.result()

if __name__ == '__main__':
	start = time.clock()
	main()
	end = time.clock()
	print('Finished in ',end-start)