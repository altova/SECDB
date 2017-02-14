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

# Lists all filings that match the given criteria.
#
# Usage:
# 	raptorxmlxbrl script scripts/find_filings.py --company "FREDS INC" --type "10-K" feeds/xbrlrss-2015-04.xml

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import re, sys, os, json, time, datetime, argparse, concurrent.futures, urllib, glob

gsRootDir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-2])
gsRootURL = 'file://'+urllib.request.pathname2url(gsRootDir)+'/'

def load_rss_schema():
	filepath = urllib.parse.urljoin(gsRootURL,'xsd/rss.xsd')
	rss_schema, log = xsd.Schema.create_from_url(filepath)
	if not rss_schema:
		raise Exception('\n'.join([error.text for error in log]))
	return rss_schema

def load_rss_feed(filename,rss_schema):
	filepath = urllib.parse.urljoin(gsRootURL,filename)
	rss_feed, log = xml.Instance.create_from_url(filepath,schema=rss_schema)
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

def find_filings(file,rss_schema,args):
	rss_feed = load_rss_feed(urllib.request.pathname2url(file),rss_schema)

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
					if args.acc and args.acc != child_as_str(xbrlFiling,'accessionNumber'):
						continue
					if args.cik and args.cik != child_as_int(xbrlFiling,'cikNumber'):
						continue
					if args.sic and args.sic != child_as_int(xbrlFiling,'assignedSic'):
						continue

					filing = {}		
					filing['companyName'] = child_as_str(xbrlFiling,'companyName')
					filing['formType'] = child_as_str(xbrlFiling,'formType')
					filing['filingDate'] = child_as_str(xbrlFiling,'filingDate')
					filing['cikNumber'] = child_as_int(xbrlFiling,'cikNumber')
					filing['accessionNumber'] = child_as_str(xbrlFiling,'accessionNumber')
					filing['fileNumber'] = child_as_str(xbrlFiling,'fileNumber')
					filing['acceptanceDatetime'] = child_as_str(xbrlFiling,'acceptanceDatetime')
					filing['period'] = child_as_str(xbrlFiling,'period')
					filing['assistantDirector'] = child_as_str(xbrlFiling,'assistantDirector')
					filing['assignedSic'] = child_as_int(xbrlFiling,'assignedSic')
					filing['otherCikNumbers'] = child_as_str(xbrlFiling,'otherCikNumbers')
					filing['fiscalYearEnd'] = child_as_int(xbrlFiling,'fiscalYearEnd')
					instanceUrl = None
					xbrlFiles = xbrlFiling.find_child_element(('xbrlFiles','http://www.sec.gov/Archives/edgar'))
					for xbrlFile in xbrlFiles.element_children():
						if xbrlFile.find_attribute(('type','http://www.sec.gov/Archives/edgar')).normalized_value == 'EX-101.INS':
							url = xbrlFile.find_attribute(('url','http://www.sec.gov/Archives/edgar')).normalized_value
							instanceUrl = dir+'/'+filing['accessionNumber']+'-xbrl.zip%7Czip/'+url.split('/')[-1]
							break
					filing['instanceUrl'] = instanceUrl
					filings.append(filing)
	return filings

def parse_args():
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Searches EDGAR RSS feeds for filings matching the given criteria.')
	parser.add_argument('rss_feeds', metavar='RSS', nargs='+', help='EDGAR RSS feed file')
	parser.add_argument('--acc', help='Accession number')
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

def main():
	# Parse script arguments
	args = parse_args()	
	
	print('Searching EDGAR RSS feeds...',)
	feeds = []
	for arg in args.rss_feeds:
		for file in glob.glob(arg):
			feeds.append(file)
	
	filings = []
	rss_schema = load_rss_schema()
	with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads) as executor:
		futures = [executor.submit(find_filings,file,rss_schema,args) for file in feeds]		
		for future in concurrent.futures.as_completed(futures):
			try:
				result = future.result()
			except Exception as e:
				print(e)
			else:
				filings.extend(result)
	
	print(json.dumps(filings,sort_keys=True,indent=4,separators=(',',': ')))
	print('Found %d filings'%len(filings))
	
if __name__ == '__main__':
	start = time.clock()
	main()
	end = time.clock()
	print('Finished in ',end-start)