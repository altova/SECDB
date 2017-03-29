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

# Downloads all filings contained in the given EDGAR RSS feed from the SEC archive (skips already existing local files).
#
# Usage:
# 	raptorxmlxbrl script scripts/download_filings.py feeds/xbrlrss-2015-05.xml

import feed_tools
import sys,re,time,os.path,urllib.request,urllib.error,glob,logging,argparse,concurrent.futures

def exists_filing(dir, url, length):
	"""Returns True if the filing already has been downloaded."""
	filepath = os.path.join(dir,url.split('/')[-1])
	return os.path.exists(filepath) and (length is None or os.path.getsize(filepath) == length)

def download_filing(dir, url, max_retries=3):
	"""Download filing at url and store within the given dir."""

	filepath = os.path.join(dir,url.split('/')[-1])
	while max_retries > 0:
		try:
			logger.info('Downloading filing %s',url)
			urllib.request.urlretrieve(url,filepath)
		except OSError:
			logger.info('Retry downloading filing %s',url)
			max_retries -= 1
			time.sleep(3)
		else:
			logger.info('Succeeded downloading filing %s',url)
			return
	logger.exception('Failed downloading filing %s',url)

def filings_dir(feedpath):
	"""Returns the absolute directory path where filings for this feed will be stored."""
	subdir = re.fullmatch(r'.*xbrlrss-(\d{4}-\d{2})\.xml',os.path.basename(feedpath)).group(1)
	return os.path.join(feed_tools.filings_dir,subdir)

def download_filings(feedpath,args=None):
	"""Go through all entries in the given EDGAR RSS feed and download any missing or new filings."""
	logger.info("Processing RSS feed %s",feedpath)

	dir = filings_dir(feedpath)
	os.makedirs(dir,exist_ok=True)

	filing_urls = []
	for filing in feed_tools.read_feed(feedpath):
		if args:
			if args.company_re and not bool(args.company_re.match(filing['companyName'])):
				continue
			if args.cik and args.cik != filing['cikNumber']:
				continue
			if args.sic and args.sic != filing['assignedSic']:
				continue
			if args.form_type and args.form_type != filing['formType']:
				continue
		if 'enclosureUrl' in filing and not exists_filing(dir,filing['enclosureUrl'],filing['enclosureLength']):
			filing_urls.append(filing['enclosureUrl'])
		if args and args.with_exhibits:
			filing_urls.extend( filing.get( 'exhibitList', [] ) )

	logger.info("Start downloading %d new filings",len(filing_urls))
	with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads) as executor:
		futures = [executor.submit(download_filing,dir,url,args.max_retries) for url in filing_urls]
		for future in concurrent.futures.as_completed(futures):
			try:
				future.result()
			except Exception as e:
				print(e)

def collect_feeds(args):
	"""Returns an generator of the resolved, absolute RSS file paths."""
	for arg in args:
		for feedpath in glob.iglob(os.path.abspath(arg)):
			yield feedpath

def parse_args():
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Downloads all filings contained in the given EDGAR RSS feed from the SEC archive (skips already existing local files).')
	parser.add_argument('rss_feeds', metavar='RSS', nargs='+', help='EDGAR RSS feed file')
	parser.add_argument('--log', metavar='LOGFILE', dest='log_file', help='specify output log file')
	parser.add_argument('--cik', help='CIK number')
	parser.add_argument('--sic', help='SIC number')
	parser.add_argument('--form-type', help='Form type (10-K,10-Q,...)')
	parser.add_argument('--company', help='Company name')
	parser.add_argument('--threads', type=int, default=8, dest='max_threads', help='specify max number of threads')
	parser.add_argument('--retries', type=int, default=3, dest='max_retries', help='specify max number of retries to download a specific filing')
	parser.add_argument('--with-exhibits', action='store_true', help='download exhibits also')
	args = parser.parse_args()
	args.company_re = re.compile(args.company, re.I) if args.company else None
	if args.cik:
		args.cik = int(args.cik)
	if args.sic:
		args.sic = int(args.sic)
	return args

def setup_logging(log_file):
	"""Setup the Python logging infrastructure."""
	global logger
	if log_file:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=log_file,filemode='w',level=logging.INFO)
	else:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.INFO)
	logger = logging.getLogger('default')

def main():
	# Parse script arguments
	args = parse_args()
	# Setup python logging framework
	setup_logging(args.log_file)

	for feedpath in collect_feeds(args.rss_feeds):
		download_filings(feedpath,args)

if __name__ == '__main__':
	start = time.clock()
	main()
	end = time.clock()
	print('Finished in ',end-start)
else:
	# create logger
	logger = logging.getLogger('default')
	