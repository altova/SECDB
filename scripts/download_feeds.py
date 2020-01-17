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

# Downloads EDGAR RSS feed files for all months and years from the SEC archive (skips already downloaded RSS feeds).
#
# Usage:
# 	raptorxmlxbrl script scripts/download_feeds.py

import feed_tools
import os.path,urllib.error,time,datetime,logging,argparse

feed_dir = feed_tools.feed_dir

def download_feed(feed):
	"""Returns the absolute file path to the downloaded EDGAR RSS feed."""
	try:
		filepath = feed.download(feed_dir)
	except urllib.error.HTTPError as e:
		logger.exception('Failed downloading RSS feed %s',feed.url)
		# Re-throw any other HTTP errors
		raise
	return filepath

def download_feeds(args=None):
	"""Returns a list absolute file paths to any new or missing EDGAR RSS feeds that were downloaded."""
	feeds = []

	if args and args.month:
	
		feed = feed_tools.Feed(*(int(i) for i in args.month.split('-')))
		feeds.append(download_feed(feed))
		
	else:

		# Download any missing or new RSS feeds from 2010 until now
		start_from = args.start_from if args else '2010-01'
		feed_start = feed_tools.Feed(*(int(i) for i in start_from.split('-')))
		today = datetime.date.today()
		current_feed = feed_tools.Feed(today.year,today.month)

		feed = feed_start
		while feed <= current_feed:
			if not os.path.exists(os.path.join(feed_dir,feed.filename)):
				if not feeds and feed != feed_start:
					# Always reload the last available RSS feed
					last_feed = feed.copy()
					last_feed.dec()
					feeds.append(download_feed(last_feed))
				feeds.append(download_feed(feed))
			feed.inc()

		# Always reload the last available RSS feed
		if not feeds:
			feeds.append(download_feed(current_feed))

	return feeds

def parse_args():
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Downloads EDGAR RSS feed files for all months and years from the SEC archive (skips already downloaded RSS feeds)')
	parser.add_argument('--log', metavar='LOGFILE', dest='log_file', help='specify output log file')
	parser.add_argument('--month', metavar='YYYY-MM', help='download EDGAR RSS feed only for the given month (in YYYY-MM format)')
	parser.add_argument('--from', metavar='YYYY-MM', dest='start_from', default='2010-01', help='download EDGAR RSS feeds starting from the given month (in YYYY-MM format)')
	return parser.parse_args()

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

	download_feeds(args)

if __name__ == '__main__':
	start = time.perf_counter()
	main()
	end = time.perf_counter()
	print('Finished in ',end-start)
else:
	# create logger
	logger = logging.getLogger('default')
