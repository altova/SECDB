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

# Generates XMLSpy .spp project file from EDGAR RSS feed for a given month.
#
# Usage:
# 	raptorxmlxbrl script scripts/generate_xmlspy_project.py feeds/xbrlrss-2015-04.xml

import feed_tools
import sys,re,time,os.path,urllib.request,glob,argparse

def collect_feeds(args):
	"""Returns an generator of the resolved, absolute RSS file paths."""
	for arg in args:
		for feedpath in glob.iglob(os.path.abspath(arg)):
			yield feedpath
	
def generate_project(feedpath):
	filings = feed_tools.read_feed(feedpath)
	filings_by_company = {}
	for filing in filings:
		filings_by_company.setdefault(filing['companyName'],[]).append(filing)

	month = re.fullmatch(r'.*xbrlrss-(\d{4}-\d{2})\.xml',os.path.basename(feedpath)).group(1)
	dir = os.path.join(feed_tools.filings_dir,month)
	file = os.path.join(dir,'%s.spp'%month)
	print('Generating project file',file)
	with open(file,'w') as f:
		f.write("""\
<?xml version="1.0" encoding="UTF-8"?>
<Project>
""")

		f.write("""\
	<Folder FolderName="Filings by name" ExtStr="xml">
""")
		for filing in filings:
			if filing['instanceUrl']:
				f.write("""\
		<File FilePath="%s" HomeFolder="Yes"/>
"""%filing['instanceUrl'][len('filings/YYYY-MM/'):].replace('%7Czip/','|zip\\'))
		f.write("""\
	</Folder>
""")

		f.write("""\
	<Folder FolderName="Filings by company">
""")
		for company in sorted(filings_by_company.keys()):
			f.write("""\
		<Folder FolderName="%s" ExtStr="xml">
"""%company.replace('&','&amp;').replace('<','&lt;'))
			for filing in filings_by_company[company]:
				if filing['instanceUrl']:
					f.write("""\
			<File FilePath="%s" HomeFolder="Yes"/>
"""%filing['instanceUrl'][len('filings/YYYY-MM/'):].replace('%7Czip/','|zip\\'))
			f.write("""\
		</Folder>
""")
		f.write("""\
	</Folder>
""")

		f.write("""\
</Project>
""")
	
def parse_args():
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Generates XMLSpy .spp project file from EDGAR RSS feed for a given month.')
	parser.add_argument('rss_feeds', metavar='RSS', nargs='+', help='EDGAR RSS feed file')
	return parser.parse_args()	

def main():
	# Parse script arguments
	args = parse_args()	
	
	for feedpath in collect_feeds(args.rss_feeds):
		generate_project(feedpath)

if __name__ == '__main__':
	start = time.perf_counter()
	main()
	end = time.perf_counter()
	print('Finished in ',end-start)
