# Copyright 2015 Altova GmbH
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
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
#   raptorxmlxbrl script scripts/validate_filings.py feeds/xbrlrss-2015-04.xml

import feed_tools
import tqdm
import re,sys,os.path,time,concurrent.futures,urllib,glob,logging,argparse,multiprocessing,threading
from altova_api.v2 import xml, xsd, xbrl


def validate(filing):
    instance, log = feed_tools.load_instance(filing)
    
    if not instance or log.has_errors():        
        logger.error('Filing %s has %d ERRORS!',feed_tools.instance_url(filing),len(list(log.errors)))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logger.log(logging.DEBUG,'\n'.join([error.text for error in log]))
        return False

    if log.has_inconsistencies():
        inconsistencies = list(log.inconsistencies)
        logger.warning('Filing %s has %d INCONSISTENCIES!',feed_tools.instance_url(filing),len(inconsistencies))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logger.log(logging.DEBUG,'\n'.join([error.text for error in inconsistencies]))
    else:
        logger.info('Filing %s is VALID!',feed_tools.instance_url(filing))
    return True

def validate_filings(filings, max_threads):
    logger.info('Processing %d filings...',len(filings))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        with tqdm.tqdm(range(len(filings))) as progressbar:
            futures = [executor.submit(validate,filing) for filing in filings]
            try:
                for future in concurrent.futures.as_completed(futures):
                    future.result()
                    progressbar.update()
            except KeyboardInterrupt:
                executor._threads.clear()
                concurrent.futures.thread._threads_queues.clear()
                raise

def parse_args():
    """Returns the arguments and options passed to the script."""
    parser = argparse.ArgumentParser(description='Validates all filings contained in the given EDGAR RSS feed from the SEC archive.')
    parser.add_argument('rss_feeds', metavar='RSS', nargs='+', help='EDGAR RSS feed file')
    parser.add_argument('-l', '--log', metavar='LOG_FILE', dest='log_file', help='log output file')
    parser.add_argument('--log-level', metavar='LOG_LEVEL', dest='log_level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO', help='log level (ERROR|WARNING|INFO|DEBUG)')
    parser.add_argument('--cik', help='CIK number')
    parser.add_argument('--sic', help='SIC number')
    parser.add_argument('--form-type', help='Form type (10-K,10-Q,...)')
    parser.add_argument('--company', help='Company name')
    parser.add_argument('--threads', type=int, default=multiprocessing.cpu_count(), dest='max_threads', help='specify max number of threads')
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
    levels = {'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}    
    if args.log_file:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=args.log_file,filemode='w',level=levels[args.log_level])
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=levels[args.log_level])
    logger = logging.getLogger('default')
    
def collect_feeds(args):
    """Returns an generator of the resolved, absolute RSS file paths."""
    for filepath in args.rss_feeds:
        for resolved_filepath in glob.iglob(os.path.abspath(filepath)):
            yield resolved_filepath

def main():
    # Parse script arguments
    args = parse_args() 
    # Setup python logging framework
    setup_logging(args)

    # Validate all filings in the given RSS feeds one month after another
    for filepath in collect_feeds(args):

        # Load EDGAR filing metadata from RSS feed (and filter out all non 10-K/10-Q filings or companies without an assigned ticker symbol)
        filings = []
        for filing in feed_tools.read_feed(filepath):
            # Google to Alphabet reorganization
            if filing['cikNumber'] == 1288776:
                filing['cikNumber'] = 1652044
            if args.form_type is None or args.form_type == filing['formType']:
                if args.sic is None or args.sic == filing['assignedSic']:
                    if args.cik is None or args.cik == filing['cikNumber']:
                        filings.append(filing)

        # Validate the selected XBRL filings
        validate_filings(filings[:100], args.max_threads)

if __name__ == '__main__':
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print('Finished validation in ',end-start)