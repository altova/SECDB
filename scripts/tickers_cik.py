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
# limitations under the License.import os
import os
import sys
import re
import csv
import collections
import time
import datetime
import urllib.request
import xml.etree.ElementTree as ET
import argparse
import sqlite3
import socket
import itertools
import zipfile
import ssl
from url_utils import mk_req

# for OTBCC see http://otce.finra.org/DailyList/Archives

Ticker = collections.namedtuple( "Ticker", ["Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","SummaryQuote","misc"])
CikColeftC = collections.namedtuple( "CikColeftC", ["Name", "CIK"])
TickerCik = collections.namedtuple("TickerCik", ["Symbol", "CIK"])
TickerCikName = collections.namedtuple("TickerCikName", ["Symbol", "CIK", "Name"])

stock_exchanges = ('nasdaq', 'nyse', 'amex')

# See https://stackoverflow.com/questions/25338608/download-all-stock-symbol-list-of-a-market
tickers_host = "old.nasdaq.com"
tickers_path = "/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download"

sec_host = 'https://www.sec.gov'
sec_symbol_path = "/cgi-bin/browse-edgar?CIK=%s&Find=Search&owner=exclude&action=getcompany&output=atom"
sec_cikcoleft_path = '/edgar/NYU/cik.coleft.c'
sec_cik_fullindex_master_path = '/Archives/edgar/full-index/master.zip'

def ticker_csv_name(ex):
    return '%s-tickers.csv' %ex

def download_tickers(now, logf):
    for ex in stock_exchanges:
        print( 'fetching from', ex, '...', end='\n', file=logf )
        url = 'https://%s/%s'%(tickers_host,tickers_path %ex)
        filename = now +'/' + ticker_csv_name(ex)
        print( '\tdownloading %s and writing to %s'%(url,filename), end='\n', file=logf)
        with urllib.request.urlopen( mk_req( url ), context=ssl.SSLContext() ) as f:
            with open( filename, 'wb' ) as O:
                O.write( f.read() )

def download_cik_coleft(now, logf):
    url = sec_host + sec_cikcoleft_path
    print( 'fetching from', url, '...', end=' ', file=logf)
    O = urllib.request.build_opener()
    with O.open(mk_req( url )) as o:
        print(o.status, o.reason, file=logf)
        r = o.read()
        with open(now +'/' + os.path.basename(sec_cikcoleft_path), "bw") as f:
            f.write(r)

def download_cik_fullindex_master( now, logf ):
    url = sec_host + sec_cik_fullindex_master_path
    print( 'fetching from', url, '...', end=' ', file=logf)
    O = urllib.request.build_opener()
    with O.open(mk_req( url )) as o:
        print(o.status, o.reason, file=logf)
        r = o.read()
        with open(now +'/' + os.path.basename(sec_cik_fullindex_master_path), "bw") as f:
            f.write(r)

def existing_tickers_from_csv(f):
    for tc in map(TickerCik._make, csv.reader(open(f))):
        yield tc

def existing_tickers_from_db(dbname):
    secdb = sqlite3.connect(dbname)
    c = secdb.cursor()
    for tc in map(TickerCik._make, c.execute('select * from tickers;')):
        yield tc

def update_db_tickers(dbname, new_tc):
    print('updating db tickers @', dbname, len(new_tc))
    with sqlite3.connect(dbname) as secdb:
        c = secdb.cursor()
        for tc in new_tc:
            c.execute("insert into tickers values(?,?)", (tc.Symbol,tc.CIK))
        c.close()
        secdb.commit()

def tickers(now):
    for fname in map(lambda _: ticker_csv_name(_), stock_exchanges ):
        with open(now + '/' + fname) as f:
            it = map(Ticker._make, csv.reader(f))
            next(it)
            for _ in it:
                yield _._replace(Symbol = _.Symbol.strip())

def cik_coleft_c(now):
    with open(now + '/cik.coleft.c', encoding='cp1252') as f:
        for _ in f:
            l = ' '.join(_.split()).split(':')
            yield CikColeftC(':'.join(l[:-2]).strip(),l[-2].strip())

def cik_fullindex_master(now):
    with zipfile.ZipFile(now + '/master.zip') as master:
        with master.open('master.idx') as idx:
            sep = '-'*80
            for _ in idx:
                l = _.decode(encoding='cp1252')
                if l.startswith(sep):
                    break

            for _ in idx:
                l = _.decode(encoding='cp1252')
                l = reversed( l.split('|')[:2] )
                yield CikColeftC._make(map(lambda t: t.strip(), l ))

def ticker_cik_from_csv(csvname):
        with open(csvname) as f:
            it = map(TickerCik._make, csv.reader(f))
            for _ in it:
                yield _._replace(CIK = int(_.CIK))

__normalize_name_del_re = re.compile(r"(&#\d+;)|(\([^)]+\))|[+$!?&']|\band\b|\bAT AL$|^The", re.I)
__normalize_name_repl_res = [
    (re.compile(r'\b(Corporation|Corporaton|Company|Corp)\b', re.I), 'CO'),
    (re.compile(r'\bLimited\b', re.I), 'LTD'),
    (re.compile(r'\bIncorporated\b', re.I), 'INC'),
    (re.compile(r'\bAktiengesellschaft\b', re.I), 'AG'),
    (re.compile(r'(\b|^)(?P<name>[A-Z](\.\s*[A-Z][^A-Z])+\.?)', re.I), lambda _: _.group('name').replace('.', '').replace(' ', '')),
    (re.compile(r'(\b|^)(?P<name>[A-Z] [A-Z])\b', re.I), lambda _: _.group('name').replace(' ', '')),
    (re.compile(r"[,.*/-]", re.I), ' ')
]

def normalize_name(name):
    n = ' '.join( name.split() ).strip().upper()
    n = __normalize_name_del_re.sub('', n)
    for _re, _sub in __normalize_name_repl_res:
        n = _re.sub(_sub, n)
    return ' '.join(n.split())

def contains_normalized_name(a, b):
    if a in b or b in a:
        return True
    for c in zip(a.split(), b.split()):
        if not c[0] in c[1] or not c[1] in c[0]:
            return False
    return True

def is_equal_normalized_name_last_try(a, b):
    _a = set(a.split())
    _b = set(b.split())
    if len(_a & _b) == min(len(_a), len(_b)):
        return True
    delta = _a ^ _b
    set_ignored = set(['CO', 'INC', 'LTD', 'PLC'])
    both = delta & set_ignored
    if len(both) == len(delta):
        return True
    pa = set(itertools.permutations(_a))
    pb = set(itertools.permutations(_b))
    if pa & pb:
        return True
    return False

def sec_query_symbol(logf, symbol, opts):
    sec_url = sec_host + sec_symbol_path %symbol
    O = urllib.request.build_opener()
    try:
        print('### TRY', symbol, sec_url, file=logf)
        with O.open(mk_req( sec_url ), timeout=60) as o:
            if o.headers.get_content_type() == 'application/atom+xml':
                response = o.read().decode()
                print('### RETRIEVED', symbol, file=logf)
                if opts and opts.verbose > 1:
                    logf.write(response)
                x = ET.fromstring(response)
                cis = list(x.iter('{http://www.w3.org/2005/Atom}company-info'))
                if len(cis) == 1:
                    cik = cis[0].find('{http://www.w3.org/2005/Atom}cik')
                    cns = cis[0].find('{http://www.w3.org/2005/Atom}conformed-name')
                    if cik is not None and cns is not None:
                        return (cik.text, cns.text)
                else:
                    ciks = []
                    for ci in cis:
                        cik = ci.find('{http://www.w3.org/2005/Atom}cik')
                        cns = ci.find('{http://www.w3.org/2005/Atom}conformed-name')
                        if cik is not None and cns is not None:
                            ciks.append((cik.text, cns.text))
                    print(symbol, 'symbol-from-sec-multi', len(ciks), ciks, file=logf)
            else:
                print('### NOATOM', symbol, file=logf)
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print('### ERROR', symbol, sec_url, e, file=logf)
    except socket.timeout as e:
        print('### ERROR', symbol, sec_url, e, file=logf)
    return (None, None)

def has_filings_of_types(cik, types):
    O = urllib.request.build_opener()
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=%s&dateb=&owner=exclude&output=atom" %(cik, os.path.commonprefix(types))
    with O.open(mk_req( sec_url ) ) as o:
        if o.headers.get_content_type() == 'application/atom+xml':
            r = o.read().decode()
            x = ET.fromstring(r)
            for _ in list(x.iter('{http://www.w3.org/2005/Atom}category')):
                if _.attrib.get('term') in types:
                    return True
    return False

def has_10Q_filings(cik):
    return has_filings_of_types(cik, ['10-Q'])

def is_terminated(cik):
    return has_filings_of_types(cik, ['15-12B', '15-12G'])

def compare_tickers(oldcsv, newcsv):
    logf = open(os.path.join(os.path.dirname(newcsv), 'diff.txt'), 'w')
    print('comparing', oldcsv, newcsv, file=logf)
    old_tickers = dict(ticker_cik_from_csv(oldcsv))
    new_tickers = dict(ticker_cik_from_csv(newcsv))
    old_symbols = set(old_tickers.values())
    new_symbols = set(new_tickers.keys())
    old_ciks = set(old_tickers.values())
    new_ciks = set(new_tickers.values())
    print('#old', len(old_tickers), 'old.symbols#', len(old_symbols), 'old.ciks#', len(old_ciks), file=logf)
    print('#new', len(new_tickers), 'new.symbols#', len(new_symbols), 'new.ciks#', len(new_ciks), file=logf)

    ciks = {}
    for _ in cik_fullindex_master(os.path.dirname(newcsv)):
        ciks[ _.CIK] = _

    delta_ciks = old_ciks - new_ciks
    print("### ciks not in new", len(delta_ciks), file=logf)
    for _ in delta_ciks:
        print('  CIK =', _, end=' ... ', file=logf)
        if is_terminated(_):
            print('TERMINATED', file=logf)
            continue

        print( 'HAS 10-Q' if has_10Q_filings(_) else 'NOREPORTS', file=logf)
        continue

        print('UNKNOWN', file=logf)

    delta_ciks = new_ciks - old_ciks
    print("### ciks not in old", len(delta_ciks), file=logf)
    # for _ in delta_ciks:
    #     print('old', _, ciks.get(_, "(no entry)"))

    print("### ciks different", file=logf)
    for t,c in new_tickers.items():
        t_re = re.compile(r'\b0+%s\b' %re.escape(t))
        _c = old_tickers.get(t)
        if _c:
            if c != _c:
                print('### DIFF', t, 'old =', _c, 'new =', c, file=logf)
                _f = os.path.join(os.path.dirname(newcsv), 'cik.coleft.c')
                for _l in open(_f):
                    _cik = str(c)
                    _cik = '%s%s' %('0'*(10-len(_cik)), _cik)
                    if _cik in _l.strip():
                        print('%s:' %_f, _l.strip(), file=logf)
                for _f in map(lambda _: os.path.join(os.path.dirname(newcsv), _), ['amex-tickers.csv', 'nasdaq-tickers.csv', 'nyse-tickers.csv', 'tickers.csv', 'update.log']):
                    for _l in open(_f):
                        if t_re.search(_l[:20]):
                            print('%s:' %_f, _l.strip(), file=logf)

def diff_cmd(opts):
    compare_tickers(opts.OLDCSV, opts.NEWCSV)

def update_cmd(opts):
    t0 = datetime.datetime.now()

    now = opts.now
    os.makedirs(now, exist_ok=True)
    print('results in', now)

    logf = open(now + '/update.log', 'w')

    download_tickers(now, logf)
    download_cik_fullindex_master(now, logf)

    if opts.fetch_only:
        return

    all_ccc = list(cik_fullindex_master(now))
    name_ccc = {}
    for name, cik in all_ccc:
        if not name in name_ccc:
            name_ccc[name] = [cik]
        elif cik not in name_ccc[name]:
            name_ccc[name].append(cik)

    norm_name_ccc = {}
    for name, cik in all_ccc:
        norm_name = normalize_name(name)
        if not norm_name in norm_name_ccc:
            norm_name_ccc[norm_name] = [cik]
        else:
            norm_name_ccc[norm_name].append(cik)

    all_tickers = list(set(tickers(now)))
    all_tc = []
    if opts.tickers_from_db:
        dbname = opts.tickers_from_db if opts.tickers_from_db != '#db' else opts.db
        all_tc = list(existing_tickers_from_db( dbname ))
    elif opts.tickers_from_csv:
        all_tc = list(existing_tickers_from_csv(opts.tickers_from_csv))

    all_symbols = set(map(lambda _: _.Symbol, all_tc)) if not opts.ticker else set()
    with open(now+'/new-tickers.csv', 'w', newline='') as f:
        o = csv.writer(f)
        for t in all_tickers:
            if not t.Symbol in all_symbols:
                if not opts.ticker or t.Symbol == opts.ticker:
                    o.writerow(t)

    new_tickers = list(map(Ticker._make, csv.reader(open(now+'/new-tickers.csv'))))
    print(len(new_tickers), 'new tickers', file=logf)

    new_tcn = []
    for t in new_tickers:
        tik = t.Symbol
        cik, cn = sec_query_symbol(logf, tik, opts)
        if cik:
            nn = normalize_name(t.Name)
            ncn = normalize_name(cn)
            if nn == ncn or contains_normalized_name(nn, ncn):
                new_tcn.append(TickerCikName(tik, cik, cn.strip()))
                all_symbols.add(tik)
                print( tik, 'symbol-from-sec', cik, 'name:', t.Name, 'cn:', cn, file=logf)
                continue
            elif is_equal_normalized_name_last_try(nn, ncn):
                new_tcn.append(TickerCikName(tik, cik, cn.strip()))
                all_symbols.add(tik)
                print( tik, 'symbol-from-sec-eq', cik, 'name:', t.Name, 'cn:', cn, file=logf)
                continue

        name = t.Name
        ccc = name_ccc.get(name, None)
        if ccc and len(set(ccc)) == 1:
            new_tcn.append(TickerCikName(tik, ccc[0], name))
            all_symbols.add(tik)
            print( name, 'name-from-ccc', ccc[0], file=logf)
            continue

        norm_name = normalize_name(name)
        ccc = norm_name_ccc.get(norm_name, None)
        if ccc:
            _ccc = set(ccc)
            if len(_ccc) == 1:
                new_tcn.append(TickerCikName(tik, ccc[0], norm_name))
                all_symbols.add(tik)
                print( name, 'normalized-from-sec', ccc[0], 'name:', name, 'norm:', norm_name, file=logf)
                continue
            elif not '^' in tik:
                has10q = False
                for c in _ccc:
                    if has_10Q_filings(c):
                        new_tcn.append(TickerCikName(tik, c, norm_name))
                        all_symbols.add(tik)
                        print( name, 'normalized-from-sec-10q', c, 'name:', name, 'norm:', norm_name, file=logf)
                        break
                if has10q:
                    continue

        print( tik, 'ticker-nok', 'name:', name, 'norm:', norm_name, 'ccc:', ccc, file=logf)

    with open(now +'/nok_tickers.csv', 'w', newline='') as f:
        o = csv.writer(f)
        for t in new_tickers:
            if not t.Symbol in all_symbols:
                o.writerow(t)

    with open(now +'/new-symbols-cik.csv', 'w', newline='') as f:
        o = csv.writer(f)
        for tcn in new_tcn:
            o.writerow(tcn)

    new_tc = list(map( lambda _: TickerCik(_.Symbol, _.CIK), new_tcn))
    if not opts.nodb:
        update_db_tickers(opts.db, new_tc)

    all_tc.extend(new_tc)
    with open('data/tickers.csv', 'w', newline='') as f:
        o = csv.writer(f)
        for tc in sorted(all_tc, key=lambda _: _.Symbol ):
            o.writerow((tc.Symbol, int(tc.CIK)))

    if opts.diff_with:
        compare_tickers(opts.diff_with, now+'/tickers.csv')

    t1 = datetime.datetime.now()
    dt = t1 - t0
    print('processed', now, 'in %ss' %dt.total_seconds(), file=logf )

def mk_arg_parser():
    argp = argparse.ArgumentParser("ticker-cik")
    _argp = argp.add_subparsers(dest='mode')
    diffp = _argp.add_parser('diff')
    diffp.add_argument("OLDCSV")
    diffp.add_argument("NEWCSV")
    updatep = _argp.add_parser('update')
    g = updatep.add_mutually_exclusive_group(required=True)
    g.add_argument('--db')
    g.add_argument('--nodb', action='store_true')
    updatep.add_argument('--now', nargs='?', default=time.strftime('%d%m%Y-%H%M'))
    updatep.add_argument('--diff-with', nargs='?')
    updatep.add_argument('-v', '--verbose', action='count', default=0)
    updatep.add_argument('-t', '--ticker')
    updatep.add_argument('-f', '--fetch-only', action='store_true')
    g = updatep.add_mutually_exclusive_group() #required=True)
    g.add_argument('--tickers-from-db', nargs='?', const='#db')
    g.add_argument('--tickers-from-csv')
    return argp

if __name__ == '__main__':

    argp = mk_arg_parser()
    opts = argp.parse_args()

    if opts.mode == 'diff':
        diff_cmd(opts)

    elif opts.mode == 'update':
        update_cmd(opts)
