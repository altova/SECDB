Processing SEC filings using Altova RaptorXML+XBRL
==================================================

Introduction
------------

Altova [RaptorXML](http://www.altova.com/raptorxml.html) is the third-generation, hyper-fast XML and XBRL processor from the makers of XMLSpy®. RaptorXML is built from the ground up to be optimized for the latest standards and parallel computing environments. Designed to be highly cross-platform capable, the engine takes advantage of today’s ubiquitous multi-CPU computers to deliver lightning fast processing of XML and XBRL data.

The following project will demonstrate how to write Python scripts using RaptorXML's XBRL engine to process and analyse XBRL filings made available on the [SEC EDGAR](http://www.sec.gov/edgar.shtml) website.

Directory structure
-------------------
```
root
+-- data
    +-- Contains various configuration and data files needed for creating the database and classifying XBRL facts in SEC filings
+-- db
    +-- Can be used to store the generated databases
+-- docs
    +-- Contains documentation and tutorials
+-- feeds
    +-- Contains downloaded EDGAR RSS feeds
+-- filings
    +-- 2010-01
        +-- Contains all downloaded filings for this month
    +-- 2010-02
        +-- Contains all downloaded filings for this month
	+-- ...
+-- logs
    +-- Can be used to store log files
+-- scripts
    +-- Contains various Python scripts using RaptorXML's XBRL engine to process SEC filings
+-- xsd
    +-- Contains the EDGAR RSS XML schema files
```
Download EDGAR RSS feeds
------------------------

The SEC maintains RSS feeds for each month with the filings submitted during that month. These feeds can be accessed on the SEC website under https://www.sec.gov/Archives/edgar/monthly/.
The script `download_feeds.py` will download all the available RSS feeds (by default starting from 2005) and store them within the `feeds` subfolder.
The script can be re-run regularly as it will check for already present feeds and download only the latest or missing feeds.

	RaptorXMLXBRL.exe script scripts\download_feeds.py

After running the command, the `feeds` subfolder will contain any downloaded feeds with the naming convention `xbrlrss-YYYY-mm.xml`.

Download SEC filings
--------------------

The actual XBRL filings can also be downloaded as zip archives from the SEC. The EDGAR RSS feeds contain amongst other meta-information the URL to the zip archive for a given filing. The script `download_filings.py` iterates over each feed items and downloads the zip archive into a `filings` subfolder. Please note that there are over 150,000 filings to this date and downloading them will take several hours and require >30GB of hard-drive space. Again, the script can be interrupted at any time and re-run multiple times as it automatically checks if a filing has already been downloaded.

Here are a few examples: This command will download all the available SEC filings (will take a long time):

	RaptorXMLXBRL.exe script scripts\download_filings.py feeds\xbrlrss-*.xml

To download the filings for a single month only, use the command with a single RSS feed as an argument. E.g. this command will download all filings for April 2015:

	RaptorXMLXBRL.exe script scripts\download_filings.py feeds\xbrlrss-2015-04.xml

Searching SEC filings in feeds
------------------------------

The script `search_filings` can be used to find individual XBRL filings in the EDGAR RSS feed. For example, to find all annual (10-K) filings for the company CARNIVAL CORP in a given month use:

	RaptorXMLXBRL.exe script scripts\search_filings.py feeds\xbrlrss-YYYY-mm.xml --company CARNIVAL --form_type "10-K"

To search through all available EDGAR RSS feeds use:

	RaptorXMLXBRL.exe script scripts\search_filings.py feeds\xbrlrss-*.xml --company CARNIVAL --form_type "10-K"

Validate SEC filings in feeds
-----------------------------

The script `validate_filings.py` shows how to use batch validation using RaptorXML+XBRL's Python API. It takes an RSS feed as input and validates all filings referenced in that feed. Please note that the filings must have been downloaded previously using the `download_filings.py` script.

	RaptorXMLXBRL.exe script scripts\validate_filings.py feeds\xbrlrss-YYYY-mm.xml

Create and populate the SEC DB
------------------------------

The script `build_secdb.py` analyses quarterly (10-Q) and annual (10-K) SEC filings and computes a summary view of the main financial statements. The data for the balance sheet, income statement and cash-flow statement as well as some popular ratios are then stored into a database. Please refer to the additional documentation describing the database schema and tables in more detail.

Use `-h` to get a list of supported options:

	RaptorXMLXBRL.exe script scripts\build_secdb.py -h

The `--db` option specifies the sqlite3 database or ODBC DNS. The `--log` option specifies the location of the output log file. The main arguments to the script are a list of EDGAR RSS feeds. When running the script for the very first time, the `--create-tables` option instructs the script to create the necessary database tables.

	RaptorXMLXBRL.exe script scripts\build_secdb.py --db=db\edgar.db3 --log=logs\log_create.txt --create-tables

Once the DB tables have been created, one can start populating the DB:

	RaptorXMLXBRL.exe script scripts\build_secdb.py feeds\xbrlrss-2010-*.xml --db=db\edgar.db3 --log=logs\log_2010.txt
	RaptorXMLXBRL.exe script scripts\build_secdb.py feeds\xbrlrss-2011-*.xml --db=db\edgar.db3 --log=logs\log_2011.txt
	RaptorXMLXBRL.exe script scripts\build_secdb.py feeds\xbrlrss-2012-*.xml --db=db\edgar.db3 --log=logs\log_2012.txt
	RaptorXMLXBRL.exe script scripts\build_secdb.py feeds\xbrlrss-2013-*.xml --db=db\edgar.db3 --log=logs\log_2013.txt
	...

The `--store-fact-mappings` can be used to store additional references to the original XBRL facts that make up each high-level lineitem in a report. The `--threads` option can be used to limit the number of instances that are processed in parallel.

Automating retrieval and processing of new EDGAR filings
--------------------------------------------------------

In order to keep the SEC DB up-to-date one needs to refresh the EDGAR RSS feed, download all new XBRL filings and finally process those filings and store the data into the SEC DB. The whole work-flow is automated by the `daily_update.py` script. It performas all necessary tasks to keep the SEC DB up-to-date. The script can be run on a daily, weekly or monthly basis from Altova FlowForce as a `commandline` function. In FlowForce `Configuration` tab, create a new job and specify `/system/shell/commandline` as the execution step. In the command edit field enter e.g.

	RaptorXMLXBRL.exe script scripts\daily_update.py --db=db\edgar.db3 --log=logs\daily_update_id{instance-id()}.log

Make sure to also specify the root directory of the SEC DB project as the working directory. Finally, in the `Triggers` section create a new timer and choose the appropriate interval.
