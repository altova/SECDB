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

# Calculates financial statements and ratios from SEC filings in the given RSS feed and adds them to a database.
#
# Usage:
# 	raptorxmlxbrl script scripts/build_secdb.py feeds/xbrlrss-2015-*.xml --db=sec2015.db3

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl
import feed_tools
import re,csv,json,glob,enum,datetime,argparse,logging,itertools,os.path,urllib,threading,concurrent.futures,timeit,calendar

class Summations(dict):
	def __missing__(self, key):
		return 0

gsRootDir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-2])
gsRootURL = 'file:'+urllib.request.pathname2url(gsRootDir)+'/'

reports = json.load(open(os.path.join(gsRootDir,'data','reports.json')))
reports['balance'].update({'kind': 'balance', 'name': 'Balance Sheet', 'mappings': json.load(open(os.path.join(gsRootDir,'data','balance_mappings.json')))})
reports['income'].update({'kind': 'income', 'name': 'Income Statement', 'mappings': json.load(open(os.path.join(gsRootDir,'data','income_mappings.json')))})
reports['cashflow'].update({'kind': 'cashflow', 'name': 'Cashflow Statement', 'mappings': json.load(open(os.path.join(gsRootDir,'data','cashflow_mappings.json')))})

def setup_db_connect(driver,name):
	"""Returns a function object that can be used to connect to the DB. The function doesn't require any additional parameters as it stores the DB name/connection string using closure."""
	logger.info('Using %s DB with DSN=%s',driver,name)
	def connect_sqlite():
		con = sqlite3.connect(name,isolation_level=None)
		con.execute('PRAGMA journal_mode=WAL')
		return con
	def connect_odbc():
		return pyodbc.connect(name)
	if driver == 'sqlite':
		import sqlite3
		return connect_sqlite
	elif driver == 'odbc':
		import pyodbc
		return connect_odbc

def create_db_tables():
	"""Create all the necessary DB tables."""
	logger.info('Creating DB tables')

	try:
		with db_connect() as con:
			cur = con.cursor()

			cur.execute("""
CREATE TABLE tickers (
	symbol VARCHAR(10) PRIMARY KEY,
	cikNumber INTEGER
);""")

			cur.execute("""
CREATE TABLE filings (
	accessionNumber CHAR(20) PRIMARY KEY,
	cikNumber INTEGER,
	companyName TEXT,
	formType TEXT,
	filingDate DATETIME,
	fileNumber TEXT,
	acceptanceDatetime DATETIME,
	period DATE,
	assistantDirector TEXT,
	assignedSic INTEGER,
	otherCikNumbers TEXT,
	fiscalYearEnd INTEGER,
	instanceUrl TEXT,
	errors TEXT
);""")

			cur.execute("""
CREATE TABLE facts (
	accessionNumber CHAR(20),
	report TEXT,
	pos SMALLINT,
	lineitem TEXT,
	label TEXT,
	namespace TEXT,
	name TEXT,
	value TEXT,
	level SMALLINT,
	is_abstract BOOLEAN,
	is_total BOOLEAN,
	is_negated BOOLEAN,
	PRIMARY KEY (accessionNumber,report,pos)
);""")

			cur.execute("""
CREATE TABLE balance_sheet (
	accessionNumber CHAR(20) PRIMARY KEY,
	cikNumber INTEGER,
	endDate DATE,
	currencyCode CHAR(3),
	%s
);""" % ','.join(key+' BIGINT' for key in reports['balance']['lineitems']))

			cur.execute("""
CREATE TABLE income_statement (
	accessionNumber CHAR(20),
	cikNumber INTEGER,
	endDate DATE,
	duration INTEGER,
	currencyCode CHAR(3),
	%s,
	PRIMARY KEY	(accessionNumber,duration)
);""" % ','.join(key+' BIGINT' for key in reports['income']['lineitems']))

			cur.execute("""
CREATE TABLE cashflow_statement (
	accessionNumber CHAR(20),
	cikNumber INTEGER,
	endDate DATE,
	duration INTEGER,
	currencyCode CHAR(3),
	%s,
	PRIMARY KEY	(accessionNumber,duration)
);""" % ','.join(key+' BIGINT' for key in reports['cashflow']['lineitems']))

			cur.execute("""
CREATE TABLE ratios (
	accessionNumber CHAR(20),
	cikNumber INTEGER,
	endDate DATE,
	kind CHAR(3),
	%s,
	PRIMARY KEY (accessionNumber,kind)
);""" % ','.join(key+' REAL' for key in reports['ratios']['lineitems']))

			con.commit()
	except:
		logger.exception('Failed creating DB tables')
		raise RuntimeError('Failed creating DB tables')

def create_db_indices():
	"""Create all the necessary DB indices."""
	logger.info('Creating DB indices')

	try:
		with db_connect() as con:
			cur = con.cursor()

			# Create indices
			cur.execute('CREATE INDEX income_cik ON income_statement (cikNumber);')
			cur.execute('CREATE INDEX balance_cik ON balance_sheet (cikNumber);')
			cur.execute('CREATE INDEX cashflow_cik ON cashflow_statement (cikNumber);')
			cur.execute('CREATE INDEX ratios_cik ON ratios (cikNumber);')
			cur.execute('CREATE INDEX filings_cik ON filings (cikNumber);')
			cur.execute('CREATE INDEX filings_company ON filings (companyName);')

			con.commit()
	except:
		logger.exception('Failed creating DB indices')
		raise RuntimeError('Failed creating DB indices')

def load_ticker_symbols():
	"""Returns a dict of CIK to ticker symbol."""
	logger.info('Loading ticker file %s','tickers.csv')

	tickers = {}
	with open(os.path.join(gsRootDir,'data','tickers.csv'),'r') as f:
		reader = csv.reader(f)
		for row in reader:
			tickers[int(row[1])] = row[0].split('^')[0]
	return tickers

def insert_ticker_symbols(tickers):
	"""Writes ticker symbol and CIK pairs to the DB."""
	with db_connect() as con:
		for cik, symbol in tickers.items():
			con.execute('INSERT INTO tickers VALUES (?,?)',(symbol,cik))

def load_xbrl_instance(url):
	"""Returns an XBRL instance object for the given filing."""
	filing_logger.info('Loading XBRL instance')

	instance, log = xbrl.Instance.create_from_url(urllib.parse.urljoin(gsRootURL,url))
	if not instance:
		error = '\n'.join([error.text for error in log])
		filing_logger.error(error)
		raise Error('Failed loading XBRL instance %s: %s'%(url,error))
	return instance

re_usgaap = re.compile('^http://[^/]+/us-gaap/')
re_dei = re.compile('^http://xbrl.us/dei/|^http://xbrl.sec.gov/dei/')

def find_std_namespaces(dts):
	"""Returns a tuple with the us-gaap and dei namespaces imported in this company extension taxonomy."""
	usgaap_ns, dei_ns = None, None
	# Iterate through all taxonomy schemas within the DTS and compare the target namespaces
	for taxonomy in dts.taxonomy_schemas:
		if re_usgaap.match(taxonomy.target_namespace):
			usgaap_ns = taxonomy.target_namespace
		elif re_dei.match(taxonomy.target_namespace):
			dei_ns = taxonomy.target_namespace
	return (usgaap_ns, dei_ns)

parts_empty_re = re.compile(r"[`,'.]")
parts_space_re = re.compile(r"[[\]{}():/&-]")

def classify_linkrole(definition):
	"""Returns the type of report based on the roleType definition string."""
	# According to EDGAR Filer Manual rule 6.7.12 definition strings must follow the following syntax:
	#	{SortCode} - {Type} - {Title}
	# where {Type} is one of Disclosure, Document, Schedule or Statement
	definition_parts = definition.split(' - ')
	if len(definition_parts) >= 3 and definition_parts[1] == 'Statement':
		# Remove any punctuation signs
		words = parts_empty_re.sub('',  parts_space_re.sub(' ', ' '.join(definition_parts[2:]).upper())).split()

		# Skip any parenthetical and other supporting statements
		if any(word in words for word in ('PARENTHETHICAL', 'PARENTHETCIAL', 'PARATHETICAL', 'PARATHENTICALS', 'PARENTHETIC', 'PARENTHETICAL', 'PARENTHETICALS', 'PARANTHETICAL', 'PARANTHETICALS', 'PARENTHICAL', 'PARENTHICALS', 'PARENTHTICAL', 'NOTE', 'DISCLOSURE', 'FOOTNOTES', 'DETAILS')):
			return 'other'

		# Check for cash flow statement
		if 'CASHFLOW' in words or 'CASHFLOWS' in words:
			return 'cashflow'
		if 'CASH' in words and ('FLOW' in words or 'FLOWN' in words or 'FLOWS' in words or 'RECEIPTS' in words):
			return 'cashflow'

		# Check for income statement
		if ('INCOME' in words and 'CHANGES' not in words and 'TAX' not in words and 'TAXES' not in words) or 'PROFIT' in words or 'EARNINGS' in words or 'REVENUES' in words or 'OPERATION' in words or 'OPERATIONS' in words or 'EXPENSES' in words or 'LOSS' in words or 'LOSSES' in words:
			return 'income'

		# Check for other naming alternatives for cash flow statement
		if 'CHANGES' in words and (('NET' in words and 'ASSETS' in words) or 'CAPITAL' in words or 'TRUST' in words):
			return 'cashflow'

		# Check for balance sheet statement
		if ('BALANCE' in words or 'BALANCES' in words) and ('SHEET' in words or 'SHEETS' in words or 'SHEEETS' in words):
			return 'balance'
		if 'FINANCIAL' in words or 'POSITION' in words or 'POSITIONS' in words or 'CONDITION' in words or 'CONDITIONS' in words:
			return 'balance'
		if 'ASSETS' in words or 'LIABILITIES' in words:
			return 'balance'

	return 'other'

def definition_string(dts,linkrole):
	role_type = dts.role_type(linkrole)
	if role_type:
		definition = role_type.definition
		if definition:
			return definition.value
	return None

def classify_presentation_link_roles(dts):
	"""Returns a dict containing a list of linkroles for each kind of financial statement."""

	linkroles = {kind: [] for kind in ('balance','income','cashflow','other')}
	for linkrole in dts.presentation_link_roles():
		definition = definition_string(dts,linkrole)
		if definition:
			kind = classify_linkrole(definition)
			linkroles[kind].append(linkrole)

	for kind in ('balance','income','cashflow'):
		if len(linkroles[kind]) > 1:
			filtered = []
			for linkrole in linkroles[kind]:
				definition = ' '.join(definition_string(dts,linkrole).split(' - ')[2:]).upper()
				if 'COMPREHESIVE' not in definition and 'COMPREHENSIVE' not in definition and 'SUPPLEMENTAL' not in definition:
					filtered.append(linkrole)
			filtered.sort(key=lambda linkrole:int(definition_string(dts,linkrole).split(' - ')[0]))
			linkroles[kind] = filtered
	return linkroles

def find_required_context(instance,dei_ns):
	"""Returns the required context for the main reporting period."""
	# According to EDGAR Filter Manual rule 6.5.20 the Required Document Information elements must be reported at least with the Required Context.
	# Required contexts can be distinguished by an absent xbrli:segment element.
	documentPeriodEndDates = instance.facts.filter(xml.QName('DocumentPeriodEndDate',dei_ns))
	for fact in documentPeriodEndDates:
		if not fact.context.entity.segment:
			return fact.context
	return None

def find_required_instant_context(instance,instant):
	"""Returns the required instant context (with absent xbrli:segment element) for the given date."""
	for context in instance.contexts:
		if context.period.is_instant() and context.period.instant.value == instant and not context.entity.segment:
			return context
	return None

def find_dimension_contexts(instance,context,dimensions):
	"""Returns a list of contexts containing the given dimension values and having the same period as the given context."""
	contexts = []
	for dimcontext in instance.contexts:
		if dimcontext.period_aspect_value == context.period_aspect_value and dimcontext.entity_identifier_aspect_value == context.entity_identifier_aspect_value:
			dim_values = list(dimcontext.dimension_aspect_values)
			if dim_values:
				matching_context = True
				for dim in dim_values:
					if dim.dimension not in dimensions or dim.value not in dimensions[dim.dimension]:
						matching_context = False
						break
				if matching_context:
					contexts.append(dimcontext)
	return contexts

def find_fact_value(instance, concept, context):
	"""Returns the fact value found for the given concept and context."""
	if context:
		facts = instance.facts.filter(concept, context)
		for fact in facts:
			# Check for xsi:nil facts
			if fact.xsi_nil:
				continue
			return fact.normalized_value
	return None

def find_numeric_value(instance, concept, context):
	"""Returns the fact numeric value found for the given concept and context."""
	# Ignore non-numeric facts
	if concept.is_numeric() and context:
		facts = instance.facts.filter(concept, context)
		for fact in facts:
			# Check for xsi:nil facts
			if fact.xsi_nil:
				continue
			return fact.effective_numeric_value
	return None

def find_monetary_value(instance, concept, context, currency):
	"""Returns the fact value found for the given concept, context and currency."""
	# Ignore non-monetary facts
	if concept.is_monetary() and context:
		facts = instance.facts.filter(concept, context)
		for fact in facts:
			# Check for xsi:nil facts
			if fact.xsi_nil:
				continue
			# Ignore facts reported with other currency units
			unit = fact.unit_aspect_value
			if unit.iso4217_currency == currency:
				return int(fact.effective_numeric_value)
	return None


def descendants(network,root,include_self=False):
	"""Returns a list of all descendant concepts form the given root."""
	def _descendants(network,root,concepts):
		concepts.append(root)
		for rel in network.relationships_from(root):
			_descendants(network,rel.target,concepts)

	concepts = []
	if include_self:
		concepts.append(root)
	for rel in network.relationships_from(root):
		_descendants(network,rel.target,concepts)
	return concepts


def presentation_concepts(dts,linkrole):
	"""Returns a tuple with a list of all primary items and a dict of dimension domain values featured in the network of presentation relationships for the given linkrole."""
	def _presentation_concepts(network,concept,preferred_label_role,level,concepts,dimensions):
		if isinstance(concept,xbrl.xdt.Dimension):
			dimensions[concept] = set(descendants(network,concept))
			return

		if isinstance(concept,xbrl.xdt.Hypercube):
			level -= 1
		else:
			concepts.append((concept,preferred_label_role,level))
		for rel in network.relationships_from(concept):
			_presentation_concepts(network,rel.target,rel.preferred_label,level+1,concepts,dimensions)

	concepts = []
	dimensions = {}
	network = dts.presentation_base_set(linkrole).network_of_relationships()
	for root in network.roots:
		_presentation_concepts(network,root,None,0,concepts,dimensions)
	return concepts, dimensions

def concept_label(concept,label_role):
	labels = list(concept.labels(label_role=label_role))
	if not labels:
		return None
	return labels[0].text

def is_total_role(preferred_label_role):
	if preferred_label_role:
		return 'total' in preferred_label_role.lower()
	return False

def is_negated_role(preferred_label_role):
	if preferred_label_role:
		return 'negated' in preferred_label_role.lower()
	return False

def is_start_role(preferred_label_role):
	if preferred_label_role:
		return 'periodstart' in preferred_label_role.lower()
	return False

def is_end_role(preferred_label_role):
	if preferred_label_role:
		return 'periodend' in preferred_label_role.lower()
	return False

def find_presentation_linkbase_values(filing, report, instance, linkrole, context, currency):
	"""Returns a dict from concept name to fact value for all monetary concepts appearing in the presentation linkbase for the given linkrole."""

	dim_contexts = []
	dim_contexts_stock = []
	# Get all concepts and dimensions in the presentation linkbase for the given linkrole
	concepts, dimensions = presentation_concepts(instance.dts,linkrole)
	if dimensions:
		dim_contexts = find_dimension_contexts(instance,context,{dim: dimensions[dim] for dim in dimensions if dim.name not in ('LegalEntityAxis','StatementClassOfStockAxis')})
		dim_contexts_stock = find_dimension_contexts(instance,context,{dim: dimensions[dim] for dim in dimensions if dim.name == 'StatementClassOfStockAxis'})

	fact_values = {}
	for i, (concept, preferred_label_role, level) in enumerate(concepts):
		# Skip abstract and non-monetary concepts
		if concept.abstract:
			value = None
		elif concept.is_monetary():
			values = []

			# Try to find a value with the main required context
			value = find_monetary_value(instance, concept, context, currency)
			if value is not None:
				values.append(value)
			else:
				# If the concept is reported only with a dimensional breakdown, sum over all dimension domain members
				for dim_context in dim_contexts:
					value = find_monetary_value(instance, concept, dim_context, currency)
					if value is not None:
						values.append(value)

			# Exception for StatementClassOfStockAxis dimension: Add the sum over all dimension domain members to the value reported without dimensions
			for dim_context in dim_contexts_stock:
				value = find_monetary_value(instance, concept, dim_context, currency)
				if value is not None:
					values.append(value)

			value = sum(values) if values else None
			if value:
				fact_values[concept.name] = {'pos': i, 'concept': concept, 'value': value}
			else:
				if is_start_role(preferred_label_role) and context.period.is_duration():
					value = find_monetary_value(instance, concept, find_required_instant_context(instance, context.period.start_date.value), currency)
				elif is_end_role(preferred_label_role) and context.period.is_duration():
					value = find_monetary_value(instance, concept, find_required_instant_context(instance, context.period.end_date.value), currency)

		elif concept.is_numeric():
			value = find_numeric_value(instance, concept, context)
		else:
			value = find_fact_value(instance, concept, context)

		# Insert fact value to DB
		if args.store_fact_mappings:
			with db_connect() as con:
				con.execute('INSERT INTO facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(filing['accessionNumber'],report['kind'],i,None,concept_label(concept,preferred_label_role),concept.target_namespace,concept.name,str(value),level,concept.abstract,is_total_role(preferred_label_role),is_negated_role(preferred_label_role)))
				con.commit()

	return fact_values

def walk_calc_tree(filing,report,instance,network,concept,weight,fact_values,lineitem_values,allowed_lineitems,other_lineitem,visited_concepts):
	"""Iterates over the concepts in the calculation tree and adds them to the appropriate report line items. If an unknown concept is encountered, it is added to the "other" line item of the current breakdown."""

	if concept in visited_concepts:
		visited_concepts.update(descendants(network,concept))
		return
	visited_concepts.add(concept)

	lineitem = None
	child_rels = list(network.relationships_from(concept))

	value = fact_values.get(concept.name)

	current_mapping = report['mappings'].get(concept.name)
	if current_mapping:

		if 'add-to' in current_mapping:
			lineitem = current_mapping['add-to'][0]
			for x in current_mapping['add-to']:
				if x in allowed_lineitems:
					lineitem = x
					break
		elif 'total' in current_mapping:
			lineitem = current_mapping['total']

		if lineitem and lineitem not in allowed_lineitems:
			# log error
			filing_logger.warning('%s: Concept %s is not expected to occur within breakdown of %s',report['name'],concept.qname,next(network.relationships_to(concept)).source.qname)
			lineitem = None

		allowed_lineitems = allowed_lineitems & set(current_mapping['allowed'] if 'allowed' in current_mapping else [lineitem])
		if len(allowed_lineitems) == 0:
			# log error
			filing_logger.warning('%s: Concept %s is not expected to occur within breakdown of %s',report['name'],concept.qname,next(network.relationships_to(concept)).source.qname)

		if 'other' in current_mapping:
			other_lineitem = current_mapping.get('other')

		if value:
			if not lineitem and not child_rels:
				lineitem = other_lineitem
			if lineitem:
				# Insert mapping to DB
				if args.store_fact_mappings:
					with db_connect() as con:
						con.execute('UPDATE facts SET lineitem = ? WHERE accessionNumber = ? AND report = ? AND pos = ?',(lineitem,filing['accessionNumber'],report['kind'],value['pos']))
						con.commit()

				if 'total' in current_mapping:
					if lineitem in lineitem_values:
						# error if already set
						filing_logger.error('%s: Overwriting already set total value of concept %s',report['name'],concept.qname)
					lineitem_values[lineitem] = weight * value['value']
				else:
					lineitem_values[lineitem] += weight * value['value']
					visited_concepts.update(descendants(network,concept))
					return
			elif not child_rels:
				# log error
				filing_logger.error('%s: Ignored value of inconsistent concept %s',report['name'],concept.qname)
	else:

		if value and not child_rels:
			if other_lineitem:
				# Insert mapping to DB
				if args.store_fact_mappings:
					with db_connect() as con:
						con.execute('UPDATE facts SET lineitem = ? WHERE accessionNumber = ? AND report = ? AND pos = ?',(other_lineitem,filing['accessionNumber'],report['kind'],value['pos']))
						con.commit()

				# log unknown concept
				filing_logger.warning('%s: Added value of unknown concept %s to %s',report['name'],concept.qname,other_lineitem)
				lineitem_values[other_lineitem] += weight * value['value']
			else:
				# log error
				filing_logger.error('%s: Ignored value of unknown concept %s',report['name'],concept.qname)
			visited_concepts.update(descendants(network,concept))
			return

	if concept.name == 'Assets':
		rel_current = None
		for rel in child_rels:
			if rel.target.name == 'AssetsCurrent':
				rel_current = rel
				break
		if rel_current:
			allowed_lineitems = set(['cashAndCashEquivalents','shortTermInvestments','cashAndShortTermInvestments','receivablesNet','inventory','currentAssetsOther','currentAssetsTotal'])
			other_lineitem = 'currentAssetsOther'
			for rel in child_rels:
				walk_calc_tree(filing,report,instance,network,rel.target,weight*int(rel.weight),fact_values,lineitem_values,allowed_lineitems,other_lineitem,visited_concepts)
				if rel == rel_current:
					allowed_lineitems = set(['longTermInvestments','propertyPlantAndEquipmentGross','accumulatedDepreciation','propertyPlantAndEquipmentNet','goodwill','intangibleAssets','nonCurrrentAssetsOther','deferredLongTermAssetCharges','nonCurrentAssetsTotal'])
					other_lineitem = 'nonCurrrentAssetsOther'
			return
	if concept.name == 'Liabilities':
		rel_current = None
		for rel in child_rels:
			if rel.target.name == 'LiabilitiesCurrent':
				rel_current = rel
				break
		if rel_current:
			allowed_lineitems = set(['accountsPayable','shortTermDebt', 'currentLiabilitiesOther', 'currentLiabilitiesTotal'])
			other_lineitem = 'currentLiabilitiesOther'
			for rel in child_rels:
				walk_calc_tree(filing,report,instance,network,rel.target,weight*int(rel.weight),fact_values,lineitem_values,allowed_lineitems,other_lineitem,visited_concepts)
				if rel == rel_current:
					allowed_lineitems = set(['longTermDebt','capitalLeaseObligations', 'longTermDebtTotal', 'deferredLongTermLiabilityCharges', 'nonCurrentLiabilitiesOther', 'nonCurrentLiabilitiesTotal'])
					other_lineitem = 'nonCurrentLiabilitiesOther'
			return

	for rel in child_rels:
		walk_calc_tree(filing,report,instance,network,rel.target,weight*int(rel.weight),fact_values,lineitem_values,allowed_lineitems,other_lineitem,visited_concepts)

def calc_total_values(total_rules,lineitem_values,lineitem):
	"""Calculates any missing (not directly reported) total values."""
	if lineitem not in lineitem_values or lineitem_values[lineitem] is None:
		values = []
		negate = False
		for summand in total_rules[lineitem]:
			if summand == '-':
				negate = True
				continue
			if summand in total_rules:
				calc_total_values(total_rules,lineitem_values,summand)
			if summand in lineitem_values and lineitem_values[summand] is not None:
				values.append(-lineitem_values[summand] if negate else lineitem_values[summand])
		lineitem_values[lineitem] = sum(values) if len(values) > 0 else None

def calc_report_values(filing,report,instance,linkrole,fact_values):
	"""Returns a dict with the calculated values for each lineitem of the report."""

	lineitem_values = Summations()
	visited_concepts = set()

	network = calculation_network(instance.dts,linkrole)
	if network:
		for root in network.roots:
			walk_calc_tree(filing,report,instance,network,root,1,fact_values,lineitem_values,set(report['lineitems']),None,visited_concepts)

	visited_concept_names = set(concept.name for concept in visited_concepts)
	for concept_name, value in fact_values.items():
		if concept_name not in visited_concept_names:

			lineitem = None

			current_mapping = report['mappings'].get(concept_name)
			if current_mapping:
				if 'add-to' in current_mapping:
					lineitem = current_mapping['add-to'][0]
				elif 'total' in current_mapping:
					lineitem = current_mapping['total']

			if lineitem:
				if lineitem not in lineitem_values:
					# Insert mapping to DB
					if args.store_fact_mappings:
						with db_connect() as con:
							con.execute('UPDATE facts SET lineitem = ? WHERE accessionNumber = ? AND report = ? AND pos = ?',(lineitem,filing['accessionNumber'],report['kind'],value['pos']))
							con.commit()

					if lineitem == 'treasuryStockValue' and value['value'] > 0:
						value['value'] *= -1
					lineitem_values[lineitem] += value['value']
				else:
					# log error
					filing_logger.warning('%s: Ignored value of concept %s outside of calculation tree to preserve totals',report['name'],value['concept'].qname)
			else:
				# log unknown concept
				filing_logger.warning('%s: Ignored value of unknown concept %s outside of calculation tree',report['name'],value['concept'].qname)

	# Set missing/not reported values to None
	for lineitem in report['lineitems']:
		if lineitem not in lineitem_values:
			lineitem_values[lineitem] = None

	for lineitem in report['totals']:
		calc_total_values(report['totals'],lineitem_values,lineitem)

	return lineitem_values

def calculation_network(dts,linkrole):
	"""Returns an object representing the network of calculation relationships for the given linkrole."""
	baseset = dts.calculation_base_set(linkrole)
	if baseset:
		return baseset.network_of_relationships()
	else:
		filing_logger.warning('No calculation linkbase found for linkrole %s',linkrole)
		return None

def end_date(context):
	"""Returns the end date specified as in the context as used in financial statements (e.g. ending Dec. 31, 2015 instead of Jan. 1)."""
	period = context.period_aspect_value
	if period.period_type == xbrl.PeriodType.INSTANT:
		return period.instant.date() - datetime.timedelta(days=1)
	elif period.period_type == xbrl.PeriodType.START_END:
		return period.end.date() - datetime.timedelta(days=1)
	else:
		return datetime.date.max
		
def calc_balance_sheet(filing,instance,context,linkroles):
	"""Calculate balance sheet line items from XBRL instance and store in DB."""
	filing_logger.info('Calculate %s',reports['balance']['name'])

	if not context:
		filing_logger.error('Skipped %s: No required context found',reports['balance']['name'])
		return
	if len(linkroles) == 0:
		filing_logger.error('Skipped %s: No linkrole found',reports['balance']['name'])
		return
	elif len(linkroles) > 1:
		filing_logger.warning('%s: Multiple linkroles found: %s',reports['balance']['name'],','.join(linkroles))
	linkrole = linkroles[0]

	fact_values = find_presentation_linkbase_values(filing,reports['balance'],instance,linkrole,context,'USD')
	values = calc_report_values(filing,reports['balance'],instance,linkrole,fact_values)
	values.update({'accessionNumber': filing['accessionNumber'], 'cikNumber': filing['cikNumber'], 'endDate': end_date(context), 'currencyCode': 'USD'})

	# Insert balance sheet into DB
	with db_connect() as con:
		db_fields = ['accessionNumber','cikNumber','endDate','currencyCode'] + reports['balance']['lineitems']
		con.execute('INSERT INTO balance_sheet VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
		con.commit()

def calc_income_statement(filing,instance,context,linkroles):
	"""Calculate income line items from XBRL instance and store in DB."""
	filing_logger.info('Calculate %s',reports['income']['name'])

	if not context:
		filing_logger.error('Skipped %s: No required context found',reports['income']['name'])
		return
	if len(linkroles) == 0:
		filing_logger.error('Skipped %s: No linkrole found',reports['income']['name'])
		return
	elif len(linkroles) > 1:
		filing_logger.warning('%s: Multiple linkroles found: %s',reports['income']['name'],','.join(linkroles))
	linkrole = linkroles[0]

	duration = 12 if filing['formType'] == '10-K' else 3
	contexts = [context2 for context2 in instance.contexts if context2.period.is_start_end() and context2.period.aspect_value.end == context.period.aspect_value.end and round((context2.period.aspect_value.end-context2.period.aspect_value.start).days/30) == duration and not context2.entity.segment]
	if not contexts:
		filing_logger.error('%s: No required context found with %d month duration',reports['income']['name'],duration)
		return
	context = contexts[0]

	fact_values = find_presentation_linkbase_values(filing,reports['income'],instance,linkrole,context,'USD')
	values = calc_report_values(filing,reports['income'],instance,linkrole,fact_values)
	values.update({'accessionNumber': filing['accessionNumber'], 'cikNumber': filing['cikNumber'], 'duration': duration, 'endDate': end_date(context), 'currencyCode': 'USD'})

	for lineitem in ('costOfRevenue','researchAndDevelopment','sellingGeneralAndAdministrative','nonRecurring','operatingExpensesOther','operatingExpensesTotal','interestExpense','incomeTaxExpense','minorityInterest','preferredStockAndOtherAdjustments'):
		if values[lineitem]:
			values[lineitem] *= -1

	# Insert income statement into DB
	with db_connect() as con:
		db_fields = ['accessionNumber','cikNumber','endDate','duration','currencyCode'] + reports['income']['lineitems']
		con.execute('INSERT INTO income_statement VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
		con.commit()

		# Calculate data for the last quarter from the annual report
		if filing['formType'] == '10-K':
			previous_year_date = datetime.date(filing['period'].year-1,filing['period'].month,calendar.monthrange(filing['period'].year-1,filing['period'].month)[1])
			previous_quarters = con.execute('SELECT * FROM income_statement WHERE duration = 3 AND accessionNumber IN (SELECT accessionNumber FROM filings WHERE cikNumber = ? AND formType = "10-Q" AND period BETWEEN ? AND ?)',(filing['cikNumber'],previous_year_date,filing['period'])).fetchall()

			if len(previous_quarters) == 3:
				field_offset = len(db_fields)-len(reports['income']['lineitems'])
				for i, lineitem in enumerate(reports['income']['lineitems']):
					if values[lineitem] is not None:
						for previous_quarter in previous_quarters:
							if previous_quarter[i+field_offset]:
								values[lineitem] -= previous_quarter[i+field_offset]

				values['duration'] = 3
				con.execute('INSERT INTO income_statement VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
				con.commit()

def calc_cashflow_statement(filing,instance,context,linkroles):
	"""Calculate cashflow line items from XBRL instance and store in DB."""
	filing_logger.info('Calculate %s',reports['cashflow']['name'])

	if not context:
		filing_logger.error('Skipped %s: No required context found',reports['cashflow']['name'])
		return
	if len(linkroles) == 0:
		filing_logger.error('Skipped %s: No linkrole found',reports['cashflow']['name'])
		return
	elif len(linkroles) > 1:
		filing_logger.warning('%s: Multiple linkroles found: %s',reports['cashflow']['name'],','.join(linkroles))
	linkrole = linkroles[0]

	duration = round((context.period_aspect_value.end.date()-context.period_aspect_value.start.date()).days/30)

	fact_values = find_presentation_linkbase_values(filing,reports['cashflow'],instance,linkrole,context,'USD')
	values = calc_report_values(filing,reports['cashflow'],instance,linkrole,fact_values)
	values.update({'accessionNumber': filing['accessionNumber'], 'cikNumber': filing['cikNumber'], 'duration': duration, 'endDate': end_date(context), 'currencyCode': 'USD'})

	# Insert cash flow statement into DB
	with db_connect() as con:
		db_fields = ['accessionNumber','cikNumber','endDate','duration','currencyCode'] + reports['cashflow']['lineitems']
		con.execute('INSERT INTO cashflow_statement VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
		con.commit()

		previous_quarters = None
		# Calculate data for the current quarter
		if filing['formType'] == '10-Q' and duration > 3:
			month, year = filing['period'].month, filing['period'].year
			month -= duration
			if month < 1:
				month += 12
				year -= 1
			previous_year_date = datetime.date(year,month,calendar.monthrange(year,month)[1])
			previous_quarters = con.execute('SELECT * FROM cashflow_statement WHERE duration = 3 AND accessionNumber IN (SELECT accessionNumber FROM filings WHERE cikNumber = ? AND formType = "10-Q" AND period BETWEEN ? and ?)',(filing['cikNumber'],previous_year_date,filing['period'])).fetchall()
			if len(previous_quarters) != (duration/3 - 1):
				filing_logger.error('%s: Missing previous quarterly reports to calculate quarterly data from compounded quarterly report',reports['cashflow']['name'])
				previous_quarters = None

		# Calculate data for the last quarter of the financial year from the annual report
		elif filing['formType'] == '10-K':
			previous_year_date = datetime.date(filing['period'].year-1,filing['period'].month,calendar.monthrange(filing['period'].year-1,filing['period'].month)[1])
			previous_quarters = con.execute('SELECT * FROM cashflow_statement WHERE duration = 3 AND accessionNumber IN (SELECT accessionNumber FROM filings WHERE cikNumber = ? AND formType = "10-Q" AND period BETWEEN ? and ?)',(filing['cikNumber'],previous_year_date,filing['period'])).fetchall()
			if len(previous_quarters) != 3:
				filing_logger.error('%s: Missing previous quarterly reports to calculate quarterly data from annual report',reports['cashflow']['name'])
				previous_quarters = None

		if previous_quarters:
			field_offset = len(db_fields)-len(reports['cashflow']['lineitems'])
			for i, lineitem in enumerate(reports['cashflow']['lineitems']):
				if values[lineitem] is not None:
					for previous_quarter in previous_quarters:
						if previous_quarter[i+field_offset]:
							values[lineitem] -= previous_quarter[i+field_offset]

			values['duration'] = 3
			con.execute('INSERT INTO cashflow_statement VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
			con.commit()

def dbvalue(dbvalues,report,lineitem,avg_over_duration):
	if lineitem[0] == '-':
		weight = -1
		lineitem = lineitem[1:]
	else:
		weight = 1
	if report == 'balance':
		if avg_over_duration:
			return weight*(dbvalues['previous_balance'][lineitem] + dbvalues['balance'][lineitem])/2
		else:
			return weight*dbvalues['balance'][lineitem]
	else:
		return weight*dbvalues[report][lineitem]

def calc_ratios_mrq(filing):
	"""Computes the ratios for the most recent quarter (mrq), annualized."""
	dbvalues = {
		'previous_balance':	Summations(),
		'balance':			Summations(),
		'income':			Summations(),
		'cashflow':			Summations()
	}
	with db_connect() as con:
		factor = 4 if filing['formType'] == '10-Q' else 1
		# Fetch end balance sheet values from DB
		for row in con.execute('SELECT * FROM balance_sheet WHERE accessionNumber = ?',(filing['accessionNumber'],)):
			for i, lineitem in enumerate(reports['balance']['lineitems']):
				dbvalues['balance'][lineitem] += row[i+4] if row[i+4] else 0
		# Fetch start balance sheet values from DB
		previous_filing = con.execute('SELECT accessionNumber FROM filings WHERE cikNumber = ? AND period < ? ORDER BY period DESC',(filing['cikNumber'],filing['period'])).fetchone()
		if previous_filing:
			for row in con.execute('SELECT * FROM balance_sheet WHERE accessionNumber = ?',(previous_filing[0],)):
				for i, lineitem in enumerate(reports['balance']['lineitems']):
					dbvalues['previous_balance'][lineitem] += row[i+4] if row[i+4] else 0
		# Fetch income statement values from DB
		for row in con.execute('SELECT * FROM income_statement WHERE accessionNumber = ?',(filing['accessionNumber'],)):
			for i, lineitem in enumerate(reports['income']['lineitems']):
				dbvalues['income'][lineitem] += factor*row[i+5] if row[i+5] else 0
		# Fetch cashflow statement values from DB
		for row in con.execute('SELECT * FROM cashflow_statement WHERE accessionNumber = ?',(filing['accessionNumber'],)):
			for i, lineitem in enumerate(reports['cashflow']['lineitems']):
				dbvalues['cashflow'][lineitem] += factor*row[i+5] if row[i+5] else 0

		values = {'accessionNumber': filing['accessionNumber'], 'cikNumber': filing['cikNumber'], 'endDate': filing['period'], 'kind': 'mrq'}
		for lineitem, ratio in reports['ratios']['formulas'].items():
			# Check if the average in assets/liabilities over the whole period should be used
			referenced_reports = set(op['report'] for op in itertools.chain(ratio['numerator'],ratio['denominator']))
			avg_over_duration = len(referenced_reports) > 1 and 'balance' in referenced_reports

			# Calculate the ratio
			numerator = sum(dbvalue(dbvalues,op['report'],op['lineitem'],avg_over_duration) for op in ratio['numerator'])
			denominator = sum(dbvalue(dbvalues,op['report'],op['lineitem'],avg_over_duration) for op in ratio['denominator'])
			values[lineitem] = numerator / denominator if denominator else None

		# Insert ratios into DB
		db_fields = ['accessionNumber','cikNumber','endDate','kind'] + reports['ratios']['lineitems']
		con.execute('INSERT INTO ratios VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
		con.commit()

def calc_ratios_ttm(filing):
	"""Computes the ratios for the trailing twelve months (ttm)."""
	dbvalues = {
		'balance':			Summations(),
		'income':			Summations(),
		'cashflow':			Summations()
	}
	previous_year_date = datetime.date(filing['period'].year-1,filing['period'].month,calendar.monthrange(filing['period'].year-1,filing['period'].month)[1])

	with db_connect() as con:
		# Fetch filings for the last year
		previous_filings = con.execute('SELECT * FROM filings WHERE cikNumber = ? AND period > ? AND period <= ?',(filing['cikNumber'],previous_year_date,filing['period'])).fetchall()
		for previous_filing in previous_filings:
			# Fetch balance sheet values from DB
			for row in con.execute('SELECT * FROM balance_sheet WHERE accessionNumber = ?',(previous_filing[0],)):
				for i, lineitem in enumerate(reports['balance']['lineitems']):
					dbvalues['balance'][lineitem] += row[i+4]/4 if row[i+4] else 0
			# Fetch income statement values from DB
			for row in con.execute('SELECT * FROM income_statement WHERE accessionNumber = ? AND duration = 3',(previous_filing[0],)):
				for i, lineitem in enumerate(reports['income']['lineitems']):
					dbvalues['income'][lineitem] += row[i+5] if row[i+5] else 0
			# Fetch cashflow statement values from DB
			for row in con.execute('SELECT * FROM cashflow_statement WHERE accessionNumber = ? AND duration = 3',(previous_filing[0],)):
				for i, lineitem in enumerate(reports['cashflow']['lineitems']):
					dbvalues['cashflow'][lineitem] += row[i+5] if row[i+5] else 0

		values = {'accessionNumber': filing['accessionNumber'], 'cikNumber': filing['cikNumber'], 'endDate': filing['period'], 'kind': 'ttm'}
		for lineitem, ratio in reports['ratios']['formulas'].items():
			# Calculate the ratio
			numerator = sum(dbvalue(dbvalues,op['report'],op['lineitem'],False) for op in ratio['numerator'])
			denominator = sum(dbvalue(dbvalues,op['report'],op['lineitem'],False) for op in ratio['denominator'])
			values[lineitem] = numerator / denominator if denominator else None

		# Insert ratios into DB
		db_fields = ['accessionNumber','cikNumber','endDate','kind'] + reports['ratios']['lineitems']
		con.execute('INSERT INTO ratios VALUES(%s)' % ','.join(['?']*len(db_fields)),[values[key] for key in db_fields])
		con.commit()

def process_filing(filing):
	"""Load XBRL instance and store extracted data to DB."""

	# Store current filing in thread-local storage
	tls.filing = filing
	filing_logger.info('Start processing filing')

	with db_connect() as con:
		# Check if the filing was already processed
		if con.execute('SELECT accessionNumber FROM filings WHERE accessionNumber = ?',(filing['accessionNumber'],)).fetchone():
			if not args.recompute:
				filing_logger.info('Skipped already processed filing')
				return
			filing_logger.info('Deleting existing filing %s',filing['accessionNumber'])
			con.execute('DELETE FROM filings WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.execute('DELETE FROM facts WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.execute('DELETE FROM balance_sheet WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.execute('DELETE FROM income_statement WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.execute('DELETE FROM cashflow_statement WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.execute('DELETE FROM ratios WHERE accessionNumber = ?',(filing['accessionNumber'],))
			con.commit()

		# Handle amendment filings
		if filing['formType'].endswith('/A'):
			filing['formType'] = filing['formType'][:-2]

			# Delete the previous amended filing
			for row in con.execute('SELECT accessionNumber FROM filings WHERE cikNumber = ? and period = ?',(filing['cikNumber'],filing['period'])):
				filing_logger.info('Deleting amended filing %s',row[0])
				con.execute('DELETE FROM filings WHERE accessionNumber = ?',(row[0],))
				con.execute('DELETE FROM facts WHERE accessionNumber = ?',(row[0],))
				con.execute('DELETE FROM balance_sheet WHERE accessionNumber = ?',(row[0],))
				con.execute('DELETE FROM income_statement WHERE accessionNumber = ?',(row[0],))
				con.execute('DELETE FROM cashflow_statement WHERE accessionNumber = ?',(row[0],))
				con.execute('DELETE FROM ratios WHERE accessionNumber = ?',(row[0],))
				con.commit()

	# Load XBRL instance from zip archive
	instance, log = xbrl.Instance.create_from_url(urllib.parse.urljoin(gsRootURL,filing['instanceUrl']))
	filing['errors'] = '\n'.join(error.text for error in log.errors) if log.has_errors() else None
	#filing['warnings'] = '\n'.join(error.text for error in itertools.chain(log.warnings, log.inconsistencies)) if log.has_warnings() or log.has_inconsistencies() else None

	# Write filing metadata into DB
	with db_connect() as con:
		con.execute('INSERT INTO filings VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[filing[key] for key in ('accessionNumber','cikNumber','companyName','formType','filingDate','fileNumber','acceptanceDatetime','period','assistantDirector','assignedSic','otherCikNumbers','fiscalYearEnd','instanceUrl','errors')])
		con.commit()

	if instance:
		# Find the appropriate linkroles for the main financial statements
		linkroles = classify_presentation_link_roles(instance.dts)

		# Find the required contexts for the main reporting period
		usgaap_ns, dei_ns = find_std_namespaces(instance.dts)
		required_context = find_required_context(instance,dei_ns)
		if required_context and required_context.period.is_start_end():
			# Check duration of required context
			duration = round((required_context.period_aspect_value.end.date()-required_context.period_aspect_value.start.date()).days/30)
			if filing['formType'] == '10-K' and duration != 12:
				filing_logger.warning('10-K Required Context has duration of %d months',duration)
			elif filing['formType'] == '10-Q' and (duration != 3 and duration != 6 and duration != 9):
				filing_logger.warning('10-Q Required Context has duration of %d months',duration)

			# Find an instant context for the period end date
			required_instant_context = find_required_instant_context(instance,required_context.period.end_date.value)

			# Calculate and store values for the main financial statements to DB
			calc_balance_sheet(filing,instance,required_instant_context,linkroles['balance'])
			calc_income_statement(filing,instance,required_context,linkroles['income'])
			calc_cashflow_statement(filing,instance,required_context,linkroles['cashflow'])

			# Calculate and store ratios to DB
			calc_ratios_mrq(filing)
			calc_ratios_ttm(filing)
		else:
			filing_logger.error('Missing or non-duration required context encountered')

	filing_logger.info('Finished processing filing')

def process_filings_for_cik(cik,filings):
	#if filings[0]['companyName'] not in ('JOHNSON & JOHNSON','INTERNATIONAL BUSINESS MACHINES CORP','EXXON MOBIL CORP','CARNIVAL CORP','Google Inc.','AMAZON COM INC','APPLE INC','MICROSOFT CORP','ORACLE CORP','General Motors Co','GENERAL ELECTRIC CO','WAL MART STORES INC'):
	#	return

	for filing in filings:
		try:
			process_filing(filing)
		except:
			logger.exception('Failed processing filing %s',filing['accessionNumber'])

def process_filings(filings):
	"""Distribute processing of filings over multiple threads/cores."""
	logger.info('Start processing 10-K/10-Q filings (count=%d)',sum(len(x) for x in filings.values()))
	with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads) as executor:
		futures = [executor.submit(process_filings_for_cik,cik,filings[cik]) for cik in filings]
		for future in concurrent.futures.as_completed(futures):
			try:
				future.result()
			except:
				logger.exception('Exception occurred')
	logger.info('Finished processing 10-K/10-Q filings')

class FilingLogAdapter(logging.LoggerAdapter):

	def process(self, msg, kwargs):
		filing = tls.filing
		return '[%s %s %s] %s'%(filing['ticker'],filing['cikNumber'],filing['accessionNumber'],msg), kwargs

def setup_logging(log_file):
	"""Setup the Python logging infrastructure."""
	global tls,logger,filing_logger
	tls = threading.local()

	if log_file:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=log_file,filemode='w',level=logging.INFO)
	else:
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.INFO)
	logger = logging.getLogger('default')
	filing_logger = FilingLogAdapter(logger,{})

def parse_args(daily_update=False):
	"""Returns the arguments and options passed to the script."""
	parser = argparse.ArgumentParser(description='Process XBRL filings and extract financial data and ratios into a DB.')
	if not daily_update:
		parser.add_argument('rss_feeds', metavar='RSS', nargs='*', help='EDGAR RSS feed file')
		parser.add_argument('--create-tables', default=False, action='store_true', help='specify very first time to create empty DB tables')
	parser.add_argument('--db', metavar='DSN', default='sec.db3', dest='db_name', help='specify the target DB datasource name or file')
	parser.add_argument('--db-driver', default='sqlite', choices=['sqlite','odbc'], help='specify the DB driver to use')
	parser.add_argument('--log', metavar='LOGFILE', dest='log_file', help='specify output log file')
	parser.add_argument('--threads', metavar='MAXTHREADS', type=int, default=8, dest='max_threads', help='specify max number of threads')
	parser.add_argument('--cik', metavar='CIK', type=int, help='limit processing to only the specified CIK number')
	parser.add_argument('--recompute', default=False, action='store_true', help='recompute and replace filings already present in DB')
	parser.add_argument('--store-fact-mappings', default=False, action='store_true', help='stores original XBRL fact values and mappings to line items in DB')
	if daily_update:
		parser.add_argument('--retries', type=int, default=3, dest='max_retries', help='specify max number of retries to download a specific filing')
	return parser.parse_args()

def build_secdb(feeds):
	# Setup python logging framework
	setup_logging(args.log_file)

	tickers = load_ticker_symbols()

	# Setup up DB connection
	global db_connect
	db_connect = setup_db_connect(args.db_driver,args.db_name)

	# Create all required DB tables
	if args.create_tables:
		create_db_tables()
		create_db_indices()
		insert_ticker_symbols(tickers)

	# Process all filings in the given RSS feeds one month after another
	for filepath in feeds:

		# Load EDGAR filing metadata from RSS feed (and filter out all non 10-K/10-Q filings or companies without an assigned ticker symbol)
		filings = {}
		for filing in feed_tools.read_feed(filepath):
			if args.cik is None or args.cik == filing['cikNumber']:
				if filing['formType'] in ('10-K','10-K/A','10-Q','10-Q/A') and filing['cikNumber'] in tickers:
					filing['ticker'] = tickers[filing['cikNumber']]
					filings.setdefault(filing['cikNumber'],[]).append(filing)

		# Process the selected XBRL filings
		process_filings(filings)

def collect_feeds(args):
	"""Returns an generator of the resolved, absolute RSS file paths."""
	for filepath in args.rss_feeds:
		for resolved_filepath in glob.iglob(os.path.abspath(filepath)):
			yield resolved_filepath

def main():
	# Parse script arguments
	global args
	args = parse_args()

	build_secdb(collect_feeds(args))

if __name__ == '__main__':
	sec = timeit.timeit(main,number=1)
	logger.info('Finished in %fs',sec)
