Altova RaptorXML+XBRL API Tutorial
==================================

Introduction
------------

[Altova RaptorXML](http://www.altova.com/raptorxml.html) is the third-generation, hyper-fast XML and XBRL processor from the makers of XMLSpy. RaptorXML is built from the ground up to be optimized for the latest standards and parallel computing environments. Designed to be highly cross-platform capable, the engine takes advantage of today's ubiquitous multi-CPU computers to deliver lightning fast processing of XML and XBRL data.

This short tutorial aims to provide a general tour of XBRL processing using the Python API available in Altova RaptorXML and show example solutions to some common problems.
The full RaptorXML Python API documentation can be found at http://manual.altova.com/RaptorXML/pyapiv2/html/.

This tutorial assumes that the reader has already some knowledge of XBRL and a basic understanding of the Python 3 programming language.

Executing scripts in RaptorXML
------------------------------

There are three ways to execute scripts within RaptorXML that provide access to the whole XBRL object model:

* Using callback hooks after validation
* Using the `script` command (available since version 2016)
* Using the `raptorxmlxbrl-python` executable (available since version 2016)

The first method using callbacks can be used in scenarios where only a single instance document needs to be processed. Example applications would be performing custom validation logic, extracting specific data or generating custom documentation for the given instance document.
Depending on the validation command the script must implement a specific function that is called by RaptorXML after the document has been loaded and validated. See the RaptorXML Python API documentation for a list of available callback functions.

The following example illustrates how to implement a simple custom validation rule that requires all context ids to start with `ctx`.

```python
import altova_api.v2.xbrl as xbrl

def on_xbrl_finished(job,instance):
	for context in instance.contexts:
		if not context.id.startswith('ctx'):
			# Report a custom error message
			job.error_log.report(xbrl.Error.create('Context {context} must have an id name starting with "ctx"!',context=context))
```

Now the script can be specified in addition to the standard XBRL validation command using the `--script` option:

	raptorxmlxbrl valxbrl --script=custom_val.py instance.xbrl

If the application logic is more complicated or requires to load and validate multiple documents, then the `script` command might be more appropriate. The `script` command allows the execution of full Python scripts within the RaptorXML engine.
Using the API provided in Altova specific modules, the script can control the loading and validation of documents and access their object model. The `raptorxmlxbrl-python` executable behaves exactly like `raptorxmlxbrl script`.

Regardless of the method used to execute scripts in RaptorXML, all Python scripts must conform to the Python 3.4 language specification.


Importing Altova modules
------------------------

All Altova RaptorXML specific Python modules are available through the `altova_api` package. Within this package there is a separate module for each available RaptorXML API version.
The current version at the time of writing is `v2`. Please refer to the Altova RaptorXML Python API document http://manual.altova.com/RaptorXML/pyapiv2/html/index.html#modules for a list of available modules.
For example, this is a typical way to import the RaptorXML XBRL module:

```python
import altova_api.v2.xbrl as xbrl
```

Alternatively, one can also import all available `v2` modules using a single import statement:

```python
from altova_api.v2 import *
```

Only when using validation commands with script callbacks, an additional alias to `altova_api.v*` is created under the name `altova`. The actual API version can be specified with the `--script-api-version` option.
Thus, if the script is used with the following command line

	raptorxmlxbrl valxbrl --script=custom_val.py --script-api-version=2 instance.xbrl

the next import statement is equivalent with the one above.

```python
from altova import *
```

Installing thrid party modules
------------------------------

Third party modules can be installed using Python's [pip install](https://docs.python.org/3/installing/index.html) command. In the Altova RaptorXML environment `pip` modules can be installed in the following way:

	raptorxmlxbrl-python -m pip install pyodbc

Loading XBRL instances
----------------------

The `xbrl.Instance` Python class represents an XBRL instance document and is the entry point to the object model of the instance and the referenced taxonomies and linkbases (DTS).
An XBRL instance can be loaded either from an URL or a (dynamically generated) byte buffer. The `xbrl.Instance` class provides two class methods for this purpose:

```python
instance,log = xbrl.Instance.create_from_url('/home/user/instance.xbrl')
instance,log = xbrl.Instance.create_from_buffer(b'<xbrl xmlns="http://www.xbrl.org/2003/instance"><!-- Instance content here --></xbrl>')
```

Both methods return a tuple with the newly created `xbrl.Instance` object and an error log. If the instance is not valid, a None object will be returned instead.
The error log contains a list of errors, warnings and inconsistencies that occured during the validation episode. For example, raising an exception in case of errors during validation can be done as follows:

```python
instance,log = xbrl.Instance.create_from_url('/home/user/instance.xbrl')
if not instance: raise Exception('\n'.join(error.text for error in log))
```

If some special URL mappings are required to be able to access the instance or taxonomy documents, an OASIS XML catalog can be specified with the `catalog` parameter:

```python
custom_catalog,log = xml.Catalog.create_from_url('/home/user/custom_catalog.xml')
instance,log = xbrl.Instance.create_from_url('/home/user/instance.xbrl',catalog=custom_catalog)
```

Additional validation options can be specified as further keyword arguments. Please refer to the [RaptorXML CLI documentation](http://manual.altova.com/RaptorXML/raptorxmlxbrlserver/index.html?rxcli_xbrl.htm) or execute `raptorxmlxbrl help valxbrl` to get a list of the available options.
Please note that any hyphens need to be changed to underscores to be valid Python argument names. Please also note that some options controlling additional post-validation steps like formula or table linkbase execution will be ignored.
The following example instructs RaptorXML to abort the validation immediately after the first error has been detected and switches on parallel assessment which tries to utilize all licensed CPU cores during XML validation:

```python
instance,log = xbrl.Instance.create_from_url('/home/user/instance.xbrl',error_limit=1,parallel_assessment=True)
```

Finally, if multiple instance documents are loaded that reference *exactly* the same DTS entry points, the DTS can be preloaded once and reused to validate each instance. This can dramaticaly improve performance, especially when validating lots of small instance files referencing a large taxonomy.

```python
preloaded_dts,log = xbrl.taxonomy.DTS.create_from_url('/home/user/taxonomy.xsd')
instance,log = xbrl.Instance.create_from_url('/home/user/instance.xbrl',dts=preloaded_dts)
```

Processing multiple XBRL instances (in parallel)
------------------------------------------------

Processing all instances in a directory could be written like this:

```python
def process_instance(url):
	# Load and validate instance
	instance, log = xbrl.Instance.create_from_url(url)
	# Do something with instance...

for url in glob.iglob('/home/xbrl/*.xbrl'):
	process_instance(url)
```

The `xbrl.Instance` class methods `create_from_url` and `create_from_buffer` are both blocking calls, meaning that the execution of the Python script will stop and wait until the instance document has been loaded and validated.
RaptorXML's validation engine can utilize all available cores during document validation, but the application logic in the Python script is only executed on the main thread.
Thus, after an instance has been loaded, the script execution continues only on the main thread not utilizing the other cores. It is possible to create additional threads in Python, but
please note that due to a technical limitation of the C Python implementation, threads created in Python are only interleaved and never actually run in parallel.
In order to maximize the through-put of a multi-core system, one can used the additional Python threads to schedule the loading and validation of many instances.
This enables the RaptorXML engine to start loading the next instances utilizing all licensed cores while the Python interpreter is executing the application logic for the available instances on the main thread.

A convenient way to schedule several Python threads on a multi-core system is to use the thread pool implementation in the `concurrent.futures` module.

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
	# Schedule processing of all instances as futures
	futures_to_url = {executor.submit(process_instance,url): url for url in glob.iglob('/home/xbrl/*.xbrl')}
	# Wait for all futures to finish their computation
	for future in concurrent.futures.as_completed(futures_to_url):
		url = future_to_url[future]
		try:
			future.result()
		except:
			print('Processing instance %s failed with an exception!' % url)
```

Retrieving facts
----------------

The `xbrl.Instance` object provides several properties to access the facts in the instance. `facts` will return all the facts, whereas `nil_facts` and `non_nil_facts` will only return facts where `xsi:nil` was set to true or false, respectively.
All three properties return objects of type `xbrl.FactSet` which represent a list of facts in document order and without any duplicates. To print the element name and value of all facts:

```python
for fact in instance.facts:
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

`xbrl.FactSet` objects support the usual special methods for Python containers like `len()`, slices, and iterators. To print only the first ten facts:

```python
print('Total number of facts: %d', len(instance.facts))
for fact in instance.facts[:10]:
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

Use the `xbrl.FactSet.filter()` method to retrieve facts with specific attributes. To list all facts with a particular name:

```python
for fact in instance.facts.filter(xml.QName('NetIncomeLoss','http://fasb.org/us-gaap/2013-01-31')):
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

Alternatively, one can also pass a `xbrl.taxonomy.Concept` object to the `filter()` method. To resolve a QName to an XBRL concept use the `xbrl.taxonomy.DTS.resolve_concept()` method.
There is a slight performance advantage using XBRL concept objects when `filter()` is called multiple times with the same concept (but different contexts or units).

```python
income_concept = instance.dts.resolve_concept(('NetIncomeLoss','http://fasb.org/us-gaap/2013-01-31'))
for fact in instance.facts.filter(income_concept):
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

To limit the search to facts referencing only a particular context, additionally specify the `xbrl.Context` object:

```python
mycontext = instance.context('FD2014Q2YTD')
for fact in instance.facts.filter(xml.QName('NetIncomeLoss','http://fasb.org/us-gaap/2013-01-31'),mycontext):
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

To find facts with a particular unit would work in a similar fashion.

`xbrl.FactSet` also supports the common set operations like intersection, union and set difference. For example, computing all facts that are not 'Assets' could be achieved in such a way:

```python
non_assets_facts = instance.facts - instance.facts.filter(xml.QName('Assets','http://fasb.org/us-gaap/2013-01-31'))
```

Finding facts by aspect values
------------------------------

Facts can also be filterd by particular aspects. Aspects are additional information about a fact described by the associated context or unit. RaptorXML supports the dimensional aspect model as described in http://www.xbrl.org/specification/variables/rec-2009-06-22/variables-rec-2009-06-22+corrected-errata-2013-11-18.html#term-dimensional-aspect-model.
To find facts with certain aspects like a specific period instant date, identifier or dimension value, pass an `xbrl.ConstraintSet` object to the `xbrl.FactSet.filter()` method.
The `xbrl.ConstraintSet` object can combine multiple aspect value constraints and the filter method will return only facts that have those matching aspect values.

To find all facts reproted in US dollar with a particular CIK entity identifier and a 3 month duration ending at 2014/05/31:

```python
constraints = xbrl.ConstraintSet()
constraints[xbrl.Aspect.ENTITY_IDENTIFIER] = xbrl.EntityIdentifierAspectValue('0000815097','http://www.sec.gov/CIK')
constraints[xbrl.Aspect.PERIOD] = xbrl.PeriodAspectValue.from_duration('2014-03-01','2014-05-31')
constraints[xbrl.Aspect.UNIT] = xbrl.UnitAspectValue.from_iso4217_currency('USD')
for fact in instance.facts.filter(constraints):
	print('Fact %s has value %s' % (fact.qname, fact.normalized_value))
```

To restrict those facts further assign additional aspect values to the constraint set. Building on the previous example, to retrieve only facts reported as ForeignExchangeOptionMembers, add a constraint for the FinancialInstrumentAxis explicit dimension:

```python
dim = instance.dts.resolve_concept(('FinancialInstrumentAxis','http://fasb.org/us-gaap/2013-01-31'))
member = instance.dts.resolve_concept(('ForeignExchangeOptionMember','http://fasb.org/us-gaap/2013-01-31'))

# Set the explicit dimension aspect value
constraints[dim] = member
facts = instance.facts.filter(constraints)
```

Adding a typed dimension constraint works in the same way as for explicit dimensions, but obtaining the typed dimension value might require additional work.
Typed dimension values are expressed as XML tree fragments (with the typed dimension domain declaration element as root). Therefore, typed dimension constraints can be set using `xml.ElementInformationItem` objects.
One way to obtain an XML tree fragment is by parsing and validating a buffer with the XML source code using the `xml.Instance.create_from_buffer()` class method. Please note that the XML fragment should be validated against the element declaration in the DTS,
otherwise results might differ due to missing type information (e.g. `<elem>3</elem>` and `<elem>03</elem>` are only equal if they are both of type `xs:integer`).

Here is an example for finding facts reported with client code (CC) 33 in the individual clients (INC) dimension in an EBA taxonomy instance.

```python
dim = instance.dts.resolve_concept(xml.QName('INC','http://www.eba.europa.eu/xbrl/crr/dict/dim'))
# Create an XML document fragment and validate it against the element declarations in the DTS
fragment, log = xml.Instance.create_from_buffer(b'<eba_typ:CC xmlns:eba_typ="http://www.eba.europa.eu/xbrl/crr/dict/typ">33</eba_typ:CC>',schema=instance.dts.schema)

# Set the typed dimension aspect value to the root element of the fragment
constraints[dim] = fragment.document_element
facts = instance.facts.filter(constraints)
```

To retrieve facts reported only with the dimensions values in the constraint set and without any additional dimensions, set the `allow_additional_dimensions` argument to `False`.

```python
facts = instance.facts.filter(constraints,allow_additional_dimensions=False)
```

Implicit filtering
------------------

A common scenario is to find groups of facts that share the same aspect values. One way to achieve this is to construct an `xbrl.ConstraintSet` object by passing in a fact object. This will initialize the constraint set with all the fact's aspect values as constraints.
Then override some of the aspects with more specific values that the matching facts must posses. This technique is similar to the notion of implicit filtering defined in the XBRL Formula specifications.
Here is an example that checks if the accounting equation in a balance sheet (`Assets` must equal `LiabilitiesAndStockholdersEquity`) really holds:

```python
# For each reported total assets value
for assets_fact in instance.facts.filter(xml.QName('Assets','http://fasb.org/us-gaap/2013-01-31')):
	# Find the matching liabilities and equity total value (with the same instant date!)
	constraints = xbrl.ConstraintSet(assets_fact)
	constraints[xbrl.Aspect.CONCEPT] = instance.dts.resolve_concept(('LiabilitiesAndStockholdersEquity','http://fasb.org/us-gaap/2013-01-31'))
	liabilities_equity_fact = instance.facts.filter(constraints)[0]
	# Check if they are both equal (in balance)
	if assets_fact.effective_numeric_value != liabilities_equity_fact.effective_numeric_value:
		print('Balance sheet does not balance!!!')
```

Working with facts
------------------

All `xbrl.Fact` objects provide many convenience properties and methods to access parts of the XBRL data model without the need to work directly with the raw XML. But if needed, the underlying XML element can always be accessed using the `xbrl.Fact.element` property.

```python
fact = instance.facts[0]	# Get the first fact in the instance
fact.id						# Get the id attribute value (using a convenience property)
fact.element.find_attribute('id').normalized_value	# Get the id attribute value directly from the raw XML
```

Other common properties available on the `xbrl.Fact` object are:

```python
fact.qname					# Returns an `xml.QName` object containing the XML element name and namespace.
fact.xsi_nil				# Returns True if the `xsi:nil` attribute on the XML element was set to true.
fact.concept				# Returns an `xbrl.taxonomy.Concept` object.
fact.footnotes(lang='en')	# Returns an iterator over all footnotes in English that are associated with this fact.
```

To check if a fact is an item or tuple use the `isinstance()` method:

```python
if isinstance(fact,xbrl.Item):
	print('Fact is an item')
elif isinstance(fact,xbrl.Tuple):
	print('Fact is a tuple')
```

Item facts (as opposed to tuples) provide additional properties to access the referenced context and unit as well as the fact's value.

```python
fact.context			# Returns an `xbrl.Context` object
fact.unit				# Returns an `xbrl.Unit` object. Might be None in case of non-numeric items.
fact.normalized_value	# Returns the fact's value as a string (always available).

# Numeric facts expose additional properties to retrieve the value as a decimal number.
if fact.concept.is_numeric():
	fact.numeric_value				# Returns a `decimal.Decimal` object with the value as written in the XML without any rounding.
	fact.effective_numeric_value	# Returns a `decimal.Decimal` object with the rounded value after taking the precision into account.
```

Tuples don't store any values directly but group together other child facts. All facts directly contained in a tuple can be accessed using `xbrl.Tuple.child_facts`.


DTS
---

The discoverable taxonomy set (DTS) is a set of XBRL taxonomies and linkbases can be found using the XBRL 2.1 discovery rules starting at the instance.
The DTS can be accessed using the `xbrl.Instance.dts` property. `xbrl.taxonomy.DTS` exposes the object model for all available concepts as well as the resources and networks of relationships defined in linkbases.
For example, the `xbrl.taxonomy.DTS.documents` property lists all documents that are contained in the DTS.

```python
for doc in instance.dts.documents:
	print(doc.uri)
```

Taxonomy concepts
-----------------

The `xbrl.taxonomy.DTS.concepts` property returns an iterator over all the XBRL concepts defined within the DTS.

```python
# List all concepts availabe in the DTS
for concept in instance.dts.concepts:
	# The qname property returns the XML name and namespace of the concept
	print(concept.qname)
```

There are several different types of XBRL concepts. The XBRL 2.1 specification defines item and tuple concepts. Further, the XBRL Dimensions 1.0 specification defined hypercube and dimension concepts.
Concepts of those different types can be accessed using the `items`, `tuples`, `hypercubes` and `dimensions` properties. To check if a concept object is of a particular type, use the `isinstance()` method:

```python
if isinstance(concept,xbrl.xdt.Dimension):
	print('Concept %s is a dimension' % concept.qname)
```

To get a particular concept by name, call the `xbrl.taxonomy.DTS.resolve_concept()` method with an XML QName:

```python
assets_concept = instance.dts.resolve_concept(xml.QName('Assets','http://fasb.org/us-gaap/2013-01-31'))
```

The `xbrl.taxonomy.Concept` class provides all the XSD Element Declaration properties as defined by the XSD Schema 1.1 specification. For more details see http://www.w3.org/TR/xmlschema11-1/#Element_Declaration_details.
For example, to check if a concept is abstract:

```python
if concept.abstract:
	print('Concept %s is an abstract concept' % concept.qname)
```

Any assigned concept labels can be retrieved with the `xbrl.taxonomy.Concept.labels()` method.

```python
for label in concept.labels():
	print('Label language: %s' % label.xml_lang)
	print('Label role: %s', % label.xlink_role)
	print('Label text: %s', % label.text)
```

The `labels()` method also allows to filter labels according to label language and label role. The `xbrl.taxonomy.Concept.references()` method works in a simliar way.

```python
text = next(concept.labels(lang='en-US',label_role='http://www.xbrl.org/2003/role/totalLabel')).text
```

`xbrl.taxonomy.Item` class exposes additional properties applicable only to XBRL item concepts. Here are a few examples:

```python
concept.is_non_numeric()	# Returns True if the concept has a non-numeric item type.
concept.is_numeric()		# Returns True if the concept has a numeric item type.
concept.is_monetary()		# Returns True if the concept has the monetaryItemType type.

concept.item_type			# Returns the concept's item type as an enumeration value
concept.item_type == xbrl.taxonomy.ItemType.MONETARY	# equivalent to concept.is_monetary()

concept.period_type			# Returns the period type as an enumeration value
if concept.period_type == xbrl.taxonomy.PeriodType.INSTANT:
	print('Concept %s can only be reported with an instant period context.' % concept.qname) 

concept.balance				# Returns the balance type as an enumeration value
if concept.balance == xbrl.taxonomy.Balance.DEBIT:
	print('Concept %s has a debit balance.' % concept.qname) 
elif concept.balance == xbrl.taxonomy.Balance.CREDIT:
	print('Concept %s has a credit balance.' % concept.qname) 
```

Networks of relationships
-------------------------

The XBRL 2.1 specification groups all linkbase XLink arcs in into separate base sets according to the arc's element name, arcrole and the containing extended link element.
Arcs within a base set express one or more relationships between concepts and/or resources. Those relationships can override or prohibit other relationships within the base set that have lower priority.
Only the effective relationships are then used within the final network of relationships, e.g. the visible presentation tree.

RaptorXML's engine does all the required XLink processing and provides a simple API to traverse any network of relationships including the standard presentation and calculation networks.

The next example shows a generic solution to traverse all the concepts in depth-first order in acyclic network of relationships (like the standard presentation and calculation networks).

```python
def traverse_node(network,concept):
	# Do something with the concept
	print(concept.qname)
	# Iterate over all relationships starting at this concept
	for rel in network.relationships_from(concept):
		traverse_node(network,rel.target)

def traverse_tree(network):
	# Traverse the network (tree) starting from each root concept
	for concept in network.roots:
		traverse_node(network,concept)

# Traverse the standard presentation tree with the given linkrole
traverse_tree(dts.network_of_relationships(linkrole))
```

Listing available linkroles
---------------------------

`xbrl.taxonomy.DTS.link_roles()` can be used to get all available link roles used within the DTS:

```python
for linkrole in instance.dts.link_roles():
	print(linkrole)
```

It is also possible to get only the link roles used in a specific linkbase, for example the presentation linkbase:

```python
for linkrole in instance.dts.presentation_link_roles():
	print(linkrole)
```

Each non-standard link role must be declared using a `<link:roleType>` element in the taxonomy. This construct might also contain an additional human readable definition string. The definition string can be accessed from the `xbrl.taxonomy.RoleType` object:

```python
def link_role_label(dts,linkrole):
	# Get the RoleType object for the link role
	roletype = dts.role_type(linkrole)
	# Check if it contains a definition object
	if roletype and roletype.definition:
		# Get the definition string value
		return roletype.definition.value
	return linkrole

for linkrole in instance.dts.link_roles():
	print(link_role_label(instance.dts,linkrole))
```

The XBRL 2.1 standard does not allow multiple definition strings, thus multi-language definition strings are not possible. In a later specification, Generic Labels have been introduced to allow to assign labels to any XML constructs within a taxonomy or linkbase.
As a workaround, a taxonomy could assign multi-language labels to `<link:roleType>` elements using generic labels. For such taxonomies, the generic labels can be easily retrieved using the `xbrl.taxonomy.RoleType.labels()` method.

```python
for roletype in instance.dts.role_types:
	# Get a list of spanish labels for this RoleType object
	labels = list(roletype.labels(lang='es'))
	if labels:
		# Print the text of the first label
		print(labels[0].text)
```

Display presentation trees
--------------------------

Below is an example how to display the content of the presentation linkbase. Please note that presentation relationship objects have an additional `preferred_label` property which contains the label role that should be used when displaying the line item caption.

```python
def label(concept,lang=None,label_role=None):
	# Use the XBRL standard label role if no preferred label role was specified
	labels = concept.labels(lang=lang,label_role=label_role if label_role else 'http://www.xbrl.org/2003/role/label')
	try:
		# Return the first label found
		return next(labels).text
	except StopIteration:
		# If no label was found return the XML element name
		return str(concept.qname)

def print_presentation_node(network,concept,lang,preferred_label=None,depth=0):
	# Print the concept label (indented by the tree depth)
	print('\t'*depth, label(concept,lang,preferred_label))
	# Iterate over all relationships starting at this concept
	for rel in network.relationships_from(concept):
		print_presentation_node(network,rel.target,lang,rel.preferred_label,depth+1)

def print_presentation_tree(network,lang=None):
	# Traverse the presentation network (tree) starting from each root concept
	for concept in network.roots:
		print_presentation_node(network,concept,lang)

# Print all the presentation trees in the standard presentation linkbase
for linkrole in instance.dts.presentation_link_roles():
	print(link_role_label(instance.dts,linkrole))
	print_presentation_tree(instance.dts.presentation_network(linkrole),lang='en-US')
```

Display calculation trees
-------------------------

Below is an example how to display the content of the calculation linkbase. Please note that calculation relationship objects have an additional `weight` property which contains the weight of the child (target) concept.

```python
def print_calculation_node(network,concept,lang,weight=None,depth=0):
	# Print the weight and concept label (indented by the tree depth)
	print('\t'*depth, weight if weight else '', label(concept,lang))
	# Iterate over all relationships starting at this concept
	for rel in network.relationships_from(concept):
		print_calculation_node(network,rel.target,lang,rel.weight,depth+1)

def print_calculation_tree(network,lang=None):
	# Traverse the calculation network (tree) starting from each root concept
	for concept in network.roots:
		print_calculation_node(network,concept,lang)

# Print the standard calculation tree with the given linkrole
for linkrole in instance.dts.calculation_link_roles():
	print(link_role_label(instance.dts,linkrole))
	print_calculation_tree(instance.dts.calculation_network(linkrole),lang='en-US')
```

Dimensional Relationship Set
----------------------------

The [XBRL Dimensions 1.0 specification](http://www.xbrl.org/specification/dimensions/rec-2012-01-25/dimensions-rec-2006-09-18+corrected-errata-2012-01-25-clean.html) is an extension to the core XBRL language.
It standardizes how to report and validate facts that are broken down across different dimensions (e.g. different regions or products). To do so, it introduced new arcroles for definition arcs that are used to build a Dimensional Relationship Set.
A Dimensional Relationship Set (DRS) expresses the possible dimensions and their domain values that can be used with a specific XBRL concept. The `xbrl.xdt.DRS` class provides a special API to query and navigate the DRS.

The following code show a generic way to traverse the consecutive relationships in the DRS:

```python
def traverse_consecutive_relationships(drs,rel):
	# Do something with the relationship
	print(rel.source.qname,'-->',rel.target.qname)
	for rel2 in drs.consecutive_relationships(rel):
		traverse_consecutive_relationships(drs,rel2)

drs = instance.dts.dimensional_relationship_set()
for linkrole in drs.link_roles():
	print(linkrole)
	for item in drs.roots(linkrole):
		for rel in drs.hashypercube_relationships(item,linkrole):
			traverse_consecutive_relationships(drs,rel)
```

The next example shows how to calculate the [effective domain](http://www.xbrl.org/specification/dimensions/rec-2012-01-25/dimensions-rec-2006-09-18+corrected-errata-2012-01-25-clean.html#term-effective-domain) for all dimensions in the DTS.

```python
def collect_usable_domain_members(drs,rel,usable_members):
	if rel.usable:
		usable_members.add(rel.target)
	for rel2 in drs.consecutive_relationships(rel):
		collect_usable_domain_members(drs,rel2,usable_members)

def dimension_domain(drs,dim):
	usable_members = set()
	# Get all usable domain members in all link roles
	for linkrole in drs.link_roles(dim):
		for rel in drs.dimension_domain_relationships(dim,linkrole):			
			collect_usable_domain_members(drs,rel,usable_members)
	return usable_members

# List all explicit dimensions and their effective domains
drs = instance.dts.dimensional_relationship_set()
for dim in instance.dts.dimensions:
	if dim.is_explicit():
		domain_members = dimension_domain(drs,dim)
		if domain_members:
			print(dim.qname)
			for member in domain_members:
				print('\t',member.qname)
```

CSV export
----------

Data from an XBRL instance can be easily exported in the CSV format using the Python [csv](https://docs.python.org/3/library/csv.html) module. Here is a simple example exporting all fact elements:

```python
import csv

with open(r'/home/user/facts.csv','w',newline='') as csvfile:
	writer = csv.writer(csvfile)
	# Write header row
	header = ['Name','Namespace','Context','Unit','Value']
	writer.writerow(header)

	# Export all facts
	for fact in instance.facts:
		data = [fact.qname.local_name,fact.qname.namespace_name,fact.contextRef,fact.unitRef,fact.normalized_value]
		writer.writerow(data)
```

XSLX export
-----------

The standard Python distribution does not include a module for writing .xslx files, but there are several third party modules which provide this functionality.
One popular third party module is [XlsxWriter](http://xlsxwriter.readthedocs.org/). To be able to use it with the RaptorXML Python API, it needs to be first installed using `pip install`:

	raptorxmlxbrl-python -m pip install XlsxWriter

The following code demonstrates how to create a simple worksheet containing all fact in the instance:

```python
import xlsxwriter

with xlsxwriter.Workbook(r'/home/user/facts.xlsx') as workbook:
	worksheet = workbook.add_worksheet('Facts')

	# Write header row
	header = ['Name','Namespace','Context','Unit','Value']
	worksheet.write_row('A1',header)

	# Export all facts
	for row, fact in enumerate(instance.facts):
		data = [fact.qname.local_name,fact.qname.namespace_name,fact.contextRef,fact.unitRef,fact.normalized_value]
		worksheet.write_row(row+1,0,data)
```

SQLite export
-------------

Python has already builtin support for reading and writing to SQLite 3 databases using the [sqlite3](https://docs.python.org/3/library/sqlite3.html) module. The following code demonstrates how to create a simple database with a table containing all facts in the instance:

```python
import sqlite3

with sqlite3.connect(r'/home/user/facts.db3') as con:

	# Create table
	con.execute('CREATE TABLE facts (name TEXT, namespace TEXT, context TEXT, unit TEXT, value TEXT)')

	# Export all facts
	for fact in instance.facts:
		data = [fact.qname.local_name,fact.qname.namespace_name,fact.contextRef,fact.unitRef,fact.normalized_value]
		con.execute('INSERT INTO facts VALUES(?,?,?,?,?)',data)
```