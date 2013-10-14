import java.util.*;
import java.util.regex.*;
import java.io.FileNotFoundException;
import com.esotericsoftware.wildcard.Paths;

class DBSyncOper
{
	public enum Scope { SCOPE_GLOBAL, SCOPE_SOURCE, SCOPE_DESTINATION };

	class SortTable implements Reader
	{
		private String tablename;
		private String conn;
		private DB.DBOper oper;
		private TreeMap<String,LinkedHashMap<String,String>> sortedmap;
		private Iterator<?> iterator;
		private String instance;
		private ArrayList<String> header;

		private TreeMap<String,LinkedHashMap<String,String>> getMap()
		{
			return new TreeMap<String,LinkedHashMap<String,String>>(db.collator);
		}

		public SortTable(XML xml,Sync sync,String name,ArrayList<String> header) throws Exception
		{
			instance = "SORT:" + name;
			this.header = header;

			XML sortxml = xml.getElement("dbsyncsorttable");
			if (sortxml == null)
			{
				xml = xml.getParent();
				sortxml = xml.getElement("dbsyncsorttable");
			}
			if (sortxml == null)
				sortxml = xml.getParent().getElement("dbsyncsorttable");
			if (sortxml != null)
			{
				tablename = sortxml.getAttribute("name");
				conn = sortxml.getAttribute("instance");
			}

			if (tablename == null || conn == null)
			{
				sortedmap = getMap();
				Misc.log(7,"Initializing memory sort");
			}
			else
				Misc.log(7,"Initializing temporary DB table sort");

			put(sync);
		}

		public void put(Sync sync) throws Exception
		{
			LinkedHashMap<String,String> row;
			while((row = fields.getNext(sync)) != null)
				put(row);
		}

		public void put(LinkedHashMap<String,String> row) throws Exception
		{
			String key = getKey(row);
			if (sortedmap != null)
			{
				sortedmap.put(key,row);
				return;
			}

			XML xml = new XML();
			xml.add("row",row);

			String value = xml.rootToString();
			String sql = "insert into " + tablename + " values (" + DB.replacement + "," + DB.replacement + ")";

			ArrayList<String> list = new ArrayList<String>();
			list.add(key);
			list.add(value);

			db.execsql(conn,sql,list);
		}

		public LinkedHashMap<String,String> next() throws Exception
		{
			LinkedHashMap<String,String> row;

			if (sortedmap != null)
			{
				if (iterator == null)
					iterator = sortedmap.keySet().iterator();
				if (!iterator.hasNext()) return null;
				Object key = iterator.next();
				row = sortedmap.get(key);

				if (Misc.isLog(15)) Misc.log("row [memsort]: " + row);
				return row;
			}

			if (oper == null)
			{
				String sql = "select key,value from " + tablename + db.getorderby(conn,new String[]{"key"},ignorecasekeys);
				oper = db.makesqloper(conn,sql);
			}

			LinkedHashMap<String,String> result = oper.next();
			if (result == null)
			{
				db.execsql(conn,"truncate table " + tablename);
				return null;
			}

			XML xml = new XML(new StringBuffer(result.get("VALUE")));
			XML[] elements = xml.getElements(null);

			row = new LinkedHashMap<String,String>();
			for(XML el:elements)
			{
				String value = el.getValue();
				if (value == null) value = "";
				row.put(el.getTagName(),value);
			}

			if (Misc.isLog(15)) Misc.log("row [dbsort]: " + row);
			return row;
		}

		public ArrayList<String> getHeader()
		{
			return header;
		}

		public String getName()
		{
			return instance;
		}
	}

	class Sync
	{
		private XML xml;
		protected Reader reader;
		private CsvWriter csvout;

		public Sync(XML xml) throws Exception
		{
			this.xml = xml;

			String dumpcsvfilename = xml.getAttribute("dumpcsvfilename");
			if (dumpcsvfilename != null) csvout = new CsvWriter(dumpcsvfilename,xml);

			// Important: reader must be set by extended constructor class
		}

		public LinkedHashMap<String,String> next() throws Exception
		{
			return reader.next();
		}

		public ArrayList<String> getHeader()
		{
			return reader.getHeader();
		}

		public XML getXML()
		{
			return xml;
		}

		public CsvWriter getCsvWriter()
		{
			return csvout;
		}

		public Reader getReader()
		{
			return reader;
		}
	}

	class SyncClass extends Sync
	{
		public SyncClass(XML xml) throws Exception
		{
			super(xml);

			reader = ReaderUtil.getReader(xml);

			String sorted = xml.getAttribute("sorted");
			if (sorted != null && sorted.equals("true")) return;

			reader = new SortTable(xml,this,reader.getName(),reader.getHeader());
		}
	}

	class SyncCsv extends Sync
	{
		public SyncCsv(XML xml) throws Exception
		{
			super(xml);

			reader = new ReaderCSV(xml);

			String sorted = xml.getAttribute("sorted");
			if (sorted != null && sorted.equals("true")) return;

			reader = new SortTable(xml,this,reader.getName(),reader.getHeader());
		}
	}

	class SyncSql extends Sync
	{
		private String conn;

		public SyncSql(XML xml) throws Exception
		{
			super(xml);

			conn = xml.getAttribute("instance");
			if (conn == null)
				throw new AdapterException(xml,"Attribute 'instance' required for database syncer");

			XML sqlxml = xml.getElement("extractsql");
			String sql = sqlxml == null ? xml.getValue() : sqlxml.getValue();

			String restrictsql = xml.getValue("restrictsql",null);
			if (restrictsql != null)
			{
				if (restrictsql.indexOf("%SQL%") != -1)
					sql = restrictsql.replace("%SQL%",sql);
				else
					sql = "select * from (" + sql + ") d3 where " + restrictsql;
			}

			sql = sql.replace("%LASTDATE%",lastdate == null ? "" : Misc.dateformat.format(lastdate));
			sql = sql.replace("%STARTDATE%",startdate == null ? "" : Misc.dateformat.format(startdate));
			sql = sql.replace("%NAME%",syncname);

			String filtersql = xml.getAttribute("filter");
			if (filtersql != null)
			{
				if (filtersql.indexOf("%SQL%") != -1)
					sql = filtersql.replace("%SQL%",sql);
				else
					sql = "select * from (" + sql + ") d2 where " + filtersql;
			}

			String sorted = xml.getAttribute("sorted");
			boolean issorted = sorted != null && sorted.equals("true");
			if (!issorted)
			{
				sql = "select * from (" + sql + ") d1";
				Set<String> keys = fields.getKeys();
				sql += db.getorderby(conn,keys.toArray(new String[keys.size()]),ignorecasekeys);
			}


			String presql = xml.getValue("preextractsql",null);
			if (presql != null)
				db.execsql(conn,presql);

			// Overwrite default reader
			reader = new ReaderSQL(conn,sql,fields.getKeys(),issorted);
		}
	}

	class SyncSoap extends Sync
	{
		private XML xml;

		public SyncSoap(XML xml) throws Exception
		{
			super(xml);

			XML request = xml.getElement("element");
			XML function = xml.getElement("function");
			if (request == null || function == null)
				throw new AdapterException(xml,"Invalid sync call");

			Subscriber sub = new Subscriber(function);
			XML result = sub.run(request.getElement(null).copy());
			reader = new ReaderXML(xml,result);

			reader = new SortTable(xml,this,reader.getName(),reader.getHeader());
		}
	}

	class SyncXML extends Sync
	{
		private XML xml;

		private void Read(XML xml,XML xmlsource) throws Exception
		{
			Reader reader = ReaderUtil.getReader(xml);

			LinkedHashMap<String,String> row;

			XML xmltable = xmlsource.add(reader.getName());

			while((row = reader.next()) != null)
				xmltable.add("row",row);
		}

		public SyncXML(XML xml,XML xmlsource) throws Exception
		{
			super(xml);

			String name = xml.getAttribute("name");
			String filename = xml.getAttribute("filename");

			if (filename == null)
			{
				if (xmlsource == null)
				{
					xmlsource = new XML();
					xmlsource.add(name == null ? "root" : name);
				}

				XML[] elements = xml.getElements(null);
				for(XML element:elements)
				{
					String tagname = element.getTagName();

					if (tagname.equals("element"))
						xmlsource.add(element);
					else if (tagname.equals("function"))
					{
						Subscriber sub = new Subscriber(element);

						Operation.ResultTypes resulttype = Operation.getResultType(element);
						XML xmlresult = sub.run(xmlsource);
						switch(resulttype)
						{
						case LAST:
							xmlsource = xmlresult;
							break;
						case MERGE:
							xmlsource.add(xmlresult);
							break;
						}
					}
					else if (tagname.equals("read"))
						Read(element,xmlsource);

					if (Misc.isLog(9)) Misc.log("XML Sync " + tagname + ": " +xmlsource);
				}
			}
			else
				xmlsource = new XML(filename);

			reader = new ReaderXML(xml,xmlsource);

			String sorted = xml.getAttribute("sorted");
			if (sorted != null && sorted.equals("true")) return;

			reader = new SortTable(xml,this,reader.getName(),reader.getHeader());
		}
	}

	class Field
	{
		class Preload
		{
			private HashMap<String,String> table;
			private HashMap<String,String> datetable;
			private Set<String> fields;
			private boolean usekeywhennotfound = false;
			private String datefield;

			Preload(XML xml) throws Exception
			{
				if (Misc.isLog(15)) Misc.log("Field: Doing preload for " + name);

				String attr = xml.getAttribute("use_key_when_not_found");
				if (attr != null && !"lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Invalid use_key_when_not_found attribute");
				if (attr != null && attr.equals("true"))
					usekeywhennotfound = true;

				datefield = xml.getAttribute("date_field");
				if (datefield != null && !"merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Invalid date_field attribute");
				if (datefield == null && "merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Attribute date_field mandatory for preload");

				Reader reader = null;
				try
				{
					reader = ReaderUtil.getReader(xml);
				}
				catch(FileNotFoundException ex)
				{
					String onnotfound = xml.getAttribute("on_not_found");
					if (onnotfound == null || onnotfound.equals("exception"))
						Misc.rethrow(ex);
					else if (onnotfound.equals("ignore"))
						return;
					else if (onnotfound.equals("warning"))
					{
						Misc.log("WARNING: Ignoring lookup operation since file not found");
						return;
					}
					else if (onnotfound.equals("error"))
					{
						Misc.log("ERROR: Ignoring lookup operation since file not found");
						return;
					}
					else
						throw new AdapterException("Invalid on_not_found attribute",xml);
				}

				LinkedHashMap<String,String> result;
				if (reader != null) while((result = reader.next()) != null)
				{
					String value = result.get(name);
					String datevalue = null;
					if (value == null)
						throw new AdapterException(xml,"Preload query doesn't return " + name + " field");

					if (table == null) table = new HashMap<String,String>();
					if (datetable == null) datetable = new HashMap<String,String>();

					if (fields == null)
					{
						fields = result.keySet();
						fields.remove(name);
						if (datefield != null)
						{
							if (!result.containsKey(datefield))
								throw new AdapterException(xml,"Preload must return date_field " + datefield);
							datevalue = result.get(datefield);
							fields.remove(datefield);
						}
						if (fields.isEmpty())
							throw new AdapterException(xml,"Preload query must return more than just " + name + " field");
					}

					String keyvalue = Misc.getKeyValue(fields,result);
					if (keyvalue == null) continue;

					if (Misc.isLog(25)) Misc.log("Field: Storing preload for " + name + " key " + keyvalue + ": " + value);

					if (table.get(keyvalue) != null)
					{
						Misc.log("ERROR: Preload for " + name + " returned more than one entries for key " + keyvalue);
						value = null;
					}

					if (value == null || "".equals(value)) continue;

					table.put(keyvalue,value);
					if (datevalue != null) datetable.put(keyvalue,datevalue);
				}

				if (table == null)
					Misc.log("WARNING: Preload for field '" + name + "' returned empty result");
			}

			public String getPreload(LinkedHashMap<String,String> values) throws Exception
			{
				if (fields == null) return null;
				String keyvalue = Misc.getKeyValue(fields,values);
				if (keyvalue == null) return null;

				if (Misc.isLog(25)) Misc.log("Field: Preload key for " + name + " is " + keyvalue);

				String result = null;
				if (usekeywhennotfound) result = keyvalue;

				if (table == null || keyvalue == null) return result;

				String lookupvalue = table.get(keyvalue);
				if (Misc.isLog(25)) Misc.log("Field: Getting preload for " + name + " key " + keyvalue + ": " + lookupvalue);

				if (lookupvalue != null) result = lookupvalue;

				if (datefield == null) return result;

				if (!values.containsKey(datefield))
					throw new AdapterException("Extraction must return a value for date_field " + datefield);
				String datevalue = values.get(datefield);
				if (datevalue == null) return result;
				String mergedatevalue = datetable.get(keyvalue);
				if (mergedatevalue == null) return null;

				Date date = Misc.dateformat.parse(datevalue);
				Date mergedate = Misc.dateformat.parse(mergedatevalue);

				if (date.after(mergedate)) return null;
				return result;
			}
		}

		private String defaultvalue;
		private String defaultlookupvalue;
		private XML xmlfield;
		private XML xmllookup;
		private XML xmllookupmerge;
		private XML xmllookupinclude;
		private XML xmllookupexclude;
		private String script;
		private String typeoverwrite;
		private String name;
		private String newname;
		private boolean dostrip = false;
		private String copyname;
		private String filtername;
		private String filterresult;
		private boolean iskey = false;
		private Preload preloadinfo;
		private Preload preloadinfomerge;

		private HashSet<String> excludetable;
		private Set<String> excludefields;
		private boolean excludefield;
		private HashSet<String> includetable;
		private Set<String> includefields;
		private boolean includeexcludefield;

		private String onnotfound;
		private String synclist[];
		private Scope scope;

		public Field(XML xml,Scope scope) throws Exception
		{
			set(xml,scope);
		}

		public void set(XML xml,Scope scope) throws Exception
		{
			xmlfield = xml;
			this.scope = scope;
			name = xml.getAttribute("name");
			newname = xml.getAttribute("rename");
			String strip = xml.getAttribute("strip");
			if (strip != null && strip.equals("true")) dostrip = true;
			copyname = xml.getAttribute("copy");
			filtername = xml.getAttribute("filter");
			filterresult = xml.getAttribute("filter_result");
			onnotfound = xml.getAttribute("on_not_found");
			String forsync = xml.getAttribute("for_sync");
			if (forsync != null) synclist = forsync.split("\\s*,\\s*");

			script = xml.getValue("script",null);
			XML xmldefault = xml.getElement("default_lookup");
			if (xmldefault != null)
			{
				Reader reader = ReaderUtil.getReader(xmldefault);
				LinkedHashMap<String,String> result = reader.next();
				if (result != null)
					defaultlookupvalue = Misc.getFirstValue(result);

				if (Misc.isLog(10)) Misc.log("Field: Default " + name + " value from lookup: " + defaultlookupvalue);
			}

			if (xml.isAttribute("default"))
			{
				defaultvalue = xml.getAttribute("default");
				if (defaultvalue == null) defaultvalue = "";
				if (Misc.isLog(10)) Misc.log("Field: Default " + name + " value: " + defaultvalue);
			}

			typeoverwrite = xml.getAttribute("type");

			xmllookup = xml.getElement("lookup");
			if (xmllookup != null && !Misc.substitutepattern.matcher(xmllookup.toString()).find())
			{
				preloadinfo = new Preload(xmllookup);
				xmllookup = null;
			}

			xmllookupmerge = xml.getElement("merge_lookup");
			if (xmllookupmerge != null && !Misc.substitutepattern.matcher(xmllookupmerge.toString()).find())
			{
				preloadinfomerge = new Preload(xmllookupmerge);
				xmllookupmerge = null;
			}

			xmllookupinclude = xml.getElement("include_lookup");
			if (xmllookupinclude != null && !Misc.substitutepattern.matcher(xmllookupinclude.toString()).find())
			{
				inLookupInit(xmllookupinclude,true);
				xmllookupinclude = null;
			}

			xmllookupexclude = xml.getElement("exclude_lookup");
			if (xmllookupexclude != null && !Misc.substitutepattern.matcher(xmllookupexclude.toString()).find())
			{
				inLookupInit(xmllookupexclude,false);
				xmllookupexclude = null;
			}
		}

		public Field(String name,boolean iskey) throws Exception
		{
			this.iskey = iskey;
			this.name = name;
		}

		public XML getXML()
		{
			return xmlfield;
		}

		private void inLookupInit(XML xml,boolean def) throws Exception
		{
			if (xml == null) return;

			String operstr = def ? "include" : "exclude";

			HashSet<String> table = new HashSet<String>();
			Set<String> fields = null;

			if (Misc.isLog(15)) Misc.log("Field: Doing " + operstr + " for " + name);

			Reader reader = ReaderUtil.getReader(xml);

			LinkedHashMap<String,String> result;
			while((result = reader.next()) != null)
			{
				if (fields == null) fields = result.keySet();

				String keyvalue = Misc.getKeyValue(fields,result);
				if (keyvalue == null) continue;

				if (Misc.isLog(25)) Misc.log("Field: Storing " + operstr + " for " + name + " key " + keyvalue);

				table.add(keyvalue);
			}

			String scope = xml.getAttribute("exclude_scope");
			boolean onfield = false;
			if (scope != null && scope.equals("field"))
				onfield = true;

			if (def)
			{
				includefields = fields;
				includetable = table;
				includeexcludefield = onfield;
			}
			else
			{
				excludefields = fields;
				excludetable = table;
				excludefield = onfield;
			}
		}

		public boolean isIncludeRecord(LinkedHashMap<String,String> result) throws Exception
		{
			if (includeexcludefield) return true;
			return isInLookup(result,includetable,includefields,true);
		}

		public boolean isExcludeRecord(LinkedHashMap<String,String> result) throws Exception
		{
			if (excludefield) return false;
			return isInLookup(result,excludetable,excludefields,false);
		}

		public boolean isIncludeField(LinkedHashMap<String,String> result) throws Exception
		{
			if (!includeexcludefield) return true;
			return isInLookup(result,includetable,includefields,true);
		}

		public boolean isExcludeField(LinkedHashMap<String,String> result) throws Exception
		{
			if (!excludefield) return false;
			return isInLookup(result,excludetable,excludefields,false);
		}

		public boolean isInLookup(LinkedHashMap<String,String> result,HashSet<String> table,Set<String> fields,boolean def) throws Exception
		{
			if (fields == null) return def;

			XML xml = def ? xmllookupinclude : xmllookupexclude;
			if (xml != null)
			{
				String value = lookupFor(xml,result);
				return value != null;
			}

			String keyvalue = Misc.getKeyValue(fields,result);
			if (keyvalue == null) return !def;

			boolean contains = table.contains(keyvalue);
			if (Misc.isLog(25)) Misc.log("Field: Getting " + (def ? "include" : "exclude") + " for " + name + " key " + keyvalue + ": " + contains);

			return contains;
		}

		public String getName()
		{
			return name;
		}

		public String getNewName()
		{
			return newname;
		}

		public boolean isStrip()
		{
			return dostrip;
		}

		public String getCopyName()
		{
			return copyname;
		}

		public String lookupMerge(LinkedHashMap<String,String> row) throws Exception
		{
			return lookupFor(xmllookupmerge,row);
		}

		public String lookup(LinkedHashMap<String,String> row) throws Exception
		{
			return lookupFor(xmllookup,row);
		}

		public String lookupFor(XML xml,final LinkedHashMap<String,String> row) throws Exception
		{
			if (xml == null) return null;

			if (Misc.isLog(15)) Misc.log("Field: Doing lookup for " + name);

			String type = xml.getAttribute("type");
			if (!"db".equals(type)) throw new AdapterException(xml,"Unsupported on demand lookup type " + type);

			String sql = xml.getValue();
			String instance = xml.getAttribute("instance");

			String str = Misc.substitute(sql,new Misc.Substituer() {
				public String getValue(String param) throws Exception
				{
					String value = param.startsWith("$") ? XML.getDefaultVariable(param) : row.get(param);
					return db.getValue(value);
				}
			});

			DB.DBOper oper = db.makesqloper(instance,str);

			LinkedHashMap<String,String> result = oper.next();
			String value = Misc.getFirstValue(result);

			oper.close();

			return value;
		}

		public String executeScript(Map<String,String> map) throws Exception
		{
			return Script.execute(script,map);
		}

		public String getDefault(Map<String,String> map) throws Exception
		{
			if (defaultlookupvalue != null) return defaultlookupvalue;
			return Misc.substitute(defaultvalue,map);
		}

		public String getDefault() throws Exception
		{
			if (defaultlookupvalue != null) return defaultlookupvalue;
			return Misc.substitute(defaultvalue);
		}

		public Preload getPreload()
		{
			return preloadinfo;
		}

		public Preload getPreloadMerge()
		{
			return preloadinfomerge;
		}

		public String getType()
		{
			if (typeoverwrite != null) return typeoverwrite;
			if (iskey) return "key";
			return null;
		}

		public boolean isKey()
		{
			return iskey;
		}

		public boolean isValid(Sync sync) throws Exception
		{
			boolean result = isValidSync(synclist,sync);
			if (!result) return false;
			if (scope == Scope.SCOPE_SOURCE && !sync.getXML().getTagName().equals("source")) return false;
			if (scope == Scope.SCOPE_DESTINATION && !sync.getXML().getTagName().equals("destination")) return false;
			return true;
		}

		public boolean isValidFilter(LinkedHashMap<String,String> result) throws Exception
		{
			if (filtername == null && filterresult == null) return true;

			XML xml = new XML();
			xml.add("root",result);
			if (Misc.isLog(30)) Misc.log("Looking for filter " + filtername + " [" + filterresult + "]: " + xml);

			return Misc.isFilterPass(xmlfield,xml);
		}
	}

	class Fields
	{
		private LinkedHashSet<String> keyfields;
		private LinkedHashSet<String> namefields;
		private LinkedHashMap<String,Field> fields;

		public Fields(String[] keyfields) throws Exception
		{
			this.keyfields = new LinkedHashSet<String>(Arrays.asList(keyfields));
			this.namefields = new LinkedHashSet<String>(Arrays.asList(keyfields));
			fields = new LinkedHashMap<String,Field>();
			for(String keyfield:keyfields)
				fields.put(keyfield,new Field(keyfield,true));
		}

		public void addDefaultVar(XML xml) throws Exception
		{
			XML.setDefaultVariable(xml.getAttribute("name"),xml.getAttribute("value"));
		}

		public void removeDefaultVar(XML xml) throws Exception
		{
			XML.setDefaultVariable(xml.getAttribute("name"),null);
		}

		public void add(XML xml,Scope scope) throws Exception
		{
			String name = xml.getAttribute("name");
			namefields.add(name);

			String forsync = xml.getAttribute("for_sync");
			if (forsync == null)
			{
				if (scope == Scope.SCOPE_SOURCE)
					name += "-!SOURCE!";
				else if (scope == Scope.SCOPE_DESTINATION)
					name += "-!DESTINATION!";
			}
			else
				name += "-" + forsync;

			Field field = fields.get(name);
			if (field == null)
				field = new Field(xml,scope);
			else
				field.set(xml,scope);

			fields.put(name,field);
		}

		public Set<String> getNames()
		{
			return namefields;
		}

		public Set<String> getKeys()
		{
			return keyfields;
		}

		public Set<Map.Entry<String,Field>> entrySet()
		{
			return fields.entrySet();
		}

		private void doFunction(LinkedHashMap<String,String> result,XML function) throws Exception
		{
			if (function == null) return;
			if (javaadapter.isShuttingDown()) return;

			XML xml = new XML();
			xml.add("root",result);

			if (Misc.isLog(30)) Misc.log("BEFORE FUNCTION: " + xml);

			Subscriber sub = new Subscriber(function);
			XML resultxml = sub.run(xml);

			if (Misc.isLog(30)) Misc.log("AFTER FUNCTION: " + resultxml);

			result.clear();
			XML elements[] = resultxml.getElements();
			for(XML element:elements)
			{
				String value = element.getValue();
				if (value == null)
				{
					if (keyfields.contains(element.getTagName()))
					{
						if (Misc.isLog(30)) Misc.log("Key " + element.getTagName() + " is null!");
						result.clear();
						return;
					}

					value = "";
				}
				result.put(element.getTagName(),value);
			}
		}

		public LinkedHashMap<String,String> getNext(Sync sync) throws Exception
		{
			LinkedHashMap<String,String> result;
			CsvWriter csvout = sync.getCsvWriter();

			while((result = sync.next()) != null)
			{
				boolean doprocessing = true;
				if (sync.getReader() instanceof SortTable)
				{
					if (Misc.isLog(30)) Misc.log("Field: Skipping field processing since already done during sort");
					doprocessing = false;
				}

				if (doprocessing) for(Field field:fields.values())
				{
					String name = field.getName();
					String value = result.get(name);

					if (Misc.isLog(30)) Misc.log("Field: Check " + name + ":" + value + ":" + sync.getXML().getTagName() + ":" + (sourcesync == null ? "NOSRC" : sourcesync.getXML().getAttribute("name")) + ":" + (destinationsync == null ? "NODEST" : destinationsync.getXML().getAttribute("name")) + ":" + result);
					if (!field.isValid(sync)) continue;
					if (!field.isValidFilter(result)) continue;

					if (Misc.isLog(30)) Misc.log("Field: " + name + " is valid");

					if (field.isExcludeRecord(result) || !field.isIncludeRecord(result))
					{
						result = null;
						break;
					}
					if (field.isExcludeField(result) || !field.isIncludeField(result))
					{
						result.remove(name);
						continue;
					}

					if (Misc.isLog(30)) Misc.log("Field: " + name + " is not excluded");

					Field.Preload preloadmerge = field.getPreloadMerge();
					String mergevalue = preloadmerge == null ? field.lookupMerge(result) : preloadmerge.getPreload(result);
					if (mergevalue != null)
					{
						if (Misc.isLog(30)) Misc.log("Field: Using value " + mergevalue + " instead of " + value + " since more recent");
						value = mergevalue;
					}

					if (value != null && !"".equals(value))
					{
						// We have a value, just use it
						if (Misc.isLog(30)) Misc.log("Field: " + name + " set to: " + value);
					}
					else
					{
						Field.Preload preload = field.getPreload();
						value = preload == null ? field.lookup(result) : preload.getPreload(result);

						if (value != null && !"".equals(value))
						{
							// We have a value from a lookup, just use it
							if (Misc.isLog(30)) Misc.log("Field: " + name + " set to after lookup: " + value);
						}
						else
						{
							value = field.getDefault(result);
							if (value != null && !"".equals(value))
							{
								// We have a default value, just use it
								if (Misc.isLog(30)) Misc.log("Field: " + name + " set to default: " + value);
							}
						}
					}

					if (value != null && field.isStrip())
					{
						String escape = "[\\s-/.,:;\\|]";
						value = value.replaceAll(escape + "*$","").replaceAll("^" + escape + "*","");
					}

					String scriptresult = field.executeScript(result);
					if (scriptresult != null)
					{
						if (Misc.isLog(30)) Misc.log("Field: " + name + " set after script to: " + value);
						value = scriptresult;
					}

					if (value == null) value = "";

					result.put(name,value);

					String copyname = field.getCopyName();
					if (copyname != null)
					{
						if (Misc.isLog(30)) Misc.log("Field: " + name + " copied to " + copyname + ": " + value);
						result.put(copyname,value);
					}

					String newname = field.getNewName();
					if (newname != null)
					{
						value = result.remove(name);
						result.put(newname,value);
						if (Misc.isLog(30)) Misc.log("Field: " + name + " renamed to " + newname + ": " + value);
						name = newname;
					}

					if (Misc.isLog(30)) Misc.log("Field: " + name + " is finally set to: " + value);
					if (!"".equals(value))
						continue;
						
					// No value found,,,
					String onnotfound = field.onnotfound;

					if (onnotfound == null || onnotfound.equals("ignore"))
						;
					else if (onnotfound.equals("reject_field"))
					{
						if (Misc.isLog(30)) Misc.log("REJECTED field: " + field.getName());
						result.remove(name);
						if (copyname != null) result.remove(copyname);
					}
					else if (onnotfound.equals("reject_record"))
					{
						if (Misc.isLog(30)) Misc.log("REJECTED record: " + result);
						result = null;
						break;
					}
					else if (onnotfound.equals("warning"))
						Misc.log("WARNING: Default value not found for field " + field.getName() + " on " + result);
					else if (onnotfound.equals("error"))
					{
						Misc.log("ERROR: Rejecting entry because a default value for field " + field.getName() + " is not found: " + result);
						result = null;
						break;
					}
					else if (onnotfound.equals("exception"))
						throw new AdapterException("Default value not found for field " + field.getName() + " on " + result);
					else
						throw new AdapterException("Invalid on_not_found attribute " + onnotfound + " for field " + field.getName());
				}

				if (result == null) continue;

				if (doprocessing)
				{
					XML function = sync.getXML().getElement("element_function");
					String forsync = function == null ? null : function.getAttribute("for_sync");
					if (forsync == null || isValidSync(forsync.split("\\s*,\\s*"),sync))
						doFunction(result,function);

					function = xmlsync.getElement("element_function");
					forsync = function == null ? null : function.getAttribute("for_sync");
					if (forsync == null || isValidSync(forsync.split("\\s*,\\s*"),sync))
						doFunction(result,function);
				}

				if (ignoreallemptyfields)
				{
					for(Iterator<Map.Entry<String,String>> it = result.entrySet().iterator();it.hasNext();)
					{
						Map.Entry<String,String> entry = it.next();
						if ("".equals(entry.getValue()) && !keyfields.contains(entry.getKey()))
						{
							if (Misc.isLog(30)) Misc.log("Removing empty entry " + entry.getKey());
							it.remove();
						}
					}
				}

				if (result.isEmpty())
				{
					if (Misc.isLog(30)) Misc.log("REJECTED because empty");
					continue;
				}

				if (Misc.isLog(30)) Misc.log("PASSED: " + result);
				if (doprocessing && csvout != null) csvout.write(result);
				return result;
			}

			if (csvout != null) csvout.flush();
			return null;
		}
	}

	class RateCounter
	{
		int total = 0;
		int add = 0;
		int remove = 0;
		int update = 0;
	}

	private int maxqueuelength;
	private static final int DEFAULTMAXQUEUELENGTH = 100;

	private XML xmlsync;

	private String syncname;
	private String rootname;
	private ArrayList<XML> xmloperlist;
	private RateCounter counter;
	private DB db;
	private Date lastdate;
	private Date startdate;
	private boolean tobreak = false;
	private int breakcount = 0;
	private boolean checkcolumn = true;
	private boolean directmode = false;
	private Sync sourcesync;
	private Sync destinationsync;
	private UpdateSQL update;
	private Fields fields;
	private Fields fieldssource;
	private Fields fieldsdestination;

	// Global flags
	private boolean doadd;
	private boolean doremove;
	private boolean doupdate;
	private boolean ignorecasekeys;
	private boolean ignorecasefields;
	private boolean ignoreallemptyfields;

	public DBSyncOper() throws Exception
	{
		db = DB.getInstance();
		update = new UpdateSQL(db);
	}

	public boolean isValidSync(String[] synclist,Sync sync) throws Exception
	{
		if (sync == null) return false;
		if (synclist == null) return true;
		if (Misc.isLog(30)) Misc.log("Field: Doing validation against " + Misc.implode(synclist));

		String name = sync.getXML().getAttribute("name");
		if (name == null) return false;
		if (Misc.indexOf(synclist,name) != -1) return true;

		String sourcename = sourcesync == null ? null : sourcesync.getXML().getAttribute("name");
		String destname = destinationsync == null ? null : destinationsync.getXML().getAttribute("name");

		if (sourcename != null && destname != null)
			return Misc.indexOf(synclist,sourcename + "-" + destname) != -1;

		if (Misc.isLog(30)) Misc.log("Field: Validation not found in: " + name + "," + sourcename + "," + destname);
		return false;
	}

	private void flush() throws Exception
	{
		if (directmode) return;
		if (destinationsync == null) return;

		breakcount++;
		// Uncomment for debugging: if (breakcount >= 10) tobreak = true;

		XML xml = new XML();

		XML xmlop = xml.add(rootname);
		xmlop.setAttribute("name",syncname);

		String[] attrs = {"instance","table","type","on_duplicates","merge_fields"};
		for(String attr:attrs)
			xmlop.setAttribute(attr,destinationsync.getXML().getAttribute(attr));

		if ((counter.add + counter.update + counter.remove) > 0)
		{
			for(XML xmloper:xmloperlist)
				xmlop.add(xmloper);

			Publisher publisher = Publisher.getInstance();
			publisher.publish(xml,xmlsync);
		}

		xmloperlist.clear();
	}

	private void push(String oper) throws Exception
	{
		if (destinationsync == null) return;

		XML xml = new XML();
		XML node = xml.add(oper);
		if (oper.equals("end"))
		{
			node.setAttribute("total","" + counter.total);
			node.setAttribute("add","" + counter.add);
			node.setAttribute("remove","" + counter.remove);
			node.setAttribute("update","" + counter.update);
		}

		if (directmode)
		{
			update.oper(destinationsync.getXML(),xml);
			return;
		}

		xmloperlist.add(xml);
	}

	private void push(String oper,LinkedHashMap<String,String> row,LinkedHashMap<String,String> rowold) throws Exception
	{
		if (destinationsync == null) return;

		XML xml = new XML();
		XML xmlop = xml.add(oper);

		ArrayList<String> destinationheader = destinationsync.getHeader();
		Set<String> keyset = new HashSet<String>(destinationheader);
		keyset.addAll(fields.getNames());
		String ignorestr = destinationsync.getXML().getAttribute("ignore_fields");
		if (ignorestr != null) keyset.removeAll(Arrays.asList(ignorestr.split("\\s*,\\s*")));

		for(String key:keyset)
		{
			String sourcevalue = row.get(key);

			String newvalue = sourcevalue;
			if (newvalue == null) newvalue = "";

			XML xmlrow;
			if (newvalue.indexOf("\n") == -1)
				xmlrow  = xmlop.add(key,newvalue);
			else
			{
				String ondupstr = sourcesync.getXML().getAttribute("on_duplicates");

				String[] duplist = null;
				String dupfields = sourcesync.getXML().getAttribute("on_duplicates_fields");
				if (dupfields != null) duplist = dupfields.split("\\s*,\\s*");

				if (duplist != null && Misc.indexOf(duplist,key) == -1);
				else if (ondupstr == null || ondupstr.equals("merge"));
				else if (ondupstr.equals("error"))
				{
					Misc.log("ERROR: Rejecting record with a duplicated key on field " + key + ": " + Misc.getKeyValue(fields.getKeys(),row));
					return;
				}
				else if (ondupstr.equals("ignore"))
					return;
				else
					throw new AdapterException("Invalid on_duplicates attribute",sourcesync.getXML());
				xmlrow  = xmlop.addCDATA(key,newvalue);
			}

			if (Misc.isLog(30)) Misc.log("Is info check " + sourcevalue + ":" + key + ":" + destinationheader);
			boolean isinfo = (sourcevalue == null || !destinationheader.contains(key));
			if (isinfo) xmlrow.setAttribute("type","info");

			for(Map.Entry<String,Field> map:fields.entrySet())
			{
				Field field = map.getValue();
				if (key.equals(field.getName()) && (field.isValid(sourcesync) || field.isValid(destinationsync)))
				{
					if (Misc.isLog(30)) Misc.log("Matched field " + map.getKey() + " with key " + key);
					String type = field.getType();
					if (type != null) xmlrow.setAttribute("type",type);
				}
			}

			if (rowold != null)
			{
				String oldvalue = rowold.get(key);
				if (oldvalue == null) oldvalue = "";
				boolean issame = ignorecasefields ? oldvalue.equalsIgnoreCase(newvalue) : oldvalue.equals(newvalue);
				if (!issame && (!isinfo || oldvalue.length() > 0))
				{
					if (oldvalue.indexOf("\n") == -1)
						xmlrow.add("oldvalue",oldvalue);
					else
						xmlrow.addCDATA("oldvalue",oldvalue);
				}
			}
		}

		String changes = null;
		for(XML entry:xml.getElements())
		{
			String tag = entry.getTagName();
			String value = entry.getValue();
			if (value == null) value = "";
			String type = entry.getAttribute("type");
			if ("key".equals(type))
				;
			else if ("info".equals(type))
				;
			else if ("update".equals(oper))
			{
				XML old = entry.getElement("oldvalue");
				if (old != null)
				{
					String oldvalue = old.getValue();
					boolean initial = "initial".equals(type);
					if (!initial || ("initial".equals(type) && oldvalue == null))
					{
						if (oldvalue == null) oldvalue = "";
						String text = tag + "[" + oldvalue + "->" + value + "]";
						changes = changes == null ? text : changes + ", " + text;
					}
				}
			}
			else
			{
				String text = tag + ":" + value;
				changes = changes == null ? text : changes + ", " + text;
			}
			
		}

		String prevkeys = Misc.getKeyValue(fields.getKeys(),row);
		if (prevkeys == null)
		{
			Misc.log("ERROR: Discarting record with null keys: " + row);
			return;
		}

		if ("update".equals(oper) && changes == null) return;
		if (Misc.isLog(2)) Misc.log(oper + ": " + prevkeys + " " + changes);

		if ("update".equals(oper)) counter.update++;
		else if ("add".equals(oper)) counter.add++;
		else if ("remove".equals(oper)) counter.remove++;
		xmlop.setAttribute("position","" + (counter.add + counter.remove + counter.update));

		if (directmode)
		{
			update.oper(destinationsync.getXML(),xml);
			return;
		}

		xmloperlist.add(xml);

		if (xmloperlist.size() >= maxqueuelength)
			flush();
	}

	private void remove(LinkedHashMap<String,String> row) throws Exception
	{
		if (!doremove) return;
		if (Misc.isLog(4)) Misc.log("quick_remove: " + row);
		push("remove",row,null);
	}

	private void add(LinkedHashMap<String,String> row) throws Exception
	{
		if (!doadd) return;
		if (destinationsync == null) return;
		if (Misc.isLog(4)) Misc.log("quick_add: " + row);
		push("add",row,null);
	}

	private void update(LinkedHashMap<String,String> rowold,LinkedHashMap<String,String> rownew) throws Exception
	{
		if (!doupdate) return;
		if (Misc.isLog(4))
		{
			String delta = null;
			for(String key:rownew.keySet())
			{
				String newvalue = rownew.get(key);
				if (newvalue == null) newvalue = "";
				String oldvalue = rowold.get(key);
				if (oldvalue == null) oldvalue = "";
				if (!oldvalue.equals(newvalue))
				{
					if (delta != null) delta += ", ";
					else delta = "";
					delta += key + "[" + oldvalue + "->" + newvalue + "]";
				}
			}

			Misc.log("quick_update: " + Misc.getKeyValue(fields.getKeys(),rownew) + " " + delta);
		}
		push("update",rownew,rowold);
	}

	private String getKey(LinkedHashMap<String,String> row)
	{
		String key = "";
		if (row == null) return key;

		for(String keyfield:fields.getKeys())
		{
			/* Use exclamation mark since it is the lowest ASCII character */
			/* This code must match db.getorderby logic */
			key += row.get(keyfield).replace(' ','!').replace('_','!') + "!";
		}

		return key;
	}

	private Sync getSync(XML xml,XML xmlextra,XML xmlsource) throws Exception
	{
		String type = xml.getAttribute("type");
		if (type == null) type = "db";

		if (xmlextra != null)
		{
			String destfilter = xmlextra.getValue("remotefilter",null);
			if (destfilter == null)
				xml.removeAttribute("filter");
			else
				xml.setAttribute("filter",destfilter);
		}

		if (type.equals("db"))
			return new SyncSql(xml);
		else if (type.equals("csv"))
			return new SyncCsv(xml);
		/* SOAP data source is now obsolete and is replaced by XML data source */
		else if (type.equals("soap"))
			return new SyncSoap(xml);
		else if (type.equals("xml"))
			return new SyncXML(xml,xmlsource);
		else if (type.equals("class"))
			return new SyncClass(xml);

		throw new AdapterException(xml,"Invalid sync type " + type);
	}

	private void compare() throws Exception
	{
		xmloperlist = new ArrayList<XML>();
		counter = new RateCounter();

		if (Misc.isLog(2))
		{
			String sourcename = sourcesync.getXML().getAttribute("name");
			if (sourcename == null) sourcename = sourcesync.getXML().getAttribute("instance");
			if (sourcename == null) sourcename = sourcesync.getXML().getAttribute("filename");
			if (destinationsync == null)
				Misc.log("Reading source " + sourcename + "...");
			else
			{
				String destinationname = destinationsync.getXML().getAttribute("name");
				if (destinationname == null) destinationname = destinationsync.getXML().getAttribute("instance");
				if (destinationname == null) destinationname = destinationsync.getXML().getAttribute("filename");
				Misc.log("Comparing source " + sourcename + " with destination " + destinationname + "...");
			}
		}

		push("start");

		LinkedHashMap<String,String> row = fields.getNext(sourcesync);
		LinkedHashMap<String,String> rowdest = (destinationsync == null) ? null : fields.getNext(destinationsync);

		/* keycheck is obsolete and should no longer be used */
		String keycheck = sourcesync.getXML().getAttribute("keycheck");
		boolean ischeck = !(keycheck != null && keycheck.equals("false"));

		if (checkcolumn && row != null && rowdest != null && ischeck)
		{
			String error = null;
			Set<String> keylist = fields.getKeys();

			if (!row.keySet().containsAll(keylist))
				error = "Source table must contain all keys";

			if (!rowdest.keySet().containsAll(keylist))
				error = "Destination table must contain all keys";

			if (error != null)
			{
				if (Misc.isLog(5)) Misc.log("Keys: " + keylist);
				if (Misc.isLog(5)) Misc.log("Source columns: " + row);
				if (Misc.isLog(5)) Misc.log("Destination columns: " + rowdest);
				throw new AdapterException("Synchronization " + syncname + " cannot be done. " + error);
			}
		}

		String destkey = getKey(rowdest);
		String sourcekey = getKey(row);

		while(row != null || rowdest != null)
		{
			if (javaadapter.isShuttingDown()) return;
			if (tobreak) break;
			counter.total++;

			if (Misc.isLog(5)) Misc.log("Key source: " + sourcekey + " dest: " + destkey);

			if (rowdest != null && (row == null || (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) > 0))
			{
				remove(rowdest);

				rowdest = fields.getNext(destinationsync);
				destkey = getKey(rowdest);

				continue;
			}

			if (row != null && (rowdest == null || (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) < 0))
			{
				add(row);

				row = fields.getNext(sourcesync);
				sourcekey = getKey(row);

				continue;
			}

			if (row != null && (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) == 0)
			{
				for(String key:destinationsync.getHeader())
				{
					String value = row.get(key);
					if (value == null) continue;
					String destvalue = rowdest.get(key);
					if (destvalue == null) destvalue = "";
					if (!value.equals(destvalue))
					{
						update(rowdest,row);
						break;
					}
				}

				row = fields.getNext(sourcesync);
				sourcekey = getKey(row);

				rowdest = fields.getNext(destinationsync);
				destkey = getKey(rowdest);

				continue;
			}
		}

		push("end");
		flush();
	}

	private void exec(XML xml,String oper) throws Exception
	{
		XML[] execlist = xml.getElements(oper);
		for(XML element:execlist)
		{
			String command = element.getValue();

			String type = element.getAttribute("type");
			if (type != null && type.equals("db"))
			{
				db.execsql(element.getAttribute("instance"),command);
				continue;
			}
			if (type != null && type.equals("xml"))
			{
				XML[] funclist = element.getElements("function");
				for(XML funcel:funclist)
				{
					Subscriber sub = new Subscriber(funcel);
					sub.run(new XML());
				}
				continue;
			}

			String charset = element.getAttribute("charset");
			Process process = Misc.exec(command,charset);
			int exitval = process.waitFor();
			if (exitval != 0)
				throw new AdapterException(element,"Command cannot be executed properly, result code is " + exitval);
		}
	}

	public void run() throws Exception
	{
		run(null,null);
	}

	public void run(XML xmlfunction) throws Exception
	{
		run(xmlfunction,null);
	}

	private Boolean getOperationFlag(XML xml,String attr) throws Exception
	{
		String dostr = xml.getAttribute(attr);
		if (dostr == null) return null;

		if (dostr.equals("true"))
			return new Boolean(true);
		else if (dostr.equals("false"))
			return new Boolean(false);
		else
			throw new AdapterException(xml,"Invalid " + attr + " attribute");
	}

	private void setOperationFlags(XML xml) throws Exception
	{
		Boolean result = getOperationFlag(xml,"do_add");
		if (result != null) doadd = result.booleanValue();
		result = getOperationFlag(xml,"do_remove");
		if (result != null) doremove = result.booleanValue();
		result = getOperationFlag(xml,"do_update");
		if (result != null) doupdate = result.booleanValue();
		result = getOperationFlag(xml,"ignore_empty");
		if (result != null) ignoreallemptyfields = result.booleanValue();

		String casestr = xml.getAttribute("ignore_case");
		if (casestr != null)
		{
			if (casestr.equals("true"))
			{
				ignorecasekeys = true;
				ignorecasefields = true;
			}
			else if (casestr.equals("false"))
			{
				ignorecasekeys = false;
				ignorecasefields = false;
			}
			else if (casestr.equals("keys_only"))
			{
				ignorecasekeys = true;
				ignorecasefields = false;
			}
			else if (casestr.equals("non_keys_only"))
			{
				ignorecasekeys = false;
				ignorecasefields = true;
			}
			else
				throw new AdapterException(xml,"Invalid ignore_case attribute");
		}
	}

	private XML[] getFilenamePatterns(XML[] syncs) throws Exception
	{
		ArrayList<XML> results = new ArrayList<XML>();
		for(XML sync:syncs)
		{
			String filename = sync.getAttribute("filename");
			if (filename == null)
				results.add(sync);
			else
			{
				String fileescape = filename.replaceAll("\\.","\\.").replaceAll("\\*","\\*");
				Matcher matcher = Misc.substitutepattern.matcher(fileescape);
				String fileglob = matcher.replaceAll("*");
				if (Misc.isLog(10)) Misc.log("File glob: " + fileglob);

				String fileextract = matcher.replaceAll("(.*)");
				Pattern patternextract = Pattern.compile(fileextract);

				Paths paths = new Paths(".",fileglob);
				String[] files = paths.getRelativePaths();

				for(String file:files)
				{
					XML newsync = sync.copy();

					if (Misc.isLog(10)) Misc.log("Filename: " + file);
					newsync.setAttribute("filename",file);

					Matcher matcherextract = patternextract.matcher(file);
					matcher.reset();
					while(matcherextract.find())
					{
						matcher.find();
						int count = matcherextract.groupCount();
						for(int x = 0;x < count;x++)
						{
							if (Misc.isLog(10)) Misc.log("Variable from file name: " + matcher.group(x + 1) + "=" + matcherextract.group(x + 1));
							XML varxml = newsync.add("variable");
							varxml.setAttribute("name",matcher.group(x + 1));
							varxml.setAttribute("value",matcherextract.group(x + 1));
						}
					}

					results.add(newsync);
				}
			}
		}

		return results.toArray(syncs);
	}

	public void run(XML xmlfunction,XML xmlsource) throws Exception
	{
		XML xmlcfg = javaadapter.getConfiguration();

		startdate = new Date();

		XML[] elements = xmlcfg.getElements("dbsync");
		for(int i = 0;i < elements.length;i++)
		{
			if (javaadapter.isShuttingDown()) return;

			xmlsync = elements[i];
			if (!Misc.isFilterPass(xmlfunction,xmlsync)) continue;

			XML[] publishers = xmlsync.getElements("publisher");
			directmode = (publishers.length == 0);

			exec(xmlsync,"preexec");

			syncname = xmlsync.getAttribute("name");
			Misc.log(1,"Syncing " + syncname + "...");

			rootname = xmlsync.getAttribute("root");
			if (rootname == null) rootname = "ISMDatabaseUpdate";

			maxqueuelength = DEFAULTMAXQUEUELENGTH;
			String maxqueue = xmlsync.getAttribute("maxqueuelength");
			if (maxqueue != null) maxqueuelength = new Integer(maxqueue);

			/* checkcolumns is obsolete and should no longer be used */
			checkcolumn = true;
			String checkstr = xmlsync.getAttribute("checkcolumns");
			if (checkstr != null && checkstr.equals("false"))
				checkcolumn = false;

			String keyfield = xmlsync.getAttribute("keyfield");
			if (keyfield == null) keyfield = xmlsync.getAttribute("keyfields");
			if (keyfield == null) throw new AdapterException(xmlsync,"keyfield is mandatory");

			XML[] sources = getFilenamePatterns(xmlsync.getElements("source"));
			XML[] destinations = xmlsync.getElements("destination");

			for(XML source:sources)
			{
				int k = 0;
				for(;k < destinations.length;k++)
				{
					if (source == null || destinations[k] == null) continue;

					fields = new Fields(keyfield.split("\\s*,\\s*"));
					XML[] varsxml = source.getElements("variable");
					for(XML var:varsxml) fields.addDefaultVar(var);
					XML[] fieldsxml = xmlsync.getElements("field");
					for(XML field:fieldsxml) fields.add(field,Scope.SCOPE_GLOBAL);
					fieldsxml = source.getElements("field");
					for(XML field:fieldsxml) fields.add(field,Scope.SCOPE_SOURCE);
					fieldsxml = destinations[k].getElements("field");
					for(XML field:fieldsxml) fields.add(field,Scope.SCOPE_DESTINATION);

					doadd = doupdate = doremove = true;
					ignoreallemptyfields = ignorecasekeys = ignorecasefields = false;
					setOperationFlags(xmlsync);
					setOperationFlags(source);
					setOperationFlags(destinations[k]);

					try
					{
						sourcesync = getSync(source,destinations[k],xmlsource);
					}
					catch(FileNotFoundException ex)
					{
						String onnotfound = source.getAttribute("on_not_found");
						if (onnotfound == null || onnotfound.equals("exception"))
							Misc.rethrow(ex);
						else if (onnotfound.equals("ignore"))
							continue;
						else if (onnotfound.equals("warning"))
						{
							Misc.log("WARNING: Ignoring sync operation since file not found");
							continue;
						}
						else if (onnotfound.equals("error"))
						{
							Misc.log("ERROR: Ignoring sync operation since file not found");
							continue;
						}
						else
							throw new AdapterException("Invalid on_not_found attribute",source);
					}
					destinationsync = getSync(destinations[k],source,xmlsource);

					compare();

					sourcesync = destinationsync = null;
					for(XML var:varsxml) fields.removeDefaultVar(var);
				}

				if (k == 0)
				{
					fields = new Fields(keyfield.split("\\s*,\\s*"));
					XML[] varsxml = source.getElements("variable");
					for(XML var:varsxml) fields.addDefaultVar(var);
					XML[] fieldsxml = xmlsync.getElements("field");
					for(XML field:fieldsxml) fields.add(field,Scope.SCOPE_GLOBAL);
					fieldsxml = source.getElements("field");
					for(XML field:fieldsxml) fields.add(field,Scope.SCOPE_SOURCE);

					sourcesync = getSync(source,null,xmlsource);
					destinationsync = null;
					try
					{
						compare();
					}
					catch(java.net.SocketTimeoutException ex)
					{
						// Don't stop processing if a timeout occurs
						Misc.log(ex);
					}

					sourcesync = null;
					for(XML var:varsxml) fields.removeDefaultVar(var);
				}
			}

			exec(xmlsync,"postexec");

			lastdate = startdate;

			Misc.log(1,"Syncing " + syncname + " done");
		}
	}

	public void close()
	{
		db.close();
	}
}
