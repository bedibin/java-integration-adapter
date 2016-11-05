import java.util.*;
import java.io.FileNotFoundException;

enum SyncLookupResultErrorOperationTypes { ERROR, WARNING, EXCEPTION, REJECT_FIELD, REJECT_RECORD, NEWVALUE, NONE };

class SyncLookupResultErrorOperation
{
	public SyncLookupResultErrorOperationTypes type = SyncLookupResultErrorOperationTypes.NONE;
	public String msg;
	public String name;

	public SyncLookupResultErrorOperation() {}

	public SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes type)
	{
		this.type = type;
	}

	public SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes type,String msg)
	{
		this.type = type;
		this.msg = msg;
	}
}

class SyncLookup
{
	class SimpleLookup
	{
		class Preload
		{
			private HashMap<String,String> table;
			private HashMap<String,String> datetable;
			private Set<String> fields;
			private String datefield;
			private XML xml;
			private Reader reader;
			private String resultname;
			private boolean loadingdone;

			Preload(XML xml,String resultname) throws Exception
			{
				this.xml = xml;
				this.resultname = resultname;
				loadingdone = false;

				if (Misc.isLog(15)) Misc.log("Lookup: Doing preload for " + fieldname);
				xml.setAttribute("default_field_name",fieldname);

				datefield = xml.getAttribute("date_field");
				if (datefield != null && !"merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Invalid date_field attribute");
				if (datefield == null && "merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Attribute date_field mandatory for merge preload");

				reader = null;
				try
				{
					reader = ReaderUtil.getReader(xml);
				}
				catch(FileNotFoundException ex)
				{
					String onnotfound = xml.getAttribute("on_file_not_found");
					if (onnotfound == null) onnotfound = xml.getAttributeDeprecated("on_not_found");
					if (onnotfound == null || onnotfound.equals("exception"))
						Misc.rethrow(ex);
					else if (onnotfound.equals("ignore"))
						return;
					else if (onnotfound.equals("warning"))
					{
						Misc.log("WARNING: Ignoring lookup operation since file not found: " + ex.getMessage());
						return;
					}
					else if (onnotfound.equals("error"))
					{
						Misc.log("ERROR: Ignoring lookup operation since file not found: " + ex.getMessage());
						return;
					}
					else
						throw new AdapterException(xml,"Invalid on_file_not_found attribute");
				}
			}

			void doInitialLoading(LinkedHashMap<String,String> values) throws Exception
			{
				LinkedHashMap<String,String> result;
				if (reader != null) while((result = reader.next()) != null)
				{
					String value;

					String datevalue = null;
					if (resultname == null)
						value = result.values().iterator().next();
					else
					{
						value = result.get(resultname);
						if (value == null)
							throw new AdapterException(xml,"Preload query doesn't return " + resultname + " field");
					}

					if (table == null) table = new HashMap<String,String>();
					if (datetable == null) datetable = new HashMap<String,String>();

					if (fields == null)
					{
						fields = new TreeSet<String>(result.keySet());
						if (resultname != null) fields.remove(resultname);
						if (datefield != null)
						{
							if (!result.containsKey(datefield))
								throw new AdapterException(xml,"Preload must return date_field " + datefield);
							datevalue = result.get(datefield);
							fields.remove(datefield);
						}
						if (values != null) fields.retainAll(values.keySet()); // Lookup only on common fields
						if (Misc.isLog(15))
						{
							Misc.log("Lookup fields are: " + result.keySet());
							if (values != null) Misc.log("Available fields are: " + values.keySet());
							Misc.log("Common lookup fields are: " + fields);
						}
						if (fields.isEmpty())
						{
							if (Misc.isLog(15)) Misc.log("WARNING: Preload for field '" + fieldname + "' empty since no fields are in common");
							loadingdone = true;
							return;
						}
					}

					String keyvalue = Misc.getKeyValue(fields,result);
					if (keyvalue == null) continue;

					String keyvaluelower = keyvalue.toLowerCase();
					if (table.get(keyvaluelower) != null)
					{
						String duperror = xml.getAttribute("show_duplicates_error");
						if (duperror == null || duperror.equals("true"))
							Misc.log("ERROR: [" + keyvalue + "] Preload for " + fieldname + " returned more than one entries");
						value = null;
					}

					if (value == null || "".equals(value)) continue;

					String[] keyvalues = keyvaluelower.split("\n"); // This won't work if multiple key fields are containing carriage return
					for(String key:keyvalues)
					{
						table.put(key,value);
						if (Misc.isLog(25)) Misc.log("Lookup: Storing preload for " + fieldname + " key " + key + ": " + value);
					}

					if (datevalue != null) datetable.put(keyvaluelower,datevalue);
				}

				if (table == null)
					Misc.log("WARNING: Preload for field '" + fieldname + "' returned empty result");
				loadingdone = true;
			}

			public String getPreload(LinkedHashMap<String,String> values) throws Exception
			{
				// Delay preloading until first use
				if (!loadingdone) doInitialLoading(values);

				if (values == null)
				{
					if (table == null) return null;
					Iterator<String> iter = table.values().iterator();
					return  iter.hasNext() ? iter.next() : null;
				}

				if (fields == null) return null;
				String keyvalue = Misc.getKeyValue(fields,values);
				if (keyvalue == null) return ""; // If key is null, do not reject or throw an error

				if (Misc.isLog(25)) Misc.log("Lookup: Preload key for " + fieldname + " is " + keyvalue);

				String keyvaluelower = keyvalue.toLowerCase();
				String result = null;
				if (table != null)
				{
					result = table.get(keyvaluelower);
					if (result == null)
					{
						// Work for simple key only
						String[] keysplit = keyvaluelower.split("\n");
						if (keysplit.length > 1)
						{
							ArrayList<String> resultlist = new ArrayList<String>();
							ArrayList<String> discardedlist = new ArrayList<String>();
							for(String line:keysplit)
							{
								String lineresult = table.get(line);
								if (lineresult == null)
									discardedlist.add(line);
								else
									resultlist.add(lineresult);
							}

							if (resultlist.size() == 0)
								result = null;
							else
							{
								if (discardedlist.size() > 0)
									Misc.log("WARNING: Discarded entries when looking up multiple values for field " + fieldname + ": " + Misc.implode(discardedlist));
								Collections.sort(resultlist,db.collator);
								result = Misc.implode(resultlist,"\n");
							}
						}
					}
					if (result == null && onlookupusekey)
						result = keyvalue;
					if (result == null) return null;
				}

				if (Misc.isLog(25)) Misc.log("Lookup: Getting preload for " + fieldname + " key " + keyvalue + ": " + result);

				if (datefield == null) return result;

				if (!values.containsKey(datefield))
					throw new AdapterException("Extraction must return a value for date_field " + datefield);
				String datevalue = values.get(datefield);
				if (datevalue == null) return result;
				String mergedatevalue = datetable.get(keyvaluelower);
				if (mergedatevalue == null) return null;

				Date date = Misc.dateformat.parse(datevalue);
				Date mergedate = Misc.dateformat.parse(mergedatevalue);

				if (date.after(mergedate)) return null;
				return result;
			}
		}

		private Preload preloadinfo;
		protected XML xmllookup;
		protected SyncLookupResultErrorOperationTypes erroroperation = SyncLookupResultErrorOperationTypes.NONE;
		private boolean onlookupusekey = false;
		private DB db;
		private String opername;

		public SimpleLookup()
		{
		}

		public SimpleLookup(XML xml) throws Exception
		{
			this(xml,fieldname);
		}

		protected SimpleLookup(XML xml,String resultname) throws Exception
		{
			opername = xml.getAttribute("name");
			xmllookup = xml;
			db = DB.getInstance();

			String onlookuperror = xml.getAttribute("on_lookup_error");
			if (onlookuperror == null)
			{
				String attr = xml.getAttributeDeprecated("use_key_when_not_found");
				if ("true".equals(attr)) onlookupusekey = true;
			}
			else if (onlookuperror.equals("use_key"))
				onlookupusekey = true;
			else if (onlookuperror.equals("ignore"))
				;
			else if (onlookuperror.equals("exception"))
				erroroperation = SyncLookupResultErrorOperationTypes.EXCEPTION;
			else if (onlookuperror.equals("error"))
				erroroperation = SyncLookupResultErrorOperationTypes.ERROR;
			else if (onlookuperror.equals("warning"))
				erroroperation = SyncLookupResultErrorOperationTypes.WARNING;
			else
				throw new AdapterException(xml,"Invalid on_lookup_error attribute " + onlookuperror);

			if (Misc.isSubstituteDefault(xml.getValue()))
			{
				String type = xmllookup.getAttribute("type");
				if (!"db".equals(type)) throw new AdapterException(xmllookup,"Unsupported on demand lookup type " + type);
				return;
			}

			preloadinfo = new Preload(xml,resultname);
		}

		protected String lookup(LinkedHashMap<String,String> row) throws Exception
		{
			if (preloadinfo != null)
				return preloadinfo.getPreload(row);

			if (Misc.isLog(15)) Misc.log("Lookup: Doing lookup for " + fieldname);

			String sql = xmllookup.getValue();
			String instance = xmllookup.getAttribute("instance");

			String str = row == null ? sql : db.substitute(instance,sql,row);
			DBOper oper = db.makesqloper(instance,str);

			LinkedHashMap<String,String> result = oper.next();
			if (result == null) return null;

			return Misc.getFirstValue(result);
		}

		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String previous = row.get(name);
			if (previous != null && !previous.isEmpty()) return new SyncLookupResultErrorOperation();

			String value = lookup(row);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			row.put(name,value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}

		public String getName()
		{
			return opername;
		}

		public String getNameDebug() throws Exception
		{
			if (opername != null) return opername;
			return xmllookup.getTagName();
		}
	}

	class MergeLookup extends SimpleLookup
	{
		public MergeLookup(XML xml) throws Exception
		{
			super(xml,null);
		}

		@Override
		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			row.put(name,value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}
	}

	class DefaultLookup extends SimpleLookup
	{
		public DefaultLookup(XML xml) throws Exception
		{
			super(xml,null);
		}

		@Override
		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String previous = row.get(name);
			if (previous != null && !previous.isEmpty()) return new SyncLookupResultErrorOperation();

			String value = lookup(null);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			row.put(name,value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}
	}

	class ExcludeLookup extends SimpleLookup
	{
		SyncLookupResultErrorOperationTypes onexclude = SyncLookupResultErrorOperationTypes.REJECT_RECORD;

		public ExcludeLookup(XML xml) throws Exception
		{
			super(xml,null);

			String scope = xml.getAttribute("on_exclude");
			if (scope == null || scope.equals("reject_record"))
				;
			else if (scope.equals("ignore"))
				onexclude = SyncLookupResultErrorOperationTypes.NONE;
			else if (scope.equals("reject_field"))
				onexclude = SyncLookupResultErrorOperationTypes.REJECT_FIELD;
			else if (scope.equals("error"))
				onexclude = SyncLookupResultErrorOperationTypes.ERROR;
			else if (scope.equals("warning"))
				onexclude = SyncLookupResultErrorOperationTypes.WARNING;
			else if (scope.equals("exception"))
				onexclude = SyncLookupResultErrorOperationTypes.EXCEPTION;
			else
				throw new AdapterException(xml,"Invalid on_exclude attribute");
		}

		@Override
		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null || value.isEmpty())
				return new SyncLookupResultErrorOperation();
			return new SyncLookupResultErrorOperation(onexclude);
		}
	}

	class IncludeLookup extends ExcludeLookup
	{
		public IncludeLookup(XML xml) throws Exception
		{
			super(xml);
		}

		@Override
		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null)
				return new SyncLookupResultErrorOperation(onexclude);
			return new SyncLookupResultErrorOperation();
		}
	}

	class ScriptLookup extends SimpleLookup
	{
		SyncLookupResultErrorOperationTypes onexception = SyncLookupResultErrorOperationTypes.WARNING;

		public ScriptLookup(XML xml) throws Exception
		{
			xmllookup = xml;

			String scope = xml.getAttribute("on_exception");
			if (scope == null || scope.equals("warning"))
				;
			else if (scope.equals("ignore"))
				onexception = SyncLookupResultErrorOperationTypes.NONE;
			else if (scope.equals("reject_field"))
				onexception = SyncLookupResultErrorOperationTypes.REJECT_FIELD;
			else if (scope.equals("error"))
				onexception = SyncLookupResultErrorOperationTypes.ERROR;
			else if (scope.equals("reject_record"))
				onexception = SyncLookupResultErrorOperationTypes.REJECT_RECORD;
			else if (scope.equals("exception"))
				onexception = SyncLookupResultErrorOperationTypes.EXCEPTION;
			else
				throw new AdapterException(xml,"Invalid on_exception attribute");
		}

		@Override
		public SyncLookupResultErrorOperation oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			try {
				String value = Script.execute(xmllookup.getValue(),row);
				if (value != null)
				{
					row.put(name,value);
					return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
				}
			} catch (AdapterScriptException ex) {
				return new SyncLookupResultErrorOperation(onexception,"SCRIPT EXCEPTION: " + ex.getMessage());
			}

			return new SyncLookupResultErrorOperation();
		}
	}

	private ArrayList<SimpleLookup> lookups = new ArrayList<SimpleLookup>();
	private String fieldname;
	private String defaultlookupvalue;
	private String defaultvalue;

	public SyncLookup(DBSyncOper.Field field) throws Exception
	{
		fieldname = field.getName();
	}

	public SyncLookup(XML xml,DBSyncOper.Field field) throws Exception
	{
		this(field);

		XML[] elements = xml.getElements(null);
		for(XML element:elements)
		{
			String name = element.getTagName();
			if (name.equals("default_lookup"))
				lookups.add(new DefaultLookup(element));
			else if (name.equals("lookup"))
				lookups.add(new SimpleLookup(element));
			else if (name.equals("merge_lookup"))
				lookups.add(new MergeLookup(element));
			else if (name.equals("include_lookup"))
				lookups.add(new IncludeLookup(element));
			else if (name.equals("exclude_lookup"))
				lookups.add(new ExcludeLookup(element));
			else if (name.equals("script"))
				lookups.add(new ScriptLookup(element));
		}

		if (xml.isAttribute("default"))
		{
			defaultvalue = xml.getAttribute("default");
			if (defaultvalue == null) defaultvalue = "";
			if (Misc.isLog(10)) Misc.log("Field: Default " + fieldname + " value: " + defaultvalue);
		}
	}

	public SyncLookupResultErrorOperation check(LinkedHashMap<String,String> row,String name) throws Exception
	{
		SyncLookupResultErrorOperationTypes oper = SyncLookupResultErrorOperationTypes.NONE;
		for(SimpleLookup lookup:lookups)
		{
			SyncLookupResultErrorOperation erroroperation = lookup.oper(row,name);
			if (Misc.isLog(25)) Misc.log("Lookup operation " + lookup.getNameDebug() + " returning " + erroroperation.type + " oper " + oper);
			if (oper != SyncLookupResultErrorOperationTypes.NEWVALUE || erroroperation.type != SyncLookupResultErrorOperationTypes.NONE)
				oper = erroroperation.type;
			if (erroroperation.type != SyncLookupResultErrorOperationTypes.NONE && erroroperation.type != SyncLookupResultErrorOperationTypes.NEWVALUE)
			{
				erroroperation.name = lookup.getName();
				return erroroperation;
			}
		}

		if (defaultvalue != null)
		{
			String value = row.get(name);
			if  (value == null || value.isEmpty())
			{
				row.put(name,Misc.substitute(defaultvalue,row));
				return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
			}
		}

		return new SyncLookupResultErrorOperation(oper);
	}
}
