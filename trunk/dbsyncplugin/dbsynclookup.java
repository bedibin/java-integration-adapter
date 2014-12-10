import java.util.*;
import java.io.FileNotFoundException;

enum SyncLookupResultErrorOperationTypes { ERROR, WARNING, EXCEPTION, REJECT_FIELD, REJECT_RECORD, NONE };

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

			Preload(XML xml,String resultname) throws Exception
			{
				if (Misc.isLog(15)) Misc.log("Lookup: Doing preload for " + fieldname);

				datefield = xml.getAttribute("date_field");
				if (datefield != null && !"merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Invalid date_field attribute");
				if (datefield == null && "merge_lookup".equals(xml.getTagName()))
					throw new AdapterException(xml,"Attribute date_field mandatory for merge preload");

				Reader reader = null;
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
						fields = result.keySet();
						if (resultname != null) fields.remove(resultname);
						if (datefield != null)
						{
							if (!result.containsKey(datefield))
								throw new AdapterException(xml,"Preload must return date_field " + datefield);
							datevalue = result.get(datefield);
							fields.remove(datefield);
						}
						if (resultname != null && fields.isEmpty())
							throw new AdapterException(xml,"Preload query must return more than just " + resultname + " field");
					}

					String keyvalue = Misc.getKeyValue(fields,result);
					if (keyvalue == null) continue;

					if (table.get(keyvalue) != null)
					{
						String duperror = xml.getAttribute("show_duplicates_error");
						if (duperror == null || duperror.equals("true"))
							Misc.log("ERROR: [" + keyvalue + "] Preload for " + fieldname + " returned more than one entries");
						value = null;
					}

					if (value == null || "".equals(value)) continue;

					String[] keyvalues = keyvalue.split("\n"); // This won't work if multiple key fields are containing carriage return
					for(String key:keyvalues)
					{
						table.put(key,value);
						if (Misc.isLog(25)) Misc.log("Lookup: Storing preload for " + fieldname + " key " + key + ": " + value);
					}

					if (datevalue != null) datetable.put(keyvalue,datevalue);
				}

				if (table == null)
					Misc.log("WARNING: Preload for field '" + fieldname + "' returned empty result");
			}

			public String getPreload(LinkedHashMap<String,String> values) throws Exception
			{
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

				String result = null;
				if (table != null)
				{
					result = table.get(keyvalue);
					if (result == null)
					{
						// Work for simple key only
						String[] keysplit = keyvalue.split("\n");
						if (keysplit.length > 1)
						{
							ArrayList<String> resultlist = new ArrayList<String>();
							for(String line:keysplit)
							{
								String lineresult = table.get(line);
								if (lineresult == null)
								{
									// Fail even if only one is not found
									resultlist = null;
									result = null;
									break;
								}
								resultlist.add(lineresult);
							}

							if (resultlist != null)
							{
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
				String mergedatevalue = datetable.get(keyvalue);
				if (mergedatevalue == null) return null;

				Date date = Misc.dateformat.parse(datevalue);
				Date mergedate = Misc.dateformat.parse(mergedatevalue);

				if (date.after(mergedate)) return null;
				return result;
			}
		}

		private Preload preloadinfo;
		protected XML xmllookup;
		protected SyncLookupResultErrorOperationTypes erroroperation = SyncLookupResultErrorOperationTypes.NONE;;
		private boolean onlookupusekey = false;
		private DB db;
		protected String opername;

		public SimpleLookup()
		{
		}

		public SimpleLookup(XML xml) throws Exception
		{
			this(xml,fieldname);
		}

		protected SimpleLookup(XML xml,String resultname) throws Exception
		{
			opername = xml.getTagName();
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

		protected String lookup(final LinkedHashMap<String,String> row) throws Exception
		{
			if (preloadinfo != null)
				return preloadinfo.getPreload(row);

			if (Misc.isLog(15)) Misc.log("Lookup: Doing lookup for " + fieldname);

			String sql = xmllookup.getValue();
			String instance = xmllookup.getAttribute("instance");

			String str = row == null ? sql : Misc.substitute(sql,new Misc.Substituer() {
				public String getValue(String param) throws Exception
				{
					String value = Misc.substituteGet(param,row.get(param));
					return db.getValue(value);
				}
			});

			DB.DBOper oper = db.makesqloper(instance,str);

			LinkedHashMap<String,String> result = oper.next();
			if (result == null) return null;

			return Misc.getFirstValue(result);
		}

		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String previous = row.get(name);
			if (previous != null && !previous.isEmpty()) return SyncLookupResultErrorOperationTypes.NONE;

			String value = lookup(row);
			if (value == null)
				return erroroperation;

			row.put(name,value);
			return SyncLookupResultErrorOperationTypes.NONE;
		}

		public String getName()
		{
			return opername;
		}
	}

	class MergeLookup extends SimpleLookup
	{
		public MergeLookup(XML xml) throws Exception
		{
			super(xml,null);
		}

		@Override
		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null)
				return erroroperation;

			row.put(name,value);
			return SyncLookupResultErrorOperationTypes.NONE;
		}
	}

	class DefaultLookup extends SimpleLookup
	{
		public DefaultLookup(XML xml) throws Exception
		{
			super(xml,null);
		}

		@Override
		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String previous = row.get(name);
			if (previous != null && !previous.isEmpty()) return SyncLookupResultErrorOperationTypes.NONE;

			row.put(name,lookup(null));
			return SyncLookupResultErrorOperationTypes.NONE;
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
		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null || value.isEmpty())
				return SyncLookupResultErrorOperationTypes.NONE;
			return onexclude;
		}
	}

	class IncludeLookup extends ExcludeLookup
	{
		public IncludeLookup(XML xml) throws Exception
		{
			super(xml);
		}

		@Override
		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = lookup(row);
			if (value == null)
				return onexclude;
			return SyncLookupResultErrorOperationTypes.NONE;
		}
	}

	class ScriptLookup extends SimpleLookup
	{
		public ScriptLookup(XML xml) throws Exception
		{
			opername = xml.getTagName();
			xmllookup = xml;
		}

		@Override
		public SyncLookupResultErrorOperationTypes oper(LinkedHashMap<String,String> row,String name) throws Exception
		{
			String value = Script.execute(xmllookup.getValue(),row);
			if (value != null)
				row.put(name,value);

			return SyncLookupResultErrorOperationTypes.NONE;
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

	public SyncLookupResultErrorOperationTypes check(LinkedHashMap<String,String> row,String name) throws Exception
	{
		for(SimpleLookup lookup:lookups)
		{
			SyncLookupResultErrorOperationTypes erroroperation = lookup.oper(row,name);
			if (Misc.isLog(25)) Misc.log("Lookup operation " + lookup.getName() + " returning " + erroroperation);
			if (erroroperation != SyncLookupResultErrorOperationTypes.NONE)
				return erroroperation;
		}

		if (defaultvalue != null)
		{
			String value = row.get(name);
			if  (value == null || value.isEmpty())
				row.put(name,Misc.substitute(defaultvalue,row));
		}

		return SyncLookupResultErrorOperationTypes.NONE;
	}
}
