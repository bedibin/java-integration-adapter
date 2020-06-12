import java.util.*;

enum SyncLookupResultErrorOperationTypes { ERROR, WARNING, EXCEPTION, REJECT_FIELD, REJECT_RECORD, NEWVALUE, NONE };

class SyncLookupResultErrorOperation
{
	private SyncLookupResultErrorOperationTypes type = SyncLookupResultErrorOperationTypes.NONE;
	private String msg;
	private String name;

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

	public void setName(String name)
	{
		this.name = name;
	}

	public SyncLookupResultErrorOperationTypes getType()
	{
		return type;
	}

	public String getMessage()
	{
		return msg;
	}

	public String getName()
	{
		return name;
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

			Preload(XML xml,String resultname) throws AdapterException
			{
				this.xml = xml;
				this.resultname = resultname;
				loadingdone = false;

				if ("insert_lookup".equals(xml.getTagName()))
				{
					loadingdone = true;
					table = new HashMap<String,String>();
					return;
				}

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
				catch(AdapterNotFoundException ex)
				{
					xml.setAttributeDeprecated("on_not_found","on_file_not_found");
					OnOper onnotfound = Field.getOnOper(xml,"on_file_not_found",OnOper.EXCEPTION,EnumSet.of(OnOper.EXCEPTION,OnOper.IGNORE,OnOper.WARNING,OnOper.ERROR));
					switch(onnotfound)
					{
					case EXCEPTION:
						throw new AdapterException(ex);
					case IGNORE:
						return;
					case WARNING:
						Misc.log("WARNING: Ignoring lookup operation since file not found: " + ex.getMessage());
						return;
					case ERROR:
						Misc.log("ERROR: Ignoring lookup operation since file not found: " + ex.getMessage());
						return;
					}
				}
			}

			void updateCache(HashMap<String,String> values)
			{
				if (fields == null || resultname == null) return;
				String value = values.get(resultname);
				if (value == null || value.isEmpty()) return;

				Set<String> keys = values.keySet();
				for(String field:fields)
					if (!keys.contains(field))
						return;

				String key = Misc.getKeyValue(fields,values).toLowerCase();
				table.put(key,value);
			}

			void doInitialLoading(FieldResult fieldresult) throws AdapterException
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
						fields = new TreeSet<String>(reader.getHeader());
						fields.addAll(result.keySet());
						if (resultname != null) fields.remove(resultname);
						if (datefield != null)
						{
							if (!result.containsKey(datefield))
								throw new AdapterException(xml,"Preload must return date_field " + datefield);
							datevalue = result.get(datefield);
							fields.remove(datefield);
						}
						if (fieldresult != null)
						{
							HashSet<String> currentfields = new HashSet<String>(fieldresult.getSync().getResultHeader());
							currentfields.addAll(fieldresult.getValues().keySet());
							fields.retainAll(currentfields); // Lookup common fields + current fields
						}

						if (Misc.isLog(15))
						{
							Misc.log("Lookup fields are: " + result.keySet());
							if (fieldresult != null) Misc.log("Available fields are: " + fieldresult.getSync().getResultHeader());
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

			public String addValue(FieldResult result) throws AdapterException
			{
				XML xmladd = new XML(new StringBuilder(Misc.substitute(xml.toString(),result.getValues())));
				try {
					reader = ReaderUtil.getReader(xmladd);
				} catch(AdapterNotFoundException ex) {
					throw new AdapterException(ex);
				}
				doInitialLoading(result);
				return getValue(result);
			}

			public String getValue(FieldResult fieldresult) throws AdapterException
			{
				// Delay preloading until first use
				if (!loadingdone) doInitialLoading(fieldresult);

				if (fieldresult == null)
				{
					if (table == null) return null;
					Iterator<String> iter = table.values().iterator();
					return iter.hasNext() ? iter.next() : null;
				}

				if (fields == null) return null;
				LinkedHashMap<String,String> values = fieldresult.getValues();
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
								Collections.sort(resultlist,db.getCollator());
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

				try {
					Date date = Misc.dateformat.parse(datevalue);
					Date mergedate = Misc.dateformat.parse(mergedatevalue);

					if (date.after(mergedate)) return null;
				} catch(java.text.ParseException ex) {
					throw new AdapterException(ex);
				}
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

		public SimpleLookup(XML xml) throws AdapterException
		{
			this(xml,fieldname,null);
		}

		public SimpleLookup(XML xml,Preload preloadinfo) throws AdapterException
		{
			this(xml,fieldname,preloadinfo);
		}

		protected SimpleLookup(XML xml,String resultname,Preload preloadinfo) throws AdapterException
		{
			opername = xml.getAttribute("name");
			xmllookup = xml;
			db = DB.getInstance();

			String attr = xml.getAttributeDeprecated("use_key_when_not_found");
			if ("true".equals(attr)) onlookupusekey = true;

			OnOper onlookuperror = Field.getOnOper(xml,"on_lookup_error",OnOper.IGNORE,EnumSet.of(OnOper.USE_KEY,OnOper.IGNORE,OnOper.EXCEPTION,OnOper.ERROR,OnOper.WARNING,OnOper.REJECT_RECORD,OnOper.REJECT_FIELD));
			switch(onlookuperror)
			{
			case USE_KEY:
				onlookupusekey = true;
				break;
			case REJECT_FIELD:
				erroroperation = SyncLookupResultErrorOperationTypes.REJECT_FIELD;
				break;
			case REJECT_RECORD:
				erroroperation = SyncLookupResultErrorOperationTypes.REJECT_RECORD;
				break;
			case EXCEPTION:
				erroroperation = SyncLookupResultErrorOperationTypes.EXCEPTION;
				break;
			case ERROR:
				erroroperation = SyncLookupResultErrorOperationTypes.ERROR;
				break;
			case WARNING:
				erroroperation = SyncLookupResultErrorOperationTypes.WARNING;
				break;
			}

			if (Misc.isSubstituteDefault(xml.getElementValue()))
			{
				String type = xmllookup.getAttribute("type");
				if (!"db".equals(type)) throw new AdapterException(xmllookup,"Unsupported on demand lookup type " + type);
				return;
			}

			this.preloadinfo = preloadinfo == null ? new Preload(xml,resultname) : preloadinfo;
		}

		protected final String add(FieldResult result) throws AdapterException
		{
			if (preloadinfo == null) throw new AdapterException("Adding non preload is not supported");
			return preloadinfo.addValue(result);
		}

		protected final String lookup(FieldResult result) throws AdapterException
		{
			if (preloadinfo != null)
				return preloadinfo.getValue(result);

			if (Misc.isLog(15)) Misc.log("Lookup: Doing lookup for " + fieldname);

			String sql = xmllookup.getValue();
			String instance = xmllookup.getAttribute("instance");

			String str = result == null ? sql : db.substitute(instance,sql,result.getValues());
			DBOper oper = db.makesqloper(instance,str);

			LinkedHashMap<String,String> nextrow = oper.next();
			if (nextrow == null) return null;

			return Misc.getFirstValue(nextrow);
		}

		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String previous = result.getValue();
			if (previous != null && !previous.isEmpty()) return new SyncLookupResultErrorOperation();

			String value = lookup(result);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			result.setValue(value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}

		public final void updateCache(HashMap<String,String> row)
		{
			if (preloadinfo != null) preloadinfo.updateCache(row);
		}

		public final String getName()
		{
			return opername;
		}

		public final String getNameDebug() throws AdapterException
		{
			if (opername != null) return opername;
			return xmllookup.getTagName();
		}

		public final Preload getPreload()
		{
			return preloadinfo;
		}

		public final boolean isValidFilter(FieldResult result) throws AdapterException
		{
			return Field.isValidFilter(xmllookup,result.getValues(),null);
		}
	}

	class MergeLookup extends SimpleLookup
	{
		public MergeLookup(XML xml) throws AdapterException
		{
			super(xml,null,null);
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String value = lookup(result);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			result.setValue(value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}
	}

	class DefaultLookup extends SimpleLookup
	{
		public DefaultLookup(XML xml) throws AdapterException
		{
			super(xml,null,null);
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String previous = result.getValue();
			if (previous != null && !previous.isEmpty()) return new SyncLookupResultErrorOperation();

			String value = lookup(null);
			if (value == null)
				return new SyncLookupResultErrorOperation(erroroperation);

			result.setValue(value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}
	}

	class ExcludeLookup extends SimpleLookup
	{
		SyncLookupResultErrorOperationTypes onexclude = SyncLookupResultErrorOperationTypes.REJECT_RECORD;

		public ExcludeLookup(XML xml) throws AdapterException
		{
			super(xml,null,null);

			OnOper scope = Field.getOnOper(xml,"on_exclude",OnOper.REJECT_RECORD,EnumSet.of(OnOper.REJECT_RECORD,OnOper.IGNORE,OnOper.REJECT_FIELD,OnOper.ERROR,OnOper.WARNING,OnOper.EXCEPTION));
			switch(scope)
			{
			case IGNORE:
				onexclude = SyncLookupResultErrorOperationTypes.NONE;
				break;
			case REJECT_FIELD:
				onexclude = SyncLookupResultErrorOperationTypes.REJECT_FIELD;
				break;
			case ERROR:
				onexclude = SyncLookupResultErrorOperationTypes.ERROR;
				break;
			case WARNING:
				onexclude = SyncLookupResultErrorOperationTypes.WARNING;
				break;
			case EXCEPTION:
				onexclude = SyncLookupResultErrorOperationTypes.EXCEPTION;
				break;
			}
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String value = lookup(result);
			if (value == null || value.isEmpty())
				return new SyncLookupResultErrorOperation();
			return new SyncLookupResultErrorOperation(onexclude);
		}
	}

	class IncludeLookup extends ExcludeLookup
	{
		public IncludeLookup(XML xml) throws AdapterException
		{
			super(xml);
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String value = lookup(result);
			if (value == null)
				return new SyncLookupResultErrorOperation(onexclude);
			return new SyncLookupResultErrorOperation();
		}
	}

	class InsertLookup extends SimpleLookup
	{
		public InsertLookup(XML xml,Preload preload) throws AdapterException
		{
			super(xml,preload);
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			String value = lookup(result);
			if (value == null) value = add(result);
			if (value == null) return new SyncLookupResultErrorOperation();
			result.setValue(value);
			return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
		}
	}

	class ScriptLookup extends SimpleLookup
	{
		SyncLookupResultErrorOperationTypes onexception = SyncLookupResultErrorOperationTypes.WARNING;

		public ScriptLookup(XML xml) throws AdapterException
		{
			xmllookup = xml;

			OnOper scope = Field.getOnOper(xml,"on_exception",OnOper.WARNING,EnumSet.of(OnOper.WARNING,OnOper.IGNORE,OnOper.REJECT_FIELD,OnOper.ERROR,OnOper.REJECT_RECORD,OnOper.EXCEPTION));
			switch(scope)
			{
			case IGNORE:
				onexception = SyncLookupResultErrorOperationTypes.NONE;
				break;
			case REJECT_FIELD:
				onexception = SyncLookupResultErrorOperationTypes.REJECT_FIELD;
				break;
			case ERROR:
				onexception = SyncLookupResultErrorOperationTypes.ERROR;
				break;
			case REJECT_RECORD:
				onexception = SyncLookupResultErrorOperationTypes.REJECT_RECORD;
				break;
			case EXCEPTION:
				onexception = SyncLookupResultErrorOperationTypes.EXCEPTION;
				break;
			}
		}

		@Override
		public SyncLookupResultErrorOperation oper(FieldResult result) throws AdapterException
		{
			try {
				HashMap<String,String> fields = new HashMap<String,String>(result.getValues());
				for(String key:result.getSync().getResultHeader())
				{
					String value = fields.get(key);
					if (value == null)
						fields.put(key,"");
				}
				String value = Script.execute(xmllookup,fields);
				if (value != null)
				{
					result.setValue(value);
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
	private int count;

	public SyncLookup(Field field) throws AdapterException
	{
		fieldname = field.getName();
		XML xml = field.getXML();
		if (xml == null) return;

		SimpleLookup.Preload preload = null;
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
			else if (name.equals("insert_lookup"))
			{
				SimpleLookup lookup = new InsertLookup(element,preload);
				lookups.add(lookup);
				preload = lookup.getPreload();
			}
			else if (name.equals("script"))
				lookups.add(new ScriptLookup(element));
		}

		count = lookups.size();

		if (xml.isAttribute("default"))
		{
			defaultvalue = xml.getAttribute("default");
			if (defaultvalue == null) defaultvalue = "";
			if (Misc.isLog(10)) Misc.log("Field: Default " + fieldname + " value: " + defaultvalue);
			count++;
		}
	}

	public SyncLookupResultErrorOperation check(FieldResult result) throws AdapterException
	{
		SyncLookupResultErrorOperationTypes oper = SyncLookupResultErrorOperationTypes.NONE;
		for(SimpleLookup lookup:lookups)
		{
			if (!lookup.isValidFilter(result)) continue;
			SyncLookupResultErrorOperation erroroperation = lookup.oper(result);
			if (Misc.isLog(25)) Misc.log("Lookup operation " + lookup.getNameDebug() + " returning " + erroroperation.getType() + " oper " + oper);
			if (oper != SyncLookupResultErrorOperationTypes.NEWVALUE || erroroperation.getType() != SyncLookupResultErrorOperationTypes.NONE)
				oper = erroroperation.getType();
			if (erroroperation.getType() != SyncLookupResultErrorOperationTypes.NONE && erroroperation.getType() != SyncLookupResultErrorOperationTypes.NEWVALUE)
			{
				erroroperation.setName(lookup.getName());
				return erroroperation;
			}
		}

		if (defaultvalue != null)
		{
			String value = result.getValue();
			if  (value == null || value.isEmpty())
			{
				result.setValue(Misc.substitute(defaultvalue,result.getValues()));
				return new SyncLookupResultErrorOperation(SyncLookupResultErrorOperationTypes.NEWVALUE);
			}
		}

		return new SyncLookupResultErrorOperation(oper);
	}

	public void updateCache(HashMap<String,String> row)
	{
		for(SimpleLookup lookup:lookups)
			lookup.updateCache(row);
	}

	public int getCount()
	{
		return count;
	}
}
