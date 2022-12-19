import java.util.*;
import java.io.*;

enum Scope { SCOPE_GLOBAL, SCOPE_SOURCE, SCOPE_DESTINATION };
enum FieldFeature { FIELD_FEATURE_ONMULTIPLE, FIELD_FEATURE_IGNORE_CASE, FIELD_FEATURE_TYPE, FIELD_FEATURE_DEVIATION };
enum OnOper { IGNORE, WARNING, ERROR, REJECT_FIELD, REJECT_RECORD, USE_KEY, USE_FIRST, EXCEPTION, MERGE, RECREATE, SUFFIX, CLEAR, TRUE, FALSE, KEYS_ONLY, NON_KEYS_ONLY, KEY, INFO, INFONULL, NOINFO, INFOAPI, INITIAL, IFUPDATE };
enum DeviationType { ABSOLUTE, PERCENTAGE };

class FieldDeviation
{
	private int value = 0;
	private DeviationType type = DeviationType.ABSOLUTE;

	FieldDeviation(String attr)
	{
		if (attr.endsWith("%"))
		{
			type = DeviationType.PERCENTAGE;
			attr = attr.substring(0,attr.length() - 1);
		}
		value = Integer.parseInt(attr);
	}

	boolean check(String oldvalue,String newvalue)
	{
		// Only interger supported for now
		if (value == 0) return false;
		if (oldvalue == null || !oldvalue.matches("^-?\\d+$")) return false;
		if (newvalue == null || !newvalue.matches("^-?\\d+$")) return false;
		int oldint = Integer.parseInt(oldvalue);
		int newint = Integer.parseInt(newvalue);
		int gap = type == DeviationType.PERCENTAGE ? oldint * value / 100 : value;
		if (Misc.isLog(25)) Misc.log("Deviation " + gap + ": " + oldint + "," + newint);
		return (newint >= oldint - gap) && (newint <= oldint + gap);
	}

}

class FieldResult
{
	private Map<String,String> values;
	private Sync sync;
	private Field field;
	private Set<String> fields = new TreeSet<>();

	FieldResult(Sync sync,Field field,Map<String,String> values)
	{
		this.values = values;
		this.sync = sync;
		this.field = field;
		fields.add(field.getName());
	}

	Sync getSync() { return sync; }
	Field getField() { return field; }
	Map<String,String> getValues() { return values; }
	String getValue() { return values.get(field.getName()); }
	void setValue(String value) { values.put(field.getName(),value); }
	Set<String> getFields() { return fields; }
	void setFields(Set<String> processed_fields) { fields.addAll(processed_fields); }
}

class Field
{
	private XML xmlfield;
	private SyncLookup lookup;
	private String name;
	private String newname;
	private boolean dostrip = false;
	private boolean hasvalue = false;
	private boolean isdefault = false;
	private String copyname;
	private boolean iskey = false;

	private OnOper onempty = OnOper.IGNORE;
	private OnOper onmissing = OnOper.IGNORE;
	private String forceemptyvalue;
	private String synclist[];
	private Scope scope;

	public Field(XML xml,Scope scope) throws AdapterException
	{
		xmlfield = xml;
		this.scope = scope;
		name = xml.getAttribute("name");
		if (name == null || name.isEmpty()) throw new AdapterException(xml,"Field name cannot be empty");

		newname = xml.getAttribute("rename");
		String strip = xml.getAttributeDeprecated("strip");
		if (strip != null && strip.equals("true")) dostrip = true;
		copyname = xml.getAttribute("copy");

		xml.setAttributeDeprecated("on_not_found","on_empty");
		onempty = getOnOper("on_empty",OnOper.IGNORE,EnumSet.of(OnOper.IGNORE,OnOper.REJECT_FIELD,OnOper.REJECT_RECORD,OnOper.WARNING,OnOper.ERROR,OnOper.EXCEPTION));
		onmissing = getOnOper("on_missing",OnOper.IGNORE,EnumSet.of(OnOper.IGNORE,OnOper.EXCEPTION));

		forceemptyvalue = xml.getAttribute("force_empty_value");
		String forsync = xml.getAttribute("for_sync");
		if (forsync != null) synclist = forsync.split("\\s*,\\s*");

		lookup = new SyncLookup(this);
		hasvalue = xml.isAttribute("default") || lookup.getCount() > 0 || "true".equals(xml.getAttribute("hasvalue"));
		isdefault = "true".equals(xml.getAttribute("isdefault"));

		if (Misc.isLog(10)) Misc.log("Initializing field " + name + " scope=" + scope + " hasvalue=" + hasvalue + " isdefault=" + isdefault);
	}

	public String toString()
	{
		return name + " (scope=" + scope + ",iskey=" + iskey + ",hasvalue=" + hasvalue + ",isdefault=" + isdefault + ")";
	}

	public Field(String name) throws AdapterException
	{
		this.name = name;
		iskey = true;
		scope = Scope.SCOPE_GLOBAL;
		hasvalue = true;
		if (Misc.isLog(10)) Misc.log("Initializing key field " + name + " " + scope);
		xmlfield = new XML();
		lookup = new SyncLookup(this);
	}

	public OnOper getOnOper(String attr,OnOper def,EnumSet<OnOper> validset) throws AdapterException
	{
		return getOnOper(xmlfield,attr,def,validset);
	}

	public String getAttribute(String attr) throws AdapterException
	{
		return xmlfield.getAttribute(attr);
	}

	public boolean isAttributeNoDefault(String attr)
	{
		return xmlfield.isAttributeNoDefault(attr);
	}

	static public OnOper getOnOper(XML xml,String attr) throws AdapterException
	{
		return xml.getAttributeEnum(attr,OnOper.class);
	}

	static public OnOper getOnOper(XML xml,String attr,OnOper def,EnumSet<OnOper> validset) throws AdapterException
	{
		return xml.getAttributeEnum(attr,def,validset,OnOper.class);
	}

	public XML getXML()
	{
		return xmlfield;
	}

	public String getName()
	{
		return name;
	}

	public String getNewName()
	{
		return newname;
	}

	public Scope getScope()
	{
		return scope;
	}

	public OnOper getOnEmpty()
	{
		return onempty;
	}

	public OnOper getOnMissing()
	{
		return onmissing;
	}

	public boolean isStrip()
	{
		return dostrip;
	}

	public String getCopyName()
	{
		return copyname;
	}

	public boolean hasValue()
	{
		return hasvalue;
	}

	public boolean isDefault()
	{
		return isdefault;
	}

	public boolean isForceEmpty(String value)
	{
		return (forceemptyvalue != null && value.equals(forceemptyvalue));
	}

	public SyncLookup getLookup()
	{
		return lookup;
	}

	public boolean isKey()
	{
		return iskey;
	}

	public String[] getForSync()
	{
		return synclist;
	}

	public boolean isValid(Sync sync) throws AdapterException
	{
		boolean result = sync.getDBSync().isValidSync(synclist,sync.getScope());
		if (!result) return false;
		if (scope == Scope.SCOPE_SOURCE && sync.getScope() != Scope.SCOPE_SOURCE) return false;
		if (scope == Scope.SCOPE_DESTINATION && sync.getScope() != Scope.SCOPE_DESTINATION) return false;
		return true;
	}

	public static boolean isValidFilter(XML xml,Map<String,String> result,String current) throws AdapterException
	{
		String ifexists = null;

		if (xml.isAttribute("if_exists"))
		{
			ifexists = xml.getAttribute("if_exists");
			if (ifexists == null) ifexists = "";
		}

		if (ifexists != null)
		{
			if (ifexists.isEmpty())
			{
				if (current == null) throw new AdapterException("if_exists attribute cannot have an empty value");
				ifexists = current;
			}

			String[] keys = ifexists.split("\\s*,\\s*");
			for(String key:keys)
			{
				String value = result.get(key);
				if (value == null || value.isEmpty()) return false;
			}
		}

		String filtername = xml.getAttribute("filter");
		String filterresult = xml.getAttribute("filter_result");

		if (filtername == null && filterresult == null) return true;

		XML xmlresult = new XML();
		xmlresult.add("root",result);
		if (Misc.isLog(30)) Misc.log("Looking for filter " + filtername + " [" + filterresult + "]: " + xmlresult);

		return Misc.isFilterPass(xml,xmlresult);
	}

	public boolean isValidFilter(Map<String,String> result) throws AdapterException
	{
		return isValidFilter(xmlfield,result,getName());
	}

	public void updateCache(HashMap<String,String> row)
	{
		lookup.updateCache(row);
	}
}

class FieldValue<T>
{
	private Field field;
	private T value;

	FieldValue(Field field,T value)
	{
		this.field = field;
		this.value = value;
	}

	Field getField() { return field; }
	T getValue(Map<String,String> row) throws AdapterException
	{
		if (row == null || field.isValidFilter(row)) return value;
		return null;
	}
}

class Fields
{
	private LinkedHashSet<String> keyfields;
	private LinkedHashSet<String> namefields;
	private EnumMap<Scope,EnumMap<FieldFeature,HashMap<String,FieldValue>>> features;
	/*
	Field list ordering is important:
	1- Keys
	2- Sync field elements
	3- Source and destination field elements
	4- Reader fields
	*/
	private ArrayList<Field> fields;
	private DBSyncOper dbsync;

	public Fields(DBSyncOper dbsync,Set<String> keyfields) throws AdapterException
	{
		this.dbsync = dbsync;
		this.keyfields = new LinkedHashSet<>(keyfields);
		namefields = new LinkedHashSet<>(keyfields);
		fields = new ArrayList<>();
		features = new EnumMap<>(Scope.class);
		features.put(Scope.SCOPE_SOURCE,new EnumMap<>(FieldFeature.class));
		features.put(Scope.SCOPE_DESTINATION,new EnumMap<>(FieldFeature.class));
		features.put(Scope.SCOPE_GLOBAL,new EnumMap<>(FieldFeature.class));
		for(String keyfield:keyfields)
			add(new Field(keyfield));
	}

	void setFeature(Scope scope,FieldFeature feature,FieldValue value,boolean overwrite)
	{
		EnumMap<FieldFeature,HashMap<String,FieldValue>> mapscope = features.get(scope);
		HashMap<String,FieldValue> mapoper = mapscope.get(feature);
		if (mapoper == null)
		{
			mapoper = new HashMap<>();
			mapscope.put(feature,mapoper);
		}

		Field field = value.getField();
		if (!mapoper.containsKey(field.getName()) || overwrite) mapoper.put(field.getName(),value);
		if ((!mapoper.containsKey(field.getNewName()) || overwrite) && field.getNewName() != null) mapoper.put(field.getNewName(),value);
		if ((!mapoper.containsKey(field.getCopyName()) || overwrite) && field.getCopyName() != null) mapoper.put(field.getCopyName(),value);
	}

	@SuppressWarnings("unchecked")
	<T>T getFeature(Scope scope,FieldFeature feature,String name,Map<String,String> row) throws AdapterException
	{
		EnumMap<FieldFeature,HashMap<String,FieldValue>> fieldscope = features.get(scope);
		HashMap<String,FieldValue> fieldfeature = fieldscope.get(feature);
		FieldValue fieldvalue = fieldfeature.get(name);
		if (fieldvalue == null) return null;
		return (T)fieldvalue.getValue(row);
	}

	void setDefaultFields(Set<String> set,Scope scope) throws AdapterException
	{
		XML xml = new XML();
		xml = xml.add("field");
		for(String name:set)
		{
			int setpos = -1;
			int pos = 0;
			Scope setscope = scope;

			for(Field field:fields)
			{
				pos++;
				if (!field.isDefault()) continue;
				if (!name.equals(field.getName())) continue;
				if (field.getScope() != Scope.SCOPE_SOURCE)
					throw new AdapterException("Unexpected repeated default field " + name);

				setpos = pos - 1;
				setscope = Scope.SCOPE_GLOBAL;
				break;
			}

			xml.setAttribute("name",name);
			xml.setAttribute("hasvalue","true");
			xml.setAttribute("isdefault","true");
			add(new Field(xml,setscope),true,setpos);
		}
	}

	public void addDefaultVar(XML xml) throws AdapterException
	{
		XML.setDefaultVariable(xml.getAttribute("name"),xml.getAttribute("value"));
	}

	public void removeDefaultVar(XML xml) throws AdapterException
	{
		XML.setDefaultVariable(xml.getAttribute("name"),null);
	}

	public boolean isKey(String name)
	{
		return keyfields.contains(name);
	}

	private void addName(Set<String> set,Field field)
	{
		if (field.getNewName() == null)
			set.add(field.getName());
		else
			set.add(field.getNewName());

		if (field.getCopyName() != null)
			set.add(field.getCopyName());
	}

	public void add(Field field) throws AdapterException
	{
		add(field,false,-1);
	}

	public void add(Field field,boolean isdefault,int pos) throws AdapterException
	{
		addName(namefields,field);

		Scope scope = field.getScope();
		String[] synclist = field.getForSync();

		final String onmultipleattr = "on_multiple";
		OnOper onmultipleoper = field.getOnOper(onmultipleattr,OnOper.IGNORE,EnumSet.of(OnOper.IGNORE,OnOper.ERROR,OnOper.WARNING,OnOper.MERGE,OnOper.REJECT_RECORD,OnOper.REJECT_FIELD,OnOper.USE_FIRST));

		final String ignorecaseattr = "ignore_case";
		OnOper ignorecaseoper = field.getOnOper(ignorecaseattr,OnOper.FALSE,EnumSet.of(OnOper.TRUE,OnOper.FALSE,OnOper.KEYS_ONLY,OnOper.NON_KEYS_ONLY));

		final String typeattr = "type";
		OnOper typeoper = field.getOnOper(typeattr,field.isKey() ? OnOper.KEY : null,EnumSet.of(OnOper.KEY,OnOper.INFONULL,OnOper.NOINFO,OnOper.INFOAPI,OnOper.INFO,OnOper.INITIAL,OnOper.IFUPDATE));

		final String deviationattr = "min_deviation";
		String deviationstr = field.getAttribute(deviationattr);
		FieldDeviation deviation = deviationstr == null ? null : new FieldDeviation(deviationstr);

		final String forceemptyattr = "force_empty_value";
		String forceempty = field.getAttribute(forceemptyattr);

		if ((scope == Scope.SCOPE_GLOBAL || scope == Scope.SCOPE_SOURCE) && dbsync.isValidSync(synclist,Scope.SCOPE_SOURCE))
			setFeature(Scope.SCOPE_SOURCE,FieldFeature.FIELD_FEATURE_ONMULTIPLE,new FieldValue<>(field,onmultipleoper),field.isAttributeNoDefault(onmultipleattr));

		if ((scope == Scope.SCOPE_GLOBAL || scope == Scope.SCOPE_DESTINATION) && dbsync.isValidSync(synclist,Scope.SCOPE_DESTINATION))
			setFeature(Scope.SCOPE_DESTINATION,FieldFeature.FIELD_FEATURE_ONMULTIPLE,new FieldValue<>(field,onmultipleoper),field.isAttributeNoDefault(onmultipleattr));

		if (dbsync.isValidSync(synclist,Scope.SCOPE_GLOBAL))
		{
			setFeature(Scope.SCOPE_GLOBAL,FieldFeature.FIELD_FEATURE_IGNORE_CASE,new FieldValue<>(field,ignorecaseoper),field.isAttributeNoDefault(ignorecaseattr));
			setFeature(Scope.SCOPE_GLOBAL,FieldFeature.FIELD_FEATURE_TYPE,new FieldValue<>(field,typeoper),field.isAttributeNoDefault(typeattr));
			setFeature(Scope.SCOPE_GLOBAL,FieldFeature.FIELD_FEATURE_DEVIATION,new FieldValue<>(field,deviation),field.isAttributeNoDefault(deviationattr));
		}

		if (Misc.isLog(10)) Misc.log("Setting field " + field.getName() + " to pos " + pos + " isdefault=" + isdefault);

		if (pos != -1)
			fields.set(pos,field);
		else if (isdefault)
			fields.add(field);
		else
		{
			int x = 0;
			for(;x < fields.size();x++)
				if (fields.get(x).isDefault())
					break;
			fields.add(x,field);
		}

		if (Misc.isLog(30))
		{
			StringBuilder sb = new StringBuilder();
			int x = 1;
			for(Field dumpfield:fields)
			{
				sb.append(": " + x + "=" + dumpfield);
				x++;
			}
			Misc.log("Field list" + sb);
		}
	}

	public Set<String> getNames()
	{
		return namefields;
	}

	public Set<String> getNames(Sync sync) throws AdapterException
	{
		Set<String> names = new LinkedHashSet<>();
		Map<String,String> renamed_names = new HashMap<>();
		for(Field field:fields)
		{
			Scope scope = field.getScope();
			if (scope != Scope.SCOPE_GLOBAL && scope != sync.getScope())
				continue;
			String name = field.getName();
			String newname = field.getNewName();
			if (newname != null) renamed_names.put(name,newname);
			if ((field.hasValue() || names.contains(name)) && field.isValid(sync))
			{
				newname = renamed_names.get(name);
				if (field.isDefault() && newname != null)
					names.add(newname);
				else
					addName(names,field);
			}
		}
                return names;
	}

	public Set<String> getKeys()
	{
		return keyfields;
	}

	public Set<String> getKeysNotUsed(Sync sync) throws AdapterException
	{
		Set<String> keys = new LinkedHashSet<String>(keyfields);
		for(Field field:fields)
		{
			if (field.isKey() || !field.isValid(sync)) continue;
			Scope scope = field.getScope();
			if (scope != Scope.SCOPE_GLOBAL && scope != sync.getScope())
				continue;
			keys.remove(field.getNewName() == null ? field.getName() : field.getNewName());
			if (field.getCopyName() != null) keys.remove(field.getCopyName());
		}
		return keys;
	}

	public Set<String> getKeysUsed(Sync sync) throws AdapterException
	{
		Set<String> keys = new LinkedHashSet<String>(keyfields);
		keys.removeAll(getKeysNotUsed(sync));
		return keys;
	}

	public ArrayList<Field> getFields()
	{
		return fields;
	}

	private void doFunction(Map<String,String> result,XML function) throws AdapterException
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

	public Map<String,String> getNext(Sync sync) throws AdapterException
	{
		try {
			return getNextSub(sync);
		} catch(AdapterException ex) {
			Misc.rethrow(ex,"ERROR: Exception generated while reading " + sync.getDescription());
		}
		return null;
	}

	public Map<String,String> getNextSub(Sync sync) throws AdapterException
	{
		final String traceid = "GETNEXT";
		Map<String,String> result;

		boolean doprocessing = !sync.isProcessed();
		if (!doprocessing && Misc.isLog(30))
			Misc.log("Field: Skipping field processing since already done during sort");

		while((result = sync.next()) != null)
		{
			String rawkey = dbsync.getKey(result);
			if (dbsync.getTraceKeys().contains(rawkey)) Misc.setTrace(traceid);
			if (Misc.isLog(30))
				Misc.log((doprocessing ? "Processing" : "No processing") + " record " + sync.getName() + ": " + Misc.implode(result));

			Set<String> keyset = getKeys();
			Set<String> usedset = new HashSet<>();

			fieldloop: for(Field field:fields)
			{
				String name = field.getName();
				boolean iskey = keyset.contains(name);
				String value = result.get(name);

				if (doprocessing)
				{
					SyncLookup lookup = field.getLookup();

					if (Misc.isLog(30)) Misc.log("Field: [" + dbsync.getDisplayKey(keyset,result) + "] Check " + field + ":" + value + ":" + sync.getXML().getTagName() + ":" + (dbsync.getSourceSync() == null ? "NOSRC" : dbsync.getSourceSync().getName()) + ":" + (dbsync.getDestinationSync() == null ? "NODEST" : dbsync.getDestinationSync().getName()) + ":" + result);

					if (!field.isValid(sync)) continue;
					if (!field.isValidFilter(result)) continue;
					if (!field.isDefault()) usedset.add(name);

					if (Misc.isLog(30)) Misc.log("Field: " + name + " is valid");

					if (field.getOnMissing() == OnOper.EXCEPTION && !result.containsKey(name))
						throw new AdapterException("[" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Required field '" + name + "' missing: " + result);

					try {
						FieldResult fieldresult = new FieldResult(sync,field,result);
						SyncLookupResultErrorOperation erroroper = lookup.check(fieldresult);
						if (Misc.isLog(30)) Misc.log("Lookup check result " + erroroper.getType());
						switch(erroroper.getType())
						{
						case NONE:
							if (iskey && value == null && field.getOnEmpty() == null) continue; // Skip not updated keys
							break;
						case NEWVALUE:
							value = result.get(name);
							break;
						case ERROR:
							if (sync.isErrors()) Misc.log("ERROR: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting record since lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "for field '" + field.getName() + "' failed: " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + Misc.implode(result,fieldresult.getFields()));
						case REJECT_RECORD:
							result = null;
							break fieldloop;
						case WARNING:
							if (sync.isErrors()) Misc.log("WARNING: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting field '" + field.getName() + "' since lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "failed: " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + Misc.implode(result,fieldresult.getFields()));
						case REJECT_FIELD:
							if (iskey)
								result.put(name,"");
							else
								result.remove(name);
							continue;
						case EXCEPTION:
							throw new AdapterException("[" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Invalid lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "for field " + field.getName() + ": " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + Misc.implode(result,fieldresult.getFields()));
						}
					} catch (AdapterException ex) {
						Misc.rethrow(ex);
					}
				} else if (value == null) continue; // Skip field if already ignored during pre-processing

				if (value == null) value = "";

				boolean emptyvalueforced = field.isForceEmpty(value);
				if (doprocessing)
				{
					if (emptyvalueforced) value = "";

					if (field.isStrip())
					{
						String escape = "[\\s-/.,:;\\|]";
						value = value.replaceAll(escape + "*$","").replaceAll("^" + escape + "*","");
					}

					if (Misc.isLog(30)) Misc.log("Field: " + name + " is finally set to: " + value);
				}

				if (value.contains("\n"))
				{
					OnOper onmultiple = getFeature(sync.getScope(),FieldFeature.FIELD_FEATURE_ONMULTIPLE,name,result);
					if (onmultiple != null) switch(onmultiple)
					{
					case REJECT_RECORD:
						result = null;
						break fieldloop;
					case ERROR:
						OnOper onempty = field.getOnEmpty();
						if (onempty != OnOper.WARNING)
						{
							if (sync.isErrors()) Misc.log("ERROR: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting record since field '" + field.getName() + "' contains multiple values: " + value);
							result = null;
							break fieldloop;
						}
						// No break here is on purpose
					case WARNING:
					case REJECT_FIELD:
						if (onmultiple == OnOper.WARNING || onmultiple == OnOper.ERROR)
							if (sync.isErrors()) Misc.log("WARNING: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting field '" + field.getName() + "' since it contains multiple values: " + value);
						if (iskey)
							result.put(name,"");
						else
							result.remove(name);
						continue;
					case MERGE:
						TreeSet<String> sortedmerge = new TreeSet<>(DB.getInstance().getCollatorIgnoreCase());
						sortedmerge.addAll(Arrays.asList(value.split("\n")));
						value = Misc.implode(sortedmerge,",");
						break;
					case USE_FIRST:
						String[] values = value.split("\n");
						value = values.length > 0 ? values[0] : "";
						if (sync.getReader().toTrim()) value = value.trim();
						break;
					}
				}

				if (doprocessing && field.isDefault() && usedset.contains(name))
				{
					if (Misc.isLog(30)) Misc.log("Field: Skipping default field " + name + " since already used");
					continue;
				}

				result.put(name,value);

				if (doprocessing)
				{
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
						if (keyset.contains(newname))
							throw new AdapterException("Renaming \"" + name + "\" to \"" + newname + "\" overwriting an existing key is not supported as it causes unexpected behaviors (use copy instead)");
						result.put(newname,value);
						if (Misc.isLog(30)) Misc.log("Field: " + name + " renamed to " + newname + ": " + value);
						name = newname;
					}

					if (emptyvalueforced || !value.isEmpty())
						continue;

					// No value found...
					OnOper onempty = field.getOnEmpty();
					if (onempty != null) switch(onempty)
					{
					case REJECT_FIELD:
						if (Misc.isLog(30)) Misc.log("REJECTED empty field: " + field.getName());
						if (!iskey) result.remove(name);
						if (copyname != null && !isKey(copyname)) result.remove(copyname);
						break;
					case REJECT_RECORD:
						if (Misc.isLog(30)) Misc.log("REJECTED record: " + result);
						result = null;
						break fieldloop;
					case WARNING:
						if (sync.isErrors()) Misc.log("WARNING: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting field '" + field.getName() + "' since empty");
						if (!iskey) result.remove(name);
						if (copyname != null && !isKey(copyname)) result.remove(copyname);
						break;
					case ERROR:
						if (sync.isErrors()) Misc.log("ERROR: [" + sync.getName() + ":" + dbsync.getDisplayKey(keyset,result) + "] Rejecting record since field '" + field.getName() + "' is empty");
						result = null;
						break fieldloop;
					case EXCEPTION:
						throw new AdapterException("Field " + field.getName() + " is empty: " + result);
					}
				}
			}

			if (result == null) continue;

			if (doprocessing)
			{
				XML function = sync.getXML().getElement("element_function");
				String forsync = function == null ? null : function.getAttribute("for_sync");
				if (forsync == null || dbsync.isValidSync(forsync.split("\\s*,\\s*"),sync.getScope()))
					doFunction(result,function);

				function = sync.getXML().getParent().getElement("element_function");
				forsync = function == null ? null : function.getAttribute("for_sync");
				if (forsync == null || dbsync.isValidSync(forsync.split("\\s*,\\s*"),sync.getScope()))
					doFunction(result,function);
			}

			if (result.isEmpty())
			{
				if (Misc.isLog(30)) Misc.log("REJECTED because empty");
				continue;
			}

			if (Misc.isLog(30)) Misc.log("PASSED: " + result);
			sync.makeDump(result);
			Misc.clearTrace(traceid);
			return result;
		}

		sync.closeDump();
		Misc.clearTrace(traceid);
		return null;
	}

	public void updateCache(HashMap<String,String> row)
	{
		if (!dbsync.isCache()) return;
		if (row.isEmpty()) return;
		for(Field field:fields)
			field.updateCache(row);
	}
}
