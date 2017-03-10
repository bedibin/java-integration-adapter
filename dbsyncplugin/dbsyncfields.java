import java.util.*;
import java.io.*;

enum Scope { SCOPE_GLOBAL, SCOPE_SOURCE, SCOPE_DESTINATION };
enum OnOper { ignore, warning, error, reject_field, reject_record, use_key, exception, merge, recreate, suffix, clear };

class Field
{
	private XML xmlfield;
	private SyncLookup lookup;
	private String typeoverride;
	private String name;
	private String newname;
	private boolean dostrip = false;
	private boolean hasvalue = false;
	private boolean isdefault = false;
	private String copyname;
	private String filtername;
	private String filterresult;
	private boolean iskey = false;

	private OnOper onempty = OnOper.ignore;
	private String forceemptyvalue;
	private OnOper onmultiple = OnOper.ignore;
	private boolean onmultiple_present = false;
	private String ifexists;
	private String synclist[];
	private Scope scope;
	private boolean ignorecase = false;

	public Field(XML xml,Scope scope) throws Exception
	{
		xmlfield = xml;
		this.scope = scope;
		name = xml.getAttribute("name");
		if (Misc.isLog(10)) Misc.log("Initializing field " + name);
		newname = xml.getAttribute("rename");
		String strip = xml.getAttributeDeprecated("strip");
		if (strip != null && strip.equals("true")) dostrip = true;
		copyname = xml.getAttribute("copy");
		filtername = xml.getAttribute("filter");
		filterresult = xml.getAttribute("filter_result");

		xml.setAttributeDeprecated("on_not_found","on_empty");
		onempty = getOnOper(xml,"on_empty",OnOper.ignore,EnumSet.of(OnOper.ignore,OnOper.reject_field,OnOper.reject_record,OnOper.warning,OnOper.error,OnOper.exception));

		String ignorecasestr = xml.getAttribute("ignore_case");
		if (ignorecasestr != null && ignorecasestr.equals("true"))
			ignorecase = true;
		forceemptyvalue = xml.getAttribute("force_empty_value");
		onmultiple = getOnOper(xml,"on_multiple",OnOper.ignore,EnumSet.of(OnOper.ignore,OnOper.error,OnOper.warning,OnOper.merge,OnOper.reject_record,OnOper.reject_field));
		onmultiple_present = xml.isAttributeNoDefault("on_multiple");
		ifexists = null;
		if (xml.isAttribute("if_exists"))
		{
			ifexists = xml.getAttribute("if_exists");
			if (ifexists == null) ifexists = "";
		}
		String forsync = xml.getAttribute("for_sync");
		if (forsync != null) synclist = forsync.split("\\s*,\\s*");

		typeoverride = xml.getAttribute("type");

		lookup = new SyncLookup(this);
		hasvalue = lookup.getCount() > 0 || "true".equals(xml.getAttribute("hasvalue"));
		isdefault = "true".equals(xml.getAttribute("isdefault"));
	}

	public Field(String name) throws Exception
	{
		this.iskey = true;
		this.scope = Scope.SCOPE_GLOBAL;
		this.name = name;
		this.hasvalue = true;
		lookup = new SyncLookup(this);
	}

	static public OnOper getOnOper(XML xml,String attr,OnOper def,EnumSet<OnOper> validset) throws AdapterException
	{
		String value = xml.getAttribute(attr);
		if (value == null) return def;

		try {
			OnOper result = Enum.valueOf(OnOper.class,value);
			if (validset != null && !validset.contains(result))
				throw new AdapterException(xml,"Invalid attribute '" + attr + "' value: " + value);
			return result;
		} catch(IllegalArgumentException ex) {
			throw new AdapterException(xml,"Invalid attribute '" + attr + "' value: " + value);
		}
	}

	public XML getXML()
	{
		return xmlfield;
	}

	public void setScope(Scope scope) throws AdapterException
	{
		if (!isdefault) throw new AdapterException("Cannot change scope on non default field");
		this.scope = scope;
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

	public boolean getOnMultiplePresent()
	{
		return onmultiple_present;
	}

	public OnOper getOnMultiple()
	{
		return onmultiple;
	}

	public OnOper getOnEmpty()
	{
		return onempty;
	}

	public void setOnMultiple(OnOper onmultiple)
	{
		this.onmultiple = onmultiple;
	}

	public boolean isStrip()
	{
		return dostrip;
	}

	public String getCopyName()
	{
		return copyname;
	}

	public String getType()
	{
		if (typeoverride != null) return typeoverride;
		if (iskey) return "key";
		return null;
	}

	public boolean hasValue()
	{
		return hasvalue;
	}

	public boolean isDefault()
	{
		return isdefault;
	}

	public boolean isIgnoreCase()
	{
		return ignorecase;
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

	public boolean isValid(Sync sync) throws Exception
	{
		boolean result = sync.getDBSync().isValidSync(synclist,sync);
		if (!result) return false;
		if (scope == Scope.SCOPE_SOURCE && sync.getScope() != Scope.SCOPE_SOURCE) return false;
		if (scope == Scope.SCOPE_DESTINATION && sync.getScope() != Scope.SCOPE_DESTINATION) return false;
		return true;
	}

	public boolean isValidFilter(LinkedHashMap<String,String> result,String name) throws Exception
	{
		if (ifexists != null)
		{
			if (ifexists.isEmpty()) ifexists = name;
			String[] keys = ifexists.split("\\s*,\\s*");
			for(String key:keys)
			{
				String value = result.get(key);
				if (value == null || value.isEmpty()) return false;
			}
		}
		if (filtername == null && filterresult == null) return true;

		XML xml = new XML();
		xml.add("root",result);
		if (Misc.isLog(30)) Misc.log("Looking for filter " + filtername + " [" + filterresult + "]: " + xml);

		return Misc.isFilterPass(xmlfield,xml);
	}

	public void updateCache(HashMap<String,String> row)
	{
		lookup.updateCache(row);
	}
}

class Fields
{
	private LinkedHashSet<String> keyfields;
	private LinkedHashSet<String> namefields;
	private ArrayList<Field> fields;
	private EnumMap<Scope,Boolean> default_set = new EnumMap<Scope,Boolean>(Scope.class);
	private DBSyncOper dbsync;

	public Fields(DBSyncOper dbsync,String[] keyfields) throws Exception
	{
		this.dbsync = dbsync;
		this.keyfields = new LinkedHashSet<String>(Arrays.asList(keyfields));
		this.namefields = new LinkedHashSet<String>(Arrays.asList(keyfields));
		fields = new ArrayList<Field>();
		for(String keyfield:keyfields)
			fields.add(new Field(keyfield));
	}

	private void setDefaultFields(LinkedHashMap<String,String> result,Scope scope) throws Exception
	{
		// To be done once
		Boolean isset = default_set.get(scope);
		if (isset != null && isset.booleanValue() == true) return;
		default_set.put(scope,Boolean.TRUE);
		XML xml = new XML();
		xml = xml.add("field");
		for(String name:result.keySet())
		{
			boolean exists = false;
			for(Field field:fields)
			{
				if (name.equals(field.getName()) && (field.getScope() == Scope.SCOPE_GLOBAL || field.getScope() == scope))
				{
					exists = true;
					break;
				}
				else if (name.equals(field.getName()) && field.getScope() != scope && field.isDefault())
				{
					field.setScope(Scope.SCOPE_GLOBAL);
					exists = true;
					break;
				}
			}
			if (exists) continue;

			xml.setAttribute("name",name);
			xml.setAttribute("hasvalue","true");
			xml.setAttribute("isdefault","true");
			add(new Field(xml,scope));
		}
	}

	public void addDefaultVar(XML xml) throws Exception
	{
		XML.setDefaultVariable(xml.getAttribute("name"),xml.getAttribute("value"));
	}

	public void removeDefaultVar(XML xml) throws Exception
	{
		XML.setDefaultVariable(xml.getAttribute("name"),null);
	}

	public void add(Field field) throws Exception
	{
		namefields.add(field.getName());

		OnOper default_onmultiple = null;
		for(Field prevfield:fields)
			if (prevfield.getName().equals(field.getName()) && (prevfield.getScope() == Scope.SCOPE_GLOBAL || prevfield.getScope() == field.getScope()) && prevfield.getOnMultiplePresent())
			{
				default_onmultiple = prevfield.getOnMultiple();
				break;
			}
		if (!field.getOnMultiplePresent() && default_onmultiple != null)
			field.setOnMultiple(default_onmultiple);

		fields.add(field);
	}

	public Set<String> getNames()
	{
		return namefields;
	}

	public Set<String> getNames(Sync sync) throws Exception
	{
		Set<String> names = new LinkedHashSet<String>();
		for(Field field:fields)
			if (field.hasValue() && field.isValid(sync))
				names.add(field.getName());
                return names;
	}

	public Set<String> getKeys()
	{
		return keyfields;
	}

	public ArrayList<Field> getFields()
	{
		return fields;
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
		try {
			return getNextSub(sync);
		} catch(Exception ex) {
			Misc.rethrow(ex,"ERROR: Exception generated while reading " + sync.getDescription());
		}
		return null;
	}

	public LinkedHashMap<String,String> getNextSub(Sync sync) throws Exception
	{
		final String traceid = "GETNEXT";
		LinkedHashMap<String,String> result;

		boolean doprocessing = !sync.isProcessed();
		if (!doprocessing && Misc.isLog(30))
			Misc.log("Field: Skipping field processing since already done during sort");

		while((result = sync.next()) != null)
		{
			if (doprocessing) setDefaultFields(result,sync.getScope());
			if (doprocessing) fieldloop: for(Field field:fields)
			{
				String rawkey = dbsync.getKey(result);
				if (dbsync.getTraceKeys().contains(rawkey))
				{
					Misc.setTrace(traceid);
					Misc.log("Record " + sync.getName() + ": " + Misc.implode(result));
				}

				Set<String> keyset = getKeys();
				String keys = dbsync.getDisplayKey(keyset,result);

				String name = field.getName();
				boolean iskey = keyset.contains(name);
				String value = result.get(name);
				SyncLookup lookup = field.getLookup();

				if (Misc.isLog(30)) Misc.log("Field: [" + keys + "] Check " + name + ":" + value + ":" + sync.getXML().getTagName() + ":" + (dbsync.getSourceSync() == null ? "NOSRC" : dbsync.getSourceSync().getName()) + ":" + (dbsync.getDestinationSync() == null ? "NODEST" : dbsync.getDestinationSync().getName()) + ":" + result);
				if (!field.isValid(sync)) continue;
				if (!field.isValidFilter(result,name)) continue;

				if (Misc.isLog(30)) Misc.log("Field: " + name + " is valid");

				try {
					SyncLookupResultErrorOperation erroroper = lookup.check(result,name);
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
						Misc.log("ERROR: [" + sync.getName() + ":" + keys + "] Rejecting record since lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "for field " + field.getName() + " failed: " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + result);
					case REJECT_RECORD:
						result = null;
						break fieldloop;
					case WARNING:
						Misc.log("WARNING: [" + sync.getName() + ":" + keys + "] Rejecting field " + field.getName() + " since lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "failed: " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + result);
					case REJECT_FIELD:
						if (iskey)
							result.put(name,"");
						else
							result.remove(name);
						continue;
					case EXCEPTION:
						throw new AdapterException("[" + sync.getName() + ":" + keys + "] Invalid lookup " + (erroroper.getName() == null ? "" : "[" + erroroper.getName() + "] ") + "for field " + field.getName() + ": " + (erroroper.getMessage() == null ? "" : erroroper.getMessage() + ": ") + result);
					}
				} catch (Exception ex) {
					Misc.rethrow(ex);
				}

				if (value == null) value = "";
				boolean emptyvalueforced = field.isForceEmpty(value);
				if (emptyvalueforced) value = "";

				if (field.isStrip())
				{
					String escape = "[\\s-/.,:;\\|]";
					value = value.replaceAll(escape + "*$","").replaceAll("^" + escape + "*","");
				}

				if (Misc.isLog(30)) Misc.log("Field: " + name + " is finally set to: " + value);

				if (value.contains("\n"))
				{
					OnOper onmultiple = field.getOnMultiple();
					switch(onmultiple)
					{
					case error:
					case reject_record:
						OnOper onempty = field.getOnEmpty();
						if (onempty == OnOper.reject_record || onempty == OnOper.error)
						{
							if (onmultiple == OnOper.error)
								Misc.log("ERROR: [" + sync.getName() + ":" + keys + "] Rejecting record since field " + field.getName() + " contains multiple values: " + value);
							result = null;
							break fieldloop;
						}
						// No break here is on purpose
					case warning:
					case reject_field:
						if (onmultiple == OnOper.warning || onmultiple == OnOper.error)
							Misc.log("WARNING: [" + sync.getName() + ":" + keys + "] Rejecting field " + field.getName() + " since it contains multiple values: " + value);
						if (iskey)
							result.put(name,"");
						else
							result.remove(name);
						continue;
					case merge:
						value = Misc.implode(value.split("\n"),",");
						break;
					}
				}

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
					if (keyset.contains(newname)) iskey = true;
					value = result.remove(name);
					if (result.containsKey(newname))
						throw new AdapterException("Renaming \"" + name + "\" to \"" + newname + "\" overwriting an existing field is not supported as it causes unexpected behaviors (use copy instead)");
					result.put(newname,value);
					if (Misc.isLog(30)) Misc.log("Field: " + name + " renamed to " + newname + ": " + value);
					name = newname;
				}

				if (emptyvalueforced || !value.isEmpty())
					continue;

				// No value found...
				switch(field.getOnEmpty())
				{
				case reject_field:
					if (Misc.isLog(30)) Misc.log("REJECTED empty field: " + field.getName());
					if (iskey)
						result.put(name,"");
					else
						result.remove(name);
					if (copyname != null) result.remove(copyname);
					break;
				case reject_record:
					if (Misc.isLog(30)) Misc.log("REJECTED record: " + result);
					result = null;
					break fieldloop;
				case warning:
					Misc.log("WARNING: [" + sync.getName() + ":" + keys + "] Rejecting field " + field.getName() + " since empty: " + result);
					if (iskey)
						result.put(name,"");
					else
						result.remove(name);
					if (copyname != null) result.remove(copyname);
					break;
				case error:
					Misc.log("ERROR: [" + sync.getName() + ":" + keys + "] Rejecting record since field " + field.getName() + " is empty: " + result);
					result = null;
					break fieldloop;
				case exception:
					throw new AdapterException("Field " + field.getName() + " is empty: " + result);
				}
			}

			if (result == null) continue;

			if (doprocessing)
			{
				XML function = sync.getXML().getElement("element_function");
				String forsync = function == null ? null : function.getAttribute("for_sync");
				if (forsync == null || dbsync.isValidSync(forsync.split("\\s*,\\s*"),sync))
					doFunction(result,function);

				function = sync.getXML().getParent().getElement("element_function");
				forsync = function == null ? null : function.getAttribute("for_sync");
				if (forsync == null || dbsync.isValidSync(forsync.split("\\s*,\\s*"),sync))
					doFunction(result,function);
			}

			if (result.isEmpty())
			{
				if (Misc.isLog(30)) Misc.log("REJECTED because empty");
				continue;
			}

			if (Misc.isLog(30)) Misc.log("PASSED: " + result);
			if (doprocessing) sync.makeDump(result);
			Misc.clearTrace(traceid);
			return result;
		}

		if (doprocessing) sync.closeDump();
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
