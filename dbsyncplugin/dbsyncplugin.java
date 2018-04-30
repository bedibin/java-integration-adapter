import java.util.*;
import java.util.regex.*;
import java.sql.SQLException;

class dbsyncplugin
{
	static public boolean preview_mode = false;
	static public boolean dumpcsv_mode = false;

	private dbsyncplugin() { }

	public static void main(String[] args)
	{
		DBSyncOper sync;

		String filename = javaadapter.DEFAULTCFGFILENAME;
		for(String arg:args)
		{
			if (arg.equals("-preview"))
				preview_mode = true;
			else if (arg.equals("-dumpcsv"))
				dumpcsv_mode = true;
			else
				filename = arg;
		}

		javaadapter.initShutdownHook();
		try
		{
			javaadapter.dohooks = false;
			javaadapter.init(filename);
			sync = new DBSyncOper();
			sync.run();
		}
		catch(Exception ex)
		{
			Misc.log(ex);
			Misc.exit(1,30000);
		}
	}
}

class DBSyncPluginHook extends Hook
{
	private DBSyncOper sync;

	public DBSyncPluginHook() throws Exception
	{
		sync = new DBSyncOper();
	}

	public DBSyncPluginHook(String classname,XML function) throws Exception
	{
		super(classname,function);
		sync = new DBSyncOper();
	}

	@Override
	public void run()
	{
		try
		{
			sync.run(getFunction(),null);
		}
		catch(Exception ex)
		{
			Misc.log(ex);
		}
	}

}

class DBSyncPluginSubscriber extends Subscriber
{
	private DBSyncOper sync;

	public DBSyncPluginSubscriber() throws Exception
	{
		sync = new DBSyncOper();
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		sync.run(getFunction(),xml);

		XML result = new XML();
		result.add("OK");

		return result;
	}
}

abstract class UpdateSubscriber extends Subscriber
{
	private boolean stoponerror = false;
	private String keyvalue;
	private String[] displayfields;
	private Rate rate;
	private int addcounter;
	private int removecounter;
	private int updatecounter;

	protected final String getKeyValue()
	{
		return keyvalue;
	}

	protected void setKeyValue(XML xml) throws Exception
	{
		keyvalue = "";

		LinkedHashMap<String,String> displayvalues = new LinkedHashMap<String,String>();
		if (displayfields != null) for(String field:displayfields)
			displayvalues.put(field,"");

		XML[] fields = xml.getElements();
		for(XML field:fields)
		{
			String name = field.getTagName();

			String fieldtype = field.getAttribute("type");
			if (!"key".equals(fieldtype))
			{
				if (displayvalues.containsKey(name)) displayvalues.put(name,field.getValue());
				continue;
			}
			String value = field.getValue();
			if (value == null) continue;
			if ("".equals(value)) continue;
			keyvalue = keyvalue.isEmpty() ? value : keyvalue + "," + value;
		}

		String displayvalue = Misc.implode(displayvalues.values());
		if (!displayvalue.isEmpty()) keyvalue = displayvalue + "/" + keyvalue;
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		String name = xml.getAttribute("name");
		if (name == null) throw new AdapterException(xml,"Missing name attribute");
		String type = xml.getAttribute("type");
		if (type == null) throw new AdapterException(xml,"Missing type attribute");

		String path = "/configuration/dbsync[@name='" + name + "']/destination[@type='" + type + "'";

		String instance = xml.getAttribute("instance");
		if (instance != null) path += " and @instance='" + instance + "'";
		String table = xml.getAttribute("table");
		if (table != null) path += " and @table='" + table + "'";

		path += "]";
		XML xmldest = javaadapter.getConfiguration().getElementByPath(path);
		if (xmldest == null) throw new AdapterException(xml,"Non matching destination element: " + path);

		String stop = xmldest.getAttribute("stop_on_error");
		if (stop != null && stop.equals("true")) stoponerror = true;

		String displayfield = xml.getAttribute("display_keyfield");
		if (displayfield != null) displayfields = displayfield.split("\\s*,\\s*");

		XML[] xmloperlist = xml.getElements(null);
		for(XML xmloper:xmloperlist)
		{
			if (javaadapter.isShuttingDown()) return null;
			setKeyValue(xmloper);
			oper(xmldest,xmloper);
		}

		XML result = new XML();
		result.add("ok");

		return result;
	}

	protected abstract void start(XML xmldest,XML xml) throws Exception;
	protected abstract void end(XML xmldest,XML xml) throws Exception;
	protected abstract void add(XML xmldest,XML xml) throws Exception;
	protected abstract void remove(XML xmldest,XML xml) throws Exception;
	protected abstract void update(XML xmldest,XML xml) throws Exception;

	private void startOper(XML xmldest,XML xml) throws Exception
	{
		addcounter = 0;
		removecounter = 0;
		updatecounter = 0;
		rate = new Rate();
		start(xmldest,xml);
	}

	private void addOper(XML xmldest,XML xml) throws Exception
	{
		addcounter++;
		rate.toString();
		add(xmldest,xml);
	}

	private void removeOper(XML xmldest,XML xml) throws Exception
	{
		removecounter++;
		rate.toString();
		remove(xmldest,xml);
	}

	private void updateOper(XML xmldest,XML xml) throws Exception
	{
		updatecounter++;
		rate.toString();
		update(xmldest,xml);
	}

	private void endOper(XML xmldest,XML xml) throws Exception
	{
		keyvalue = null;
		String add = xml.getAttribute("add");
		String update = xml.getAttribute("update");
		String remove = xml.getAttribute("remove");
		if ((add != null && Integer.parseInt(add) != addcounter)
				|| (remove != null && Integer.parseInt(remove) != removecounter)
				|| (update != null && Integer.parseInt(update) != updatecounter))
			throw new AdapterException("Invalid number of processed update entries. add: " + add + "/" + addcounter + " update: " + update + "/" + updatecounter + " remove: " + remove + "/" + removecounter);

		if (Misc.isLog(2))
		{
			String count = rate.getCount();
			if (!count.equals("0"))
			{
				int total = addcounter + removecounter + updatecounter;
				Misc.log("Final processing rate: " + rate.getMax() + " a second, count: " + count + " add: " + addcounter + " update: " + updatecounter + " remove: " + removecounter + " total record: " + total);
			}
		}

		rate = null;
		end(xmldest,xml);
	}

	protected String getExceptionMessage(Exception ex)
	{
		String msg = ex.getMessage();
		return msg == null ? ex.toString() : msg;
	}

	public final void oper(XML xmldest,XML xmloper) throws Exception
	{
		try
		{
			SyncOper oper = Enum.valueOf(SyncOper.class,xmloper.getTagName().toUpperCase());
			switch(oper)
			{
			case UPDATE:
				updateOper(xmldest,xmloper);
				break;
			case ADD:
				addOper(xmldest,xmloper);
				break;
			case REMOVE:
				removeOper(xmldest,xmloper);
				break;
			case START:
				startOper(xmldest,xmloper);
				break;
			case END:
				endOper(xmldest,xmloper);
				break;
			}
		}
		catch(Exception ex)
		{
			if (stoponerror) Misc.rethrow(ex);
			String key = getKeyValue();
			Misc.log("ERROR: " + (keyvalue == null ? "" : "[" + keyvalue + "] ") + getExceptionMessage(ex) + Misc.CR + "XML message was: " + xmloper);
		}
	}
}

class ParallelSubscriber extends UpdateSubscriber
{
	protected void start(XML xmldest,XML xml) throws Exception {}
	protected void end(XML xmldest,XML xml) throws Exception {}
	protected void add(XML xmldest,XML xml) throws Exception {}
	protected void remove(XML xmldest,XML xml) throws Exception {}
	protected void update(XML xmldest,XML xml) throws Exception {}
}

class DatabaseUpdateSubscriber extends UpdateSubscriber
{
	protected DB db;
	private String quotefield = "\"";

	public DatabaseUpdateSubscriber() throws Exception
	{
		db = DB.getInstance();
	}

	public void setQuoteField(String field)
	{
		quotefield = field;
	}

	protected void start(XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		XML[] xmlsqllist = xmldest.getElements("preinsertsql");
		for(XML xmlsql:xmlsqllist)
		{
			String sql = xmlsql.getValue();
			Misc.log(3,"preinsertsql: " + sql);
			db.execsql(instance,sql);
		}
	}

	protected void end(XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		XML[] xmlsqllist = xmldest.getElements("postinsertsql");
		for(XML xmlsql:xmlsqllist)
		{
			String sql = xmlsql.getValue();
			Misc.log(3,"postinsertsql: " + sql);
			db.execsql(instance,sql);
		}
	}

	private boolean customsql(String oper,XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		XML[] sqlxmllist = xmldest.getElements("custom" + oper + "sql");
		if (sqlxmllist.length == 0) return false;

		for(XML sqlxml:sqlxmllist)
		{
			if (!Misc.isFilterPass(sqlxml,xml)) continue;

			String sql = sqlxml.getValue();
			if (sql == null) return false;

			XML[] fields = xml.getElements();
			for(XML field:fields)
				sql = sql.replace("%" + field.getTagName() + "%",db.getValue(field));

			if (Misc.isLog(10)) Misc.log(oper + ": " + sql);
			db.execsql(instance,sql);
		}

		return true;
	}

	private String getWhereClause(XML xmldest,XML xml) throws Exception
	{
		XML[] fields = xml.getElements();

		String sep = "where";
		StringBuilder sql = new StringBuilder();

		for(XML field:fields)
		{
			String type = field.getAttribute("type");
			if (type == null || !type.equals("key")) continue;

			String name = field.getTagName();
			String value = field.getValue();
			if (value == null) value = field.getValue("oldvalue",null);

			sql.append(" " + sep + " " + db.getFieldEqualsValue(quotefield + name + quotefield,value));
			sep = "and";
		}

		if (!sep.equals("and")) throw new AdapterException(xml,"dbsync: SQL operation without a key");

		String idfield = xmldest.getAttribute("idfield");
		if (idfield != null)
		{
			String table = xmldest.getAttribute("table");
			String instance = xmldest.getAttribute("instance");

			String sqlid = "select " + idfield + " from " + table + sql;
			ArrayList<LinkedHashMap<String,String>> ids = db.execsql(instance,sqlid);
			if (ids.size() != 1)
			{
				Misc.log(1,"WARNING: Operation on table " + table + " cannot be done since key doesn't exists: " + sql);
				return " where false";
			}

			String id = Misc.getFirstValue(ids.get(0));
			return " where " + idfield + " = " + db.getValue(id);
		}

		return sql.toString();
	}

	private void handleRetryException(Exception ex,XML xmldest,XML xml,boolean isretry) throws Exception
	{
		String table = xmldest.getAttribute("table");
		String instance = xmldest.getAttribute("instance");
		XML[] fields = xml.getElements();

		String message = ex.getMessage();
		if (message == null) message = "";
		if (Misc.isLog(20)) Misc.log("Retry exception: " + message);

		Pattern ondupspattern = null;
		String ondupsmatch = xmldest.getAttribute("on_duplicates_match");
		if (ondupsmatch != null) ondupspattern = Pattern.compile(ondupsmatch);

		if ((ondupspattern != null && ondupspattern.matcher(message).find()) || (ondupspattern == null && (message.contains("unique constraint") || message.contains("contrainte unique") || message.contains("ORA-00001:") || message.contains("duplicate key") || message.contains("NoDupIndexTriggered") || message.contains("already exists") || message.contains("existe déjà"))))
		{
			if (Misc.isLog(15))
			{
				Misc.log("Extracting current duplicate since message: " + message);
				String sqlchk = "select * from " + table + getWhereClause(xmldest,xml);
				db.execsql(instance,sqlchk);
			}

			if (!isretry)
			{
				switch(Field.getOnOper(xmldest,"on_duplicates",OnOper.MERGE,EnumSet.of(OnOper.MERGE,OnOper.CLEAR,OnOper.SUFFIX,OnOper.RECREATE,OnOper.WARNING,OnOper.ERROR,OnOper.IGNORE)))
				{
				case MERGE:
					String mergefields = xmldest.getAttribute("merge_fields");
					String[] attrs = mergefields == null ? null : mergefields.split("\\s*,\\s*");

					String keyfields = xmldest.getAttribute("merge_keys");
					String[] keys = keyfields == null ? null : keyfields.split("\\s*,\\s*");

					XML updxml = new XML();
					XML xmlop = updxml.add("update");
					if (keys != null) for(String key:keys)
					{
						XML field = xml.getElement(key);
						if (field == null) throw new AdapterException(xmldest,"Invalid key '" + key + "' in merge_keys attribute: " + xml);
						String value = field.getValue("oldvalue");
						if (value == null) value = field.getValue();

						xmlop.add(key,value);
					}

					for(XML field:fields)
					{
						String tagname = field.getTagName();
						String type = field.getAttribute("type");
						if (keys == null && type != null && type.equals("key"))
							xmlop.add(field);
						if (type == null && (attrs == null || Arrays.asList(attrs).contains(tagname)))
						{
							XML xmlel = xmlop.add(tagname,field.getValue());
							String oldvalue = field.getValue("oldvalue","");
							xmlel.add("oldvalue",oldvalue);
						}
					}
					Misc.log(1,"WARNING: Record already present." + Misc.CR + "Updating record information with XML message: " + updxml);
					update(xmldest,updxml,true);
					break;
				case CLEAR:
					String clearfields = xmldest.getAttribute("clear_fields");
					if (clearfields == null) throw new AdapterException(xmldest,"clear on_duplicates requires clear_fields attribute");
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Clearing fields " + clearfields + ": " + xml);
					String sqlclear = "update " + table + " ";
					String sepclear = "set ";
					for(String field:clearfields.split("\\s*,\\s*"))
					{
						sqlclear += sepclear + field + "=NULL";
						sepclear = ",";
					}
					sqlclear += " " + getWhereClause(xmldest,xml);
					db.execsql(instance,sqlclear);
					break;
				case SUFFIX:
					String suffixfields = xmldest.getAttribute("suffix_fields");
					if (suffixfields == null) throw new AdapterException(xmldest,"suffix on_duplicates requires suffix_fields attribute");
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Adding suffix '_' to fields " + suffixfields + ": " + xml);
					String sqlsuf = "update " + table + " ";
					String sepsuf = "set ";
					for(String field:suffixfields.split("\\s*,\\s*"))
					{
						sqlsuf += sepsuf + field + "=" + db.getConcat(instance,field,"'_'");
						sepsuf = ",";
					}
					sqlsuf += " " + getWhereClause(xmldest,xml);
					db.execsql(instance,sqlsuf);
					break;
				case RECREATE:
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Record will be automatically recreated with XML message: " + xml);
					remove(xmldest,xml);
					add(xmldest,xml,true);
					break;
				case WARNING:
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Error: " + message + Misc.CR + "Ignored XML message: " + xml);	
					break;
				case ERROR:
					Misc.log(1,"ERROR: [" + getKeyValue() + "] Record already present. Error: " + message + Misc.CR + "Ignored XML message: " + xml);
				}
				return;
			}
		}

		Misc.rethrow(ex);
	}

	protected void add(XML xmldest,XML xml) throws Exception
	{
		add(xmldest,xml,false);
	}

	protected void add(XML xmldest,XML xml,boolean isretry) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		if (customsql("add",xmldest,xml)) return;

		String table = xmldest.getAttribute("table");
		if (table == null) throw new AdapterException(xmldest,"dbsync: destination 'table' attribute required");

		XML[] customs = xmldest.getElements("customadd");
		if (customs.length > 0)
			throw new AdapterException(xmldest,"customadd not supported yet for SQL operations");

		String sql = "insert into " + table + " (";
		String fieldnames = null;

		XML[] fields = xml.getElements();
		for(XML field:fields)
		{
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI)) continue;

			if (fieldnames == null)
				fieldnames = "";
			else
				fieldnames += ",";
			fieldnames += quotefield + field.getTagName() + quotefield;
		}

		sql += fieldnames + ") values (";

		String sep = "";
		ArrayList<String> list = new ArrayList<String>();
		for(XML field:fields)
		{
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI)) continue;

			sql += sep + DB.replacement;
			list.add(field.getValue());
			sep = ",";
		}
		sql += ")";

		if (Misc.isLog(10)) Misc.log("add SQL: " + sql);

		try
		{
			db.execsql(instance,sql,list);
		}
		catch(Exception ex)
		{
			handleRetryException(ex,xmldest,xml,isretry);
		}
	}

	protected void remove(XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		if (customsql("remove",xmldest,xml)) return;

		String table = xmldest.getAttribute("table");
		if (table == null) throw new AdapterException(xmldest,"dbsync: destination 'table' attribute required");

		ArrayList<String> list = new ArrayList<String>();
		String sql = "delete from " + table + getWhereClause(xmldest,xml);
		XML[] customs = xmldest.getElements("customremove");
		if (customs.length > 0)
		{
			sql = "update " + table;
			String sep = "set";
			for(XML custom:customs)
			{
				String name = custom.getAttribute("name");
				if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
				String value = custom.getAttribute("value");
				if (value == null) value = "";
				sql += " " + sep + " " + quotefield + name + quotefield + "=" + DB.replacement;
				list.add(Misc.substitute(value,xml));
				sep = ",";
			}
			sql += getWhereClause(xmldest,xml);
		}

		if (Misc.isLog(10)) Misc.log("remove SQL: " + sql);
		db.execsql(instance,sql,list);
	}

	protected void update(XML xmldest,XML xml) throws Exception
	{
		update(xmldest,xml,false);
	}

	protected void update(XML xmldest,XML xml,boolean isretry) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		if (customsql("update",xmldest,xml)) return;

		String table = xmldest.getAttribute("table");
		if (table == null) throw new AdapterException(xmldest,"dbsync: destination 'table' attribute required");

		String sql = "update " + table;
		String sep = "set";
		XML[] fields = xml.getElements();
		ArrayList<String> list = new ArrayList<String>();
		for(XML field:fields)
		{
			XML old = field.getElement("oldvalue");
			if (old == null) continue;

			String oldvalue = old.getValue();
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI || type == OnOper.KEY || (type == OnOper.INITIAL && oldvalue != null))) continue;

			sql += " " + sep + " " + quotefield + field.getTagName() + quotefield + " = " + DB.replacement;
			list.add(field.getValue());
			sep = ",";
		}

		if (sep.equals("set")) return;

		sql += getWhereClause(xmldest,xml);

		if (Misc.isLog(10)) Misc.log("update SQL: " + sql);

		try
		{
			db.execsql(instance,sql,list);
		}
		catch(Exception ex)
		{
			handleRetryException(ex,xmldest,xml,isretry);
		}
	}
}

