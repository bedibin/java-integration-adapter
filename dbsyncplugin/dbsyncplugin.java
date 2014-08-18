import java.util.*;
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
			System.exit(1);
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

class UpdateSubscriber extends Subscriber
{
	protected boolean stoponerror = false;
	private String keyvalue;
	private String[] displayfields;

	protected String getKeyValue()
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
		String instance = xml.getAttribute("instance");
		if (instance == null) return null;
		String name = xml.getAttribute("name");
		if (name == null) return null;
		String type = xml.getAttribute("type");
		if (type == null || !type.equals("db")) return null;
		String table = xml.getAttribute("table");

		String path = "/configuration/dbsync[@name='" + name + "']/destination[@type='" + type + "' and @instance='" + instance + "'";
		if (table != null) path += " and @table='" + table + "'";
		path += "]";

		XML xmldest = javaadapter.getConfiguration().getElementByPath(path);
		if (xmldest == null) return null;

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

	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
		throw new AdapterException("Operation method must be overridden");
	}
}

class DatabaseUpdateSubscriber extends UpdateSubscriber
{
	protected DB db;
	private Rate rate;
	private String quotefield = "\"";

	public DatabaseUpdateSubscriber() throws Exception
	{
		db = DB.getInstance();
	}

	@Override
	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
		try
		{
			String oper = xmloper.getTagName();
			if (oper.equals("update")) update(xmldest,xmloper);
			else if (oper.equals("add")) add(xmldest,xmloper);
			else if (oper.equals("remove")) remove(xmldest,xmloper);
			else if (oper.equals("start")) start(xmldest,xmloper);
			else if (oper.equals("end")) end(xmldest,xmloper);
		}
		catch(SQLException ex)
		{
			if (stoponerror) Misc.rethrow(ex);
			Misc.log("ERROR: [" + getKeyValue() + "] " + ex.getMessage() + Misc.CR + "XML message was: " + xmloper);
		}
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

		rate = new Rate();
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

		if (Misc.isLog(2))
		{
			String count = rate.getCount();
			if (!count.equals("0"))
			{
				String msg = "Final processing rate: " + rate.getMax() + " a second, count: " + count;
				String add = xml.getAttribute("add");
				if (add != null) msg += " add: " + add;
				String update = xml.getAttribute("update");
				if (update != null) msg += " update: " + update;
				String remove = xml.getAttribute("remove");
				if (remove != null) msg += " remove: " + remove;
				String total = xml.getAttribute("total");
				if (total != null) msg += " total record: " + total;
				Misc.log(msg);
			}
		}
	}

	private boolean customsql(String oper,XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		XML sqlxml = xmldest.getElement("custom" + oper + "sql");
		if (sqlxml == null) return false;
		String sql = sqlxml.getValue();
		if (sql == null) return false;

		XML[] fields = xml.getElements();
		for(XML field:fields)
			sql = sql.replace("%" + field.getTagName() + "%",db.getValue(field));

		rate.toString();
		if (Misc.isLog(10)) Misc.log(oper + ": " + sql);
		db.execsql(instance,sql);

		return true;
	}

	private String getWhereClause(XML xmldest,XML xml) throws Exception
	{
		XML[] fields = xml.getElements();

		String sep = "where";
		String sql = "";

		for(XML field:fields)
		{
			String type = field.getAttribute("type");
			if (type == null || !type.equals("key")) continue;

			String name = field.getTagName();
			sql += " " + sep + " (" + quotefield + name + quotefield;
			String value = field.getValue();
			if (value == null) value = field.getValue("oldvalue",null);
			if (value == null)
				sql += " is null";
			else
			{
				String[] valuesplit = value.split("\n");
				if (valuesplit.length == 1)
					sql += " = " + db.getValue(value,name);
				else
				{
					for(int i = 0;i < valuesplit.length;i++)
					{
						if (i != 0) sql += " or " + quotefield + name + quotefield;
						sql += " = " + db.getValue(valuesplit[i],name);
					}
				}
			}
			sql += ")";
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

		return sql;
	}

	private void handleRetryException(Exception ex,XML xmldest,XML xml,boolean isretry) throws Exception
	{
		String table = xmldest.getAttribute("table");
		String instance = xmldest.getAttribute("instance");
		XML[] fields = xml.getElements();

		String message = ex.getMessage();
		if (message == null) message = "";
		if (Misc.isLog(20)) Misc.log("Retry exception: " + message);

		if (message.indexOf("unique constraint") != -1 || message.indexOf("contrainte unique") != -1 || message.indexOf("ORA-00001:") != -1 || message.indexOf("duplicate key") != -1 || message.indexOf("NoDupIndexTriggered") != -1)
		{
			if (Misc.isLog(15))
			{
				Misc.log("Extracting current duplicate since message: " + message);
				String sqlchk = "select * from " + table + getWhereClause(xmldest,xml);
				db.execsql(instance,sqlchk);
			}

			if (!isretry)
			{
				String ondups = xmldest.getAttribute("on_duplicates");
				if (ondups == null || ondups.equals("merge"))
				{
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
				}
				else if (ondups.equals("recreate"))
				{
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Record will be automatically recreated with XML message: " + xml);
					remove(xmldest,xml);
					add(xmldest,xml,true);
				}
				else if (ondups.equals("warning"))
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Error: " + message + Misc.CR + "Ignored XML message: " + xml);
				else if (ondups.equals("error"))
					Misc.log(1,"ERROR: [" + getKeyValue() + "] Record already present. Error: " + message + Misc.CR + "Ignored XML message: " + xml);
				else if (ondups.equals("ignore"));
				else
					throw new AdapterException(xmldest,"Invalid on_duplicates attribute " + ondups);
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

		String sql = "insert into " + table + " (";
		String fieldnames = null;

		XML[] fields = xml.getElements();
		for(XML field:fields)
		{
			String type = field.getAttribute("type");
			if (type != null && type.equals("info")) continue;

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
			String type = field.getAttribute("type");
			if (type != null && type.equals("info")) continue;

			sql += sep + DB.replacement;
			list.add(field.getValue());
			sep = ",";
		}
		sql += ")";

		rate.toString();
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
				list.add(value);
				sep = ",";
			}
			sql += getWhereClause(xmldest,xml);
		}

		rate.toString();
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
			String type = field.getAttribute("type");
			if (type != null && (type.equals("info") || type.equals("key") || (type.equals("initial") && oldvalue != null))) continue;

			sql += " " + sep + " " + quotefield + field.getTagName() + quotefield + " = " + DB.replacement;
			list.add(field.getValue());
			sep = ",";
		}

		if (sep.equals("set")) return;

		sql += getWhereClause(xmldest,xml);

		rate.toString();
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

