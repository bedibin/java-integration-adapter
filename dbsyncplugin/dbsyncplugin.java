import java.util.*;
import java.sql.SQLException;

class dbsyncplugin
{
	private dbsyncplugin() { }

	public static void main(String[] args)
	{
		DBSyncOper sync;

		String filename = javaadapter.DEFAULTCFGFILENAME;
		if (args.length > 0)
			filename = args[0];
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

class UpdateSQL
{
	protected DB db;
	private Rate rate;
	private String quotefield = "\"";

	public UpdateSQL() {};

	public UpdateSQL(DB db) throws Exception
	{
		this.db = db;
	}

	public void setQuoteField(String field)
	{
		quotefield = field;
	}

	private void start(XML xmldest,XML xml) throws Exception
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

	private void end(XML xmldest,XML xml) throws Exception
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

			sql += " " + sep + " (" + quotefield + field.getTagName() + quotefield;
			String value = field.getValue();
			if (value == null) value = field.getValue("oldvalue",null);
			if (value == null)
				sql += " is null";
			else
			{
				String[] valuesplit = value.split("\n");
				if (valuesplit.length == 1)
					sql += " = " + db.getValue(value);
				else
				{
					for(int i = 0;i < valuesplit.length;i++)
					{
						if (i != 0) sql += " or " + quotefield + field.getTagName() + quotefield;
						sql += " = " + db.getValue(valuesplit[i]);
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
				if (ondups == null || ondups.equals("recreate"))
				{
					Misc.log(1,"WARNING: Record already present." + Misc.CR + "Record will be automatically recreated with XML message: " + xml);
					remove(xmldest,xml);
					add(xmldest,xml,true);
				}
				else if (ondups.equals("error"))
					Misc.log(1,"ERROR: Record already present. Error: " + message + Misc.CR + "Ignored XML message: " + xml);
				else if (ondups.equals("merge"))
				{
					String mergefields = xmldest.getAttribute("merge_fields");
					String[] attrs = mergefields == null ? null : mergefields.split("\\s*,\\s*");
					XML updxml = new XML();
					XML xmlop = updxml.add("update");
					for(XML field:fields)
					{
						String type = field.getAttribute("type");
						if (type != null && type.equals("key"))
							xmlop.add(field);
						if (type == null && attrs == null)
						{
							XML xmlel = xmlop.add(field);
							xmlel.add("oldvalue","");
						}
					}
					if (attrs != null) for(String attr:attrs)
					{
						XML field = xml.getElement(attr);
						if (field != null)
						{
							String type = field.getAttribute("type");
							if (type == null || !type.equals("info"))
							{
								XML xmlel = xmlop.add(field);
								xmlel.add("oldvalue","");
							}
						}
					}
					Misc.log(1,"WARNING: Record already present." + Misc.CR + "Updating record information with XML message: " + updxml);
					update(xmldest,updxml,true);
				}
				else if (ondups.equals("ignore"));
				else
					throw new AdapterException(xmldest,"Invalid on_duplicates attribute " + ondups);
				return;
			}
		}

		Misc.rethrow(ex);
	}

	private void add(XML xmldest,XML xml) throws Exception
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

	private void remove(XML xmldest,XML xml) throws Exception
	{
		String instance = xmldest.getAttribute("instance");
		if (customsql("remove",xmldest,xml)) return;

		String table = xmldest.getAttribute("table");
		if (table == null) throw new AdapterException(xmldest,"dbsync: destination 'table' attribute required");

		String sql = "delete from " + table + getWhereClause(xmldest,xml);

		rate.toString();
		if (Misc.isLog(10)) Misc.log("remove SQL: " + sql);
		db.execsql(instance,sql);
	}

	private void update(XML xmldest,XML xml) throws Exception
	{
		update(xmldest,xml,false);
	}

	private void update(XML xmldest,XML xml,boolean isretry) throws Exception
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
			if (type != null && (type.equals("info") || type.equals("key") || (type.equals("initial") && old != null))) continue;

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

	public void oper(XML xmldest,XML xmloper) throws Exception
	{
		String oper = xmloper.getTagName();
		if (oper.equals("update")) update(xmldest,xmloper);
		else if (oper.equals("add")) add(xmldest,xmloper);
		else if (oper.equals("remove")) remove(xmldest,xmloper);
		else if (oper.equals("start")) start(xmldest,xmloper);
		else if (oper.equals("end")) end(xmldest,xmloper);
	}
}

class UpdateSubscriber extends Subscriber
{
	protected boolean stoponerror = false;

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

		XML[] xmloperlist = xml.getElements(null);
		for(XML xmloper:xmloperlist)
		{
			if (javaadapter.isShuttingDown()) return null;
			oper(xmldest,xmloper);
		}

		XML result = new XML();
		result.add("ok");

		return result;
	}

	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
	}
}

class DatabaseUpdateSubscriber extends UpdateSubscriber
{
	private UpdateSQL update;
	protected DB db;

	public DatabaseUpdateSubscriber() throws Exception
	{
		db = DB.getInstance();
		update = new UpdateSQL(db);
	}

	@Override
	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
		try
		{
			update.oper(xmldest,xmloper);
		}
		catch(SQLException ex)
		{
			if (stoponerror) Misc.rethrow(ex);
			Misc.log("ERROR: " + ex.getMessage() + Misc.CR + "XML message was: " + xmloper);
		}
	}

	public void close()
	{
		db.close();
	}
}

