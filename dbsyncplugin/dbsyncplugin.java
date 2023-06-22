import java.util.*;
import java.util.regex.*;
import java.util.concurrent.*;
import java.sql.SQLException;

class dbsyncplugin
{
	static private boolean previewmode = false;
	static private boolean dumpcsvmode = false;

	private dbsyncplugin() { }

	static boolean getPreviewMode() { return previewmode; }
	static boolean getDumpCsvMode() { return dumpcsvmode; }

	public static void main(String[] args)
	{
		DBSyncOper sync;

		String filename = javaadapter.DEFAULTCFGFILENAME;
		for(String arg:args)
		{
			if (arg.equals("-preview"))
				previewmode = true;
			else if (arg.equals("-dumpcsv"))
				dumpcsvmode = true;
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
			System.exit(0);
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

	public DBSyncPluginHook() throws AdapterException
	{
		sync = new DBSyncOper();
	}

	public DBSyncPluginHook(String classname,XML function) throws AdapterException
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
		catch(AdapterException ex)
		{
			Misc.log(ex);
		}
	}

}

class DBSyncPluginSubscriber extends Subscriber
{
	private DBSyncOper sync;

	public DBSyncPluginSubscriber() throws AdapterException
	{
		sync = new DBSyncOper();
	}

	@Override
	public XML run(XML xml) throws AdapterException
	{
		sync.run(getFunction(),xml);

		XML result = new XML();
		result.add("OK");

		return result;
	}
}

class UpdateDestInfo
{
	XML xml;
	boolean stoponerror = false;
	String instance;
	String idfield;
	String tablename;
	String username;
	String password;
	String url;
	ArrayList<String> preinsertlist = new ArrayList<>();
	ArrayList<String> postinsertlist = new ArrayList<>();
	HashMap<SyncOper,ArrayList<XML>> customsqloperlist = new HashMap<>();
	HashMap<SyncOper,ArrayList<XML>> customoperlist = new HashMap<>();
	Pattern ondupspattern;
	OnOper onduplicates;
	String[] mergefields;
	String[] mergekeys;
	String[] clearfields;
	String[] suffixfields;
	String[] displayfields;
	ExecutorService pool;
	BlockingQueue<XML> queue;

	UpdateDestInfo(XML xml) throws AdapterException
	{
		this.xml = xml;
		String stop = xml.getAttribute("stop_on_error");
		if (stop != null && stop.equals("true")) stoponerror = true;
		username = xml.getAttribute("username");
		password = xml.getAttributeCrypt("password");
		url = xml.getAttribute("url");
		instance = xml.getAttribute("instance_write");
		if (instance == null) instance = xml.getAttribute("instance");
		XML[] xmlsqllist = xml.getElements("preinsertsql");
		for(XML xmlsql:xmlsqllist)
			preinsertlist.add(xmlsql.getValue());
		xmlsqllist = xml.getElements("postinsertsql");
		for(XML xmlsql:xmlsqllist)
			postinsertlist.add(xmlsql.getValue());
		idfield = xml.getAttribute("idfield");
		tablename = xml.getAttribute("table");
		String displayfield = xml.getAttribute("display_keyfield");
		if (displayfield != null) displayfields = displayfield.split("\\s*,\\s*");
		String ondupsmatch = xml.getAttribute("on_duplicates_match");
		if (ondupsmatch != null) ondupspattern = Pattern.compile(ondupsmatch);

		SyncOper[] operlist = { SyncOper.ADD, SyncOper.REMOVE, SyncOper.UPDATE };
		for(SyncOper oper:operlist)
		{
			ArrayList<XML> customlist = new ArrayList<>();
			xmlsqllist = xml.getElements("custom" + oper.toString().toLowerCase());
			for(XML xmlsql:xmlsqllist)
				customlist.add(xmlsql);
			customoperlist.put(oper,customlist);
			customlist = new ArrayList<>();
			xmlsqllist = xml.getElements("custom" + oper.toString().toLowerCase() + "sql");
			for(XML xmlsql:xmlsqllist)
				customlist.add(xmlsql);
			customsqloperlist.put(oper,customlist);
		}
		onduplicates = Field.getOnOper(xml,"on_duplicates",OnOper.MERGE,EnumSet.of(OnOper.MERGE,OnOper.CLEAR,OnOper.SUFFIX,OnOper.RECREATE,OnOper.WARNING,OnOper.ERROR,OnOper.IGNORE));
		String mergefieldsattr = xml.getAttribute("merge_fields");
		if (mergefieldsattr != null) mergefields = mergefieldsattr.split("\\s*,\\s*");
		String mergekeysattr = xml.getAttribute("merge_keys");
		if (mergekeysattr != null) mergekeys = mergekeysattr.split("\\s*,\\s*");
		String clearfieldsattr = xml.getAttribute("clear_fields");
		if (clearfieldsattr != null) clearfields = clearfieldsattr.split("\\s*,\\s*");
		String suffixfieldsattr = xml.getAttribute("suffix_fields");
		if (suffixfieldsattr != null) suffixfields = suffixfieldsattr.split("\\s*,\\s*");

		String poolsize = xml.getAttribute("pool_size");
		if (poolsize != null)
		{
			pool = Executors.newFixedThreadPool(Integer.parseInt(poolsize));
			queue = new ArrayBlockingQueue<>(Integer.parseInt(poolsize));
		}
	}

	XML getXML() { return xml; }
	boolean getStopOnError() { return stoponerror; }
	String getUserName() { return username; }
	String getPassword() { return password; }
	String getUrl() { return url; }
	String getInstance() { return instance; }
	String getIdField() { return idfield; }
	String getTableName() { return tablename; }
	ArrayList<String> getPreInsertList() { return preinsertlist; }
	ArrayList<String> getPostInsertList() { return postinsertlist; }
	ArrayList<XML> getCustomSqlList(SyncOper oper) { return customsqloperlist.get(oper); }
	ArrayList<XML> getCustomList(SyncOper oper) { return customoperlist.get(oper); }
	Pattern getOnDupsPattern() { return ondupspattern; }
	OnOper getOnDuplicates() { return onduplicates; }
	String[] getMergeFields() { return mergefields; }
	String[] getMergeKeys() { return mergekeys; }
	String[] getClearFields() { return clearfields; }
	String[] getSuffixFields() { return suffixfields; }
	String[] getDisplayFields() { return displayfields; }
	ExecutorService getPool() { return pool; };
	BlockingQueue<XML> getBlockingQueue() { return queue; };
}

abstract class UpdateSubscriber extends Subscriber
{
	private String keyvalue;
	private Rate rate;
	private int addcounter;
	private int removecounter;
	private int updatecounter;
	private UpdateDestInfo destinfo;
	private Throwable poolexception;

	protected final String getKeyValue() { return keyvalue; }
	protected final UpdateDestInfo getDestInfo() { return destinfo; }

	protected void setKeyValue(XML xml) throws AdapterException
	{
		keyvalue = "";

		LinkedHashMap<String,String> displayvalues = new LinkedHashMap<>();
		String[] displayfields = destinfo.getDisplayFields();
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
	public XML run(XML xml) throws AdapterException
	{
		XML[] xmloperlist = xml.getElements(null);
		for(XML xmloper:xmloperlist)
		{
			if (javaadapter.isShuttingDown()) return null;

			SyncOper oper = Enum.valueOf(SyncOper.class,xmloper.getTagName().toUpperCase());
			if (oper == SyncOper.START)
			{
				if (destinfo != null) throw new AdapterException(xml,"Start operation but last one didn't complete");

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

				destinfo = new UpdateDestInfo(xmldest);
			}

			if (destinfo == null) throw new AdapterException(xml,"Start operation must be done before processing");
			setKeyValue(xmloper);

			ExecutorService pool = destinfo.getPool();
			if (pool == null)
				oper(xmloper);
			else
			{
				BlockingQueue<XML> queue = destinfo.getBlockingQueue();
				try {
					queue.put(xmloper);
				} catch(InterruptedException ex) {
					throw new AdapterException(ex);
				}
				pool.execute(new Runnable() {
					@Override
					public void run()
					{
						try {
							oper(xmloper);
							queue.take();
						} catch(Throwable ex) {
							// try to catch first exception and stop processing as soon as possible
							if (poolexception == null) poolexception = ex;
							Misc.log(ex);
						}
					}
				});
				if (poolexception != null || oper == SyncOper.END)
				{
					pool.shutdown();
					try {
						pool.awaitTermination(24,TimeUnit.HOURS);
					} catch(InterruptedException ex) {
						throw new AdapterException(ex);
					}
					destinfo = null;
					if (poolexception != null)
					{
						if (poolexception instanceof RuntimeException) throw new RuntimeException(poolexception);
						throw new AdapterException(poolexception);
					}
				}
			}
		}

		XML result = new XML();
		result.add("ok");

		return result;
	}

	protected abstract void start(XML xml) throws AdapterException;
	protected abstract void end(XML xml) throws AdapterException;
	protected abstract void add(XML xml) throws AdapterException;
	protected abstract void remove(XML xml) throws AdapterException;
	protected abstract void update(XML xml) throws AdapterException;

	private void startOper(XML xml) throws AdapterException
	{
		addcounter = 0;
		removecounter = 0;
		updatecounter = 0;
		rate = new Rate();
		start(xml);
	}

	private void addOper(XML xml) throws AdapterException
	{
		addcounter++;
		rate.toString();
		add(xml);
	}

	private void removeOper(XML xml) throws AdapterException
	{
		removecounter++;
		rate.toString();
		remove(xml);
	}

	private void updateOper(XML xml) throws AdapterException
	{
		updatecounter++;
		rate.toString();
		update(xml);
	}

	private void endOper(XML xml) throws AdapterException
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
		end(xml);
	}

	protected String getExceptionMessage(Exception ex)
	{
		String msg = ex.getMessage();
		return msg == null ? ex.toString() : msg;
	}

	public final void oper(XML xmloper) throws AdapterException
	{
		try
		{
			SyncOper oper = Enum.valueOf(SyncOper.class,xmloper.getTagName().toUpperCase());
			switch(oper)
			{
			case UPDATE:
				updateOper(xmloper);
				break;
			case ADD:
				addOper(xmloper);
				break;
			case REMOVE:
				removeOper(xmloper);
				break;
			case START:
				startOper(xmloper);
				break;
			case END:
				endOper(xmloper);
				break;
			}
		}
		catch(AdapterException ex)
		{
			if (destinfo.getStopOnError()) Misc.rethrow(ex);
			String key = getKeyValue();
			Misc.log("ERROR: " + (keyvalue == null ? "" : "[" + keyvalue + "] ") + getExceptionMessage(ex) + Misc.CR + "XML message was: " + xmloper);
			if (Misc.isLog(30)) Misc.log(ex);
		}
	}
}

class DatabaseUpdateSubscriber extends UpdateSubscriber
{
	protected DB db;
	private String quotefield = "\"";

	public DatabaseUpdateSubscriber() throws AdapterException
	{
		db = DB.getInstance();
	}

	public void setQuoteField(String field)
	{
		quotefield = field;
	}

	protected void start(XML xml) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		for(String sql:destinfo.getPreInsertList())
		{
			Misc.log(3,"preinsertsql: " + sql);
			db.execsql(instance,sql);
		}
	}

	protected void end(XML xml) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		for(String sql:destinfo.getPostInsertList())
		{
			Misc.log(3,"postinsertsql: " + sql);
			db.execsql(instance,sql);
		}
	}

	private boolean customsql(SyncOper oper,XML xml) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		ArrayList<XML> sqlxmllist = destinfo.getCustomSqlList(oper);
		if (sqlxmllist.size() == 0) return false;

		for(XML sqlxml:sqlxmllist)
		{
			if (!Misc.isFilterPass(sqlxml,xml)) continue;

			String sql = sqlxml.getValue();
			if (sql == null) return false;

			XML[] fields = xml.getElements();
			final ArrayList<DBField> list = new ArrayList<>();

			String sqlresult = Misc.substitute(sql,new Misc.Substituer() {
				public String getValue(String fieldname) throws AdapterException
				{
					XML field = xml.getElement(fieldname);
					if (field != null)
					{
						list.add(new DBField(destinfo.getTableName(),fieldname,field.getValue()));
						return DB.replacement;
					}
					return Misc.substituteGet(fieldname,null,null);
				}
			});

			if (Misc.isLog(10)) Misc.log(oper + ": " + sqlresult);
			db.execsql(instance,sqlresult,list);
		}

		return true;
	}

	private String getWhereClause(XML xml,List<DBField> list) throws AdapterException
	{
		XML[] fields = xml.getElements();
		UpdateDestInfo destinfo = getDestInfo();
		String table = destinfo.getTableName();

		String sep = "where";
		StringBuilder sql = new StringBuilder();

		for(XML field:fields)
		{
			String type = field.getAttribute("type");
			if (type == null || !type.equals("key")) continue;

			String name = field.getTagName();
			String value = field.getValue();
			if (value == null) value = field.getValue("oldvalue",null);

			sql.append(" " + sep + " " + db.getFieldEqualsValue(quotefield,table,name,value,list));
			sep = "and";
		}

		if (!sep.equals("and")) throw new AdapterException(xml,"dbsync: SQL operation without a key");

		String idfield = destinfo.getIdField();
		if (idfield != null)
		{
			String instance = destinfo.getInstance();

			String sqlid = "select " + idfield + " from " + table + sql;
			List<Map<String,String>> ids = db.execsql(instance,sqlid);
			if (ids.size() != 1)
			{
				Misc.log(1,"WARNING: Operation on table " + table + " cannot be done since key doesn't exists: " + sql);
				return " where false";
			}

			String id = Misc.getFirstValue(ids.get(0));
			list.add(new DBField(table,idfield,id));
			return " where " + idfield + " = " + DB.replacement;
		}

		return sql.toString();
	}

	private void handleRetryException(AdapterDbException ex,XML xml,boolean isretry) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String table = destinfo.getTableName();
		String instance = destinfo.getInstance();
		XML[] fields = xml.getElements();

		String message = ex.getMessage();
		if (message == null) message = "";
		if (Misc.isLog(20)) Misc.log("Retry exception: " + message);

		Pattern ondupspattern = destinfo.getOnDupsPattern();

		if ((ondupspattern != null && ondupspattern.matcher(message).find()) || (ondupspattern == null && (message.contains("unique constraint") || message.contains("contrainte unique") || message.contains("ORA-00001:") || message.contains("duplicate key") || message.contains("NoDupIndexTriggered") || message.contains("already exists") || message.contains("existe d\u00e9j\u00e0"))))
		{
			if (Misc.isLog(15))
			{
				Misc.log("Extracting current duplicate since message: " + message);
				ArrayList<DBField> list = new ArrayList<>();
				String sqlchk = "select * from " + table + getWhereClause(xml,list);
				db.execsql(instance,sqlchk,list);
			}

			if (!isretry)
			{
				switch(destinfo.getOnDuplicates())
				{
				case MERGE:
					String[] attrs = destinfo.getMergeFields();
					String[] keys = destinfo.getMergeKeys();

					XML updxml = new XML();
					XML xmlop = updxml.add("update");
					if (keys != null) for(String key:keys)
					{
						XML field = xml.getElement(key);
						if (field == null) throw new AdapterException("Invalid key '" + key + "' in merge_keys attribute: " + xml);
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
					update(updxml,true);
					break;
				case CLEAR:
					String[] clearfields = destinfo.getClearFields();
					if (clearfields == null) throw new AdapterException("clear on_duplicates requires clear_fields attribute");
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Clearing fields " + Misc.implode(clearfields) + ": " + xml);
					StringBuilder sqlclear = new StringBuilder("update " + table + " ");
					String sepclear = "set ";
					ArrayList<DBField> listclear = new ArrayList<>();
					for(String field:clearfields)
					{
						sqlclear.append(sepclear + field + " = " + DB.replacement);
						listclear.add(new DBField(table,field,null));
						sepclear = ",";
					}
					sqlclear.append(" " + getWhereClause(xml,listclear));
					db.execsql(instance,sqlclear.toString(),listclear);
					break;
				case SUFFIX:
					String[] suffixfields = destinfo.getSuffixFields();
					if (suffixfields == null) throw new AdapterException("suffix on_duplicates requires suffix_fields attribute");
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Adding suffix '_' to fields " + Misc.implode(suffixfields) + ": " + xml);
					StringBuilder sqlsuf = new StringBuilder("update " + table + " ");
					String sepsuf = "set ";
					ArrayList<DBField> listsuffix = new ArrayList<>();
					for(String field:suffixfields)
					{
						sqlsuf.append(sepsuf + field + " = " + DB.replacement);
						listsuffix.add(new DBField(table,field,db.getConcat(instance,field,"'_'")));
						sepsuf = ",";
					}
					sqlsuf.append(" " + getWhereClause(xml,listsuffix));
					db.execsql(instance,sqlsuf.toString(),listsuffix);
					break;
				case RECREATE:
					Misc.log(1,"WARNING: [" + getKeyValue() + "] Record already present. Record will be automatically recreated with XML message: " + xml);
					remove(xml);
					add(xml,true);
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

	protected void add(XML xml) throws AdapterException
	{
		add(xml,false);
	}

	protected void add(XML xml,boolean isretry) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		if (customsql(SyncOper.ADD,xml)) return;

		String table = destinfo.getTableName();
		if (table == null) throw new AdapterException("dbsync: destination 'table' attribute required");

		ArrayList<XML> customs = destinfo.getCustomList(SyncOper.ADD);
		if (customs.size() > 0)
			throw new AdapterException("customadd not supported yet for SQL operations");

		StringBuilder sql = new StringBuilder("insert into " + table + " (");
		String sep = "";

		XML[] fields = xml.getElements();
		for(XML field:fields)
		{
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI)) continue;

			sql.append(sep + quotefield + field.getTagName() + quotefield);
			sep = ",";
		}

		sql.append(") values (");

		sep = "";
		ArrayList<DBField> list = new ArrayList<>();
		for(XML field:fields)
		{
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI)) continue;

			sql.append(sep + DB.replacement);
			list.add(new DBField(table,field.getTagName(),field.getValue()));
			sep = ",";
		}
		sql.append(")");

		if (Misc.isLog(10)) Misc.log("add SQL: " + sql);

		try
		{
			db.execsql(instance,sql.toString(),list);
		}
		catch(AdapterDbException ex)
		{
			handleRetryException(ex,xml,isretry);
		}
	}

	protected void remove(XML xml) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		if (customsql(SyncOper.REMOVE,xml)) return;

		String table = destinfo.getTableName();
		if (table == null) throw new AdapterException("dbsync: destination 'table' attribute required");

		StringBuilder sql = new StringBuilder();
		ArrayList<DBField> list = new ArrayList<>();
		ArrayList<XML> customs = destinfo.getCustomList(SyncOper.REMOVE);

		if (customs.size() > 0)
		{
			sql.append("update " + table);
			String sep = "set";
			for(XML custom:customs)
			{
				String name = custom.getAttribute("name");
				if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
				String value = custom.getAttribute("value");
				if (value == null) value = "";
				sql.append(" " + sep + " " + quotefield + name + quotefield + "=" + DB.replacement);
				list.add(new DBField(table,name,Misc.substitute(value,xml)));
				sep = ",";
			}
			sql.append(getWhereClause(xml,list));
		}
		else
			sql.append("delete from " + table + getWhereClause(xml,list));

		if (Misc.isLog(10)) Misc.log("remove SQL: " + sql);
		db.execsql(instance,sql.toString(),list);
	}

	protected void update(XML xml) throws AdapterException
	{
		update(xml,false);
	}

	protected void update(XML xml,boolean isretry) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		String instance = destinfo.getInstance();
		if (customsql(SyncOper.UPDATE,xml)) return;

		String table = destinfo.getTableName();
		if (table == null) throw new AdapterException("dbsync: destination 'table' attribute required");

		StringBuilder sql = new StringBuilder("update " + table);
		String sep = "set";
		XML[] fields = xml.getElements();
		ArrayList<DBField> list = new ArrayList<>();
		for(XML field:fields)
		{
			XML old = field.getElement("oldvalue");
			if (old == null) continue;

			String oldvalue = old.getValue();
			OnOper type = Field.getOnOper(field,"type");
			if (type != null && (type == OnOper.INFO || type == OnOper.INFOAPI || type == OnOper.KEY || (type == OnOper.INITIAL && oldvalue != null))) continue;

			sql.append(" " + sep + " " + quotefield + field.getTagName() + quotefield + " = " + DB.replacement);
			list.add(new DBField(table,field.getTagName(),field.getValue()));
			sep = ",";
		}

		if (sep.equals("set")) return;

		sql.append(getWhereClause(xml,list));

		if (Misc.isLog(10)) Misc.log("update SQL: " + sql);

		try
		{
			db.execsql(instance,sql.toString(),list);
		}
		catch(AdapterDbException ex)
		{
			handleRetryException(ex,xml,isretry);
		}
	}
}

