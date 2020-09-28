import java.util.*;
import java.io.*;

class Sync
{
	private XML xml;
	private XML origxml;
	private DBSyncOper dbsync;
	private DB db;
	private Reader reader;
	private Set<String> keys;
	private CsvWriter csvout;
	private String syncname;
	private boolean dumplogfile = false;
	private boolean isprocessed = false;
	private boolean iserrors = true;
	private Scope scope;

	public Sync(DBSyncOper dbsync,XML xml) throws AdapterException
	{
		this.dbsync = dbsync;
		db = DB.getInstance();

		Fields fields = dbsync.getFields();
		keys = fields == null ? null : fields.getKeys();

		if (xml == null) return;
		this.xml = xml;
		origxml = xml.copy(); // Needed because "fields" is copied during reader initialisation

		String dumplogfilestr = xml.getAttribute("dumplogfile");
		if (dumplogfilestr != null && dumplogfilestr.equals("true"))
			dumplogfile = true;

		String errors = xml.getAttribute("show_errors");

		if (xml.getTagName().equals("source"))
		{
			scope = Scope.SCOPE_SOURCE;
			iserrors = errors == null || !errors.equals("false"); // true is default
		}
		else if (xml.getTagName().equals("destination"))
		{
			scope = Scope.SCOPE_DESTINATION;
			iserrors = errors != null && errors.equals("true"); // false is default
		}
		else
			throw new AdapterException(xml,"Only source and destination tags are supported for sync elements");
	}

	protected void setReader(Reader reader) throws AdapterException
	{
		this.reader = reader;
	}

	public LinkedHashMap<String,String> next() throws AdapterException
	{
		LinkedHashMap<String,String> result = reader == null ? null : reader.next();
		if (result == null && !isprocessed) isprocessed = true;
		return result;
	}

	public XML getXML()
	{
		return xml;
	}

	public Reader getReader()
	{
		return reader;
	}

	protected Set<String> getKeys()
	{
		return keys;
	}

	public boolean isErrors()
	{
		return iserrors;
	}

	public Scope getScope()
	{
		return scope;
	}

	public String getDescription() throws AdapterException
	{
		String syncname = xml.getAttribute("name");
		if (reader == null) return syncname;
		String readername = reader.getName().replace("memsort/","");
		if (syncname == null) return readername;
		if (!readername.equalsIgnoreCase(syncname))
			return syncname + "/" + readername;
		return syncname;
	}

	public String getName() throws AdapterException
	{
		if (syncname != null) return syncname;
		if (xml == null) return "NONE";
		syncname = xml.getAttribute("name");
		if (syncname == null) syncname = xml.getAttribute("instance");
		if (syncname == null) syncname = xml.getAttribute("filename");
		if (syncname == null) syncname = xml.getTagName();
		return syncname;
	}

	public Set<String> getResultHeader() throws AdapterException
	{
		return getDBSync().getFields().getNames(this);
	}

	public void setPostInitReader() throws AdapterException
	{
		// This must be called after all field elements are initialized since sort/cache will use them
		if (this instanceof SyncSql)
		{
			// This one is special since no sort simply means sort is already part of the SQL statement
			// However it supports caching
			String iscache = xml.getAttribute("cached");
			if (iscache != null && iscache.equals("true"))
			reader = new CacheReader(xml,reader);
		}
		else if (!reader.isSorted()) reader = new SortTable(xml,this);

		String dumpcsvfilename = xml.getAttribute("dumpcsvfilename");
		if (dumpcsvfilename == null && dbsyncplugin.dumpcsv_mode)
			dumpcsvfilename = javaadapter.getName() + "_" + dbsync.getName() + "_" + getName() + "_" + Misc.implode(keys,"_") + ".csv";

		if (dumpcsvfilename != null)
		{
			String fieldsattr = xml.getAttribute("csv_fields");
			if (fieldsattr == null) fieldsattr = xml.getAttribute("fields");
			Set<String> headers;
			if (fieldsattr == null)
			{
				// Prioritize fields from reader
				headers = new LinkedHashSet<String>(reader.getHeader());
				headers.addAll(getResultHeader());
			}
			else headers = Misc.arrayToSet(fieldsattr.split("\\s*,\\s*"));
			csvout = new CsvWriter(dumpcsvfilename,headers,xml);
		}
	}

	public void makeDump(LinkedHashMap<String,String> result) throws AdapterException
	{
		if (csvout != null) csvout.write(result);
		if (dumplogfile) Misc.log(Misc.implode(result));
	}

	public void closeDump() throws AdapterException
	{
		if (csvout != null) csvout.flush();
	}

	public boolean isProcessed()
	{
		return isprocessed;
	}

	public DBSyncOper getDBSync()
	{
		return dbsync;
	}
}

class SyncClass extends Sync
{
	public SyncClass(DBSyncOper dbsync,XML xml) throws AdapterException
	{
		super(dbsync,xml);
		setReader(ReaderUtil.getReader(xml));
	}
}

class SyncCsv extends Sync
{
	public SyncCsv(DBSyncOper dbsync,XML xml) throws AdapterException
	{
		super(dbsync,xml);
		setReader(new ReaderCSV(xml));
	}
}

class SyncSql extends Sync
{
	private String conn;
	private DB db;

	public SyncSql(DBSyncOper dbsync,XML xml) throws AdapterException
	{
		super(dbsync,xml);

		db = DB.getInstance();
		conn = xml.getAttribute("instance");
		if (conn == null)
			throw new AdapterException(xml,"Attribute 'instance' required for database syncer");

		XML sqlxml = xml.getElement("extractsql");
		String sql = sqlxml == null ? xml.getValue() : sqlxml.getValue();
		if (sql == null) sql = "";

		String restrictsql = xml.getValue("restrictsql",null);
		if (restrictsql != null)
		{
			if (restrictsql.indexOf("%SQL%") != -1)
				sql = restrictsql.replace("%SQL%",sql);
			else
				sql = "select * from (" + sql + ") d3 where " + restrictsql;
		}

		Set<String> keys = getKeys();
		ArrayList<DBField> list = new ArrayList<DBField>();

		// Small unsorted source to large sql destination optimization
		if (getScope() == Scope.SCOPE_DESTINATION && dbsync.getDoRemove() == DBSyncOper.doTypes.FALSE && dbsync.getSourceSync().getReader() instanceof SortTable)
		{
			TreeMap<String,LinkedHashMap<String,Set<String>>> map = ((SortTable)dbsync.getSourceSync().getReader()).getSortedMap();
			if (map != null && map.size() <= (50 / keys.size()))
			{
				StringBuilder sb = new StringBuilder();
				String sepor = "";
				for(LinkedHashMap<String,Set<String>> entry:map.values())
				{
					String sepand = "";
					sb.append(sepor + "(");
					for(String keyname:keys)
					{
						String value = Misc.implode(entry.get(keyname),"\n");
						if (value.isEmpty())
							sb.append(sepand + "\"" + keyname + "\" is null");
						else
						{
							sb.append(sepand + "\"" + keyname + "\"=" + DB.replacement);
							list.add(new DBField(keyname,value));
						}
						sepand = " and ";
					}
					sb.append(")");
					sepor = " or ";
				}

				sql = "select * from (" + sql + ") d4 where " + (sepor.isEmpty() ? "1 = 0" : sb);
			}
		}

		sql = sql.replace("%LASTDATE%",dbsync.getLastDate() == null ? "" : Misc.gmtdateformat.format(dbsync.getLastDate()));
		sql = sql.replace("%STARTDATE%",dbsync.getStartDate() == null ? "" : Misc.gmtdateformat.format(dbsync.getStartDate()));
		sql = sql.replace("%NAME%",dbsync.getName());

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
			sql += db.getOrderBy(conn,keys.toArray(new String[keys.size()]),dbsync.getIgnoreCaseKeys());
		}

		String presql = xml.getValue("preextractsql",null);
		if (presql != null)
			db.execsql(conn,presql);

		// Overwrite default reader
		String trim = xml.getAttribute("trim");
		boolean totrim = !(trim != null && trim.equals("false"));
		setReader(new ReaderSQL.Builder(conn,sql).setDBFields(list).setKeys(keys).setTrim(totrim).setSorted(issorted).build());
	}
}

class SyncSoap extends Sync
{
	private XML xml;

	public SyncSoap(DBSyncOper dbsync,XML xml) throws AdapterException
	{
		super(dbsync,xml);

		XML request = xml.getElement("element");
		XML function = xml.getElement("function");
		if (request == null || function == null)
			throw new AdapterException(xml,"Invalid sync call");

		Subscriber sub = new Subscriber(function);
		XML result = sub.run(request.getElement(null).copy());
		setReader(new ReaderXML(xml,result));
	}
}

class SyncXML extends Sync
{
	private XML xml;

	public SyncXML(DBSyncOper dbsync,XML xml,XML xmlsource) throws AdapterException
	{
		super(dbsync,xml);

		setReader(new ReaderXML(xml,xmlsource));
	}
}
