import java.util.*;
import java.io.*;

class Sync
{
	private XML xml;
	private DBSyncOper dbsync;
	private DB db;
	protected Reader reader;
	protected Set<String> keys;
	private CsvWriter csvout;
	private String syncname;
	private boolean dumplogfile = false;
	private boolean isprocessed = false;
	private Scope scope;

	public Sync(DBSyncOper dbsync,XML xml) throws Exception
	{
		this.dbsync = dbsync;
		db = DB.getInstance();

		Fields fields = dbsync.getFields();
		keys = fields == null ? null : fields.getKeys();

		if (xml == null) return;
		this.xml = xml;

		String fieldsattr = xml.getAttribute("fields");
		Set<String> headers = fieldsattr == null ? null : Misc.arrayToSet(fieldsattr.split("\\s*,\\s*"));
 
		String dumpcsvfilename = xml.getAttribute("dumpcsvfilename");
		if (dumpcsvfilename == null && dbsyncplugin.dumpcsv_mode)
			dumpcsvfilename = javaadapter.getName() + "_" + dbsync.getName() + "_" + getName() + "_" + Misc.implode(keys,"_") + ".csv";

		if (dumpcsvfilename != null) csvout = new CsvWriter(dumpcsvfilename,headers,xml);

		String dumplogfilestr = xml.getAttribute("dumplogfile");
		if (dumplogfilestr != null && dumplogfilestr.equals("true"))
			dumplogfile = true;

		if (xml.getTagName().equals("source"))
			scope = Scope.SCOPE_SOURCE;
		else if (xml.getTagName().equals("destination"))
			scope = Scope.SCOPE_DESTINATION;
		else
			throw new AdapterException(xml,"Only source and destination tags are supported for sync elements");

		// Important: reader must be set by extended constructor class
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> result = reader == null ? null : reader.next();
		if (result == null && !isprocessed) isprocessed = true;
		return result;
	}

	public Set<String> getHeader() throws Exception
	{
		if (reader == null) return null;
		Set<String> keyset = new LinkedHashSet<String>(reader.getHeader());
		keyset.addAll(dbsync.getFields().getNames(this));
		return keyset;
	}

	public XML getXML()
	{
		return xml;
	}

	public Reader getReader()
	{
		return reader;
	}

	public Scope getScope()
	{
		return scope;
	}

	public String getDescription() throws Exception
	{
		String syncname = xml.getAttribute("name");
		if (reader == null) return syncname;
		String readername = reader.getName().replace("memsort/","");
		if (syncname == null) return readername;
		if (!readername.equalsIgnoreCase(syncname))
			return syncname + "/" + readername;
		return syncname;
	}

	public String getName() throws Exception
	{
		if (syncname != null) return syncname;
		if (xml == null) return "NONE";
		syncname = xml.getAttribute("name");
		if (syncname == null) syncname = xml.getAttribute("instance");
		if (syncname == null) syncname = xml.getAttribute("filename");
		if (syncname == null) syncname = xml.getTagName();
		return syncname;
	}

	public void makeDump(LinkedHashMap<String,String> result) throws Exception
	{
		if (csvout != null) csvout.write(result);
		if (dumplogfile) Misc.log(Misc.implode(result));
	}

	public void closeDump() throws Exception
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
	public SyncClass(DBSyncOper dbsync,XML xml) throws Exception
	{
		super(dbsync,xml);

		reader = ReaderUtil.getReader(xml);

		String sorted = xml.getAttribute("sorted");
		if (sorted != null && sorted.equals("true")) return;

		reader = new SortTable(xml,this);
	}
}

class SyncCsv extends Sync
{
	public SyncCsv(DBSyncOper dbsync,XML xml) throws Exception
	{
		super(dbsync,xml);

		reader = new ReaderCSV(xml);

		String sorted = xml.getAttribute("sorted");
		if (sorted != null && sorted.equals("true")) return;

		reader = new SortTable(xml,this);
	}
}

class SyncSql extends Sync
{
	private String conn;
	private DB db;

	public SyncSql(DBSyncOper dbsync,XML xml) throws Exception
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
						sb.append(sepand + "\"" + keyname + "\"=" + db.getFieldValue(Misc.implode(entry.get(keyname),"\n")));
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
		reader = new ReaderSQL(conn,sql,keys,issorted);

		String iscache = xml.getAttribute("cached");
		if (iscache != null && iscache.equals("true"))
			reader = new CacheReader(xml,reader);
	}
}

class SyncSoap extends Sync
{
	private XML xml;

	public SyncSoap(DBSyncOper dbsync,XML xml) throws Exception
	{
		super(dbsync,xml);

		XML request = xml.getElement("element");
		XML function = xml.getElement("function");
		if (request == null || function == null)
			throw new AdapterException(xml,"Invalid sync call");

		Subscriber sub = new Subscriber(function);
		XML result = sub.run(request.getElement(null).copy());
		reader = new ReaderXML(xml,result);

		reader = new SortTable(xml,this);
	}
}

class SyncXML extends Sync
{
	private XML xml;

	public SyncXML(DBSyncOper dbsync,XML xml,XML xmlsource) throws Exception
	{
		super(dbsync,xml);

		reader = new ReaderXML(xml,xmlsource);

		String sorted = xml.getAttribute("sorted");
		if (sorted != null && sorted.equals("true")) return;

		reader = new SortTable(xml,this);
	}
}
