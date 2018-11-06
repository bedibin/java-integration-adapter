import java.util.*;
import java.sql.*;
import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.text.Collator;

enum dbtype { MYSQL, MSSQL, ORACLE, DB2, OTHER };

interface DBProcessor
{
	public String getFieldValue(byte[] bytes) throws Exception;
}

class DBProcessorManager
{
	static HashMap<String,DBProcessor> processorList = new HashMap<String,DBProcessor>();
	static synchronized public void register(DBProcessor processor)
	{
		processorList.put(processor.getClass().getName(),processor);
	}
	static synchronized public DBProcessor get(String name)
	{
		return processorList.get(name);
	}
}

class ConnectionTimeout extends Thread
{
	private Connection conn;
	private boolean sleep = true;
	private SQLException exception;
	private String username;
	private String password;
	private String url;

	public ConnectionTimeout(String url,String username,String password)
	{
		this.url = url;
		this.username = username;
		this.password = password;
	}

	@Override
	public void run()
	{
		try {
			if (username == null)
				conn = DriverManager.getConnection(url);
			else
				conn = DriverManager.getConnection(url,username,password);
			sleep = false;
		} catch (SQLException e) {
			exception = e;
		}
	}

	static public Connection getConnection(String name,String url,String username,String password) throws Exception
	{
		ConnectionTimeout ct = new ConnectionTimeout(url,username,password);
		ct.start();
		try {
			for(int i=1;i<=60;i++)
				if (ct.sleep)
					 Thread.sleep(1000);  
		} catch (InterruptedException e) {}

		if (ct.exception != null) Misc.rethrow(ct.exception,"Exception connecting to database " + name);
		if (ct.conn == null) throw new AdapterException("Timeout connecting to database " + name);
		return ct.conn ;
	}
}

class DBConnection implements VariableContext
{
	private Connection conn;
	private String quote = "\"";
	private dbtype dbtype;
	private Calendar calendar;
	private TimeZone timezone;
	private XML xml;
	private String name;
	private Set<String> processors = new TreeSet<String>();

	public static final String ORACLEJDBCDRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String SQLSERVERJDBCDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String MYSQLJDBCDRIVER = "com.mysql.jdbc.Driver";
	private static final String DB2JDBCDRIVER = "com.ibm.db2.jcc.DB2Driver";
	private static final String SYBASEJDBCDRIVER = "com.sybase.jdbc4.jdbc.SybDriver";
	private static final String SQLDATEFORMAT = "'YYYY-MM-DD HH24:MI:SS'";

	public DBConnection(XML xml) throws Exception
	{
		init(xml);
		this.xml = xml;
	}

	public Set<String> getProcessors()
	{
		return processors;
	}

	private void execsql(String sql) throws SQLException
	{
		PreparedStatement stmt = conn.prepareStatement(sql);
		if (Misc.isLog(15)) Misc.log("Executing initialization statement: " + sql);
		stmt.executeUpdate();
		stmt.close();
	}

	private void init(XML xml) throws Exception
	{
		name = xml.getAttribute("name");
		String urlstr = xml.getValue("url",null);
		String driverstr = xml.getValue("driver",null);
		String connstr = (urlstr == null) ? "jdbc:oracle:thin:@" + xml.getValue("server") + ":" + xml.getValue("port","1521") + ":" + xml.getValue("instance") : urlstr;

		try
		{
			DriverManager.setLoginTimeout(30);
			String username = xml.getValue("username",null);
			String password = username == null ? null : xml.getValueCrypt("password");
			conn = ConnectionTimeout.getConnection(name,connstr,username,password);
		}
		catch(SQLException ex)
		{
			Misc.rethrow(ex,"ERROR: Connecting to database " + name);
		}

		dbtype = dbtype.OTHER;

		calendar = Calendar.getInstance();
		String tz = xml.getValue("timezone","UTC");
		if (!tz.equals("local"))
		{
			timezone = TimeZone.getTimeZone(tz);
			if (Misc.isLog(5)) Misc.log("DB " + name + " timezone is " + timezone.getDisplayName());
			calendar.setTimeZone(timezone);
		}

		XML[] xmlprolist = xml.getElements("processor");
		for(XML xmlpro:xmlprolist)
			processors.add(xmlpro.getValue());

		if (urlstr == null || (driverstr != null && driverstr.equals(ORACLEJDBCDRIVER)))
		{
			execsql("alter session set NLS_DATE_FORMAT = " + SQLDATEFORMAT);
			execsql("alter session set NLS_TIMESTAMP_FORMAT = " + SQLDATEFORMAT);
			execsql("alter session set NLS_SORT = unicode_binary");
			dbtype = dbtype.ORACLE;
		}
		else if (driverstr != null && driverstr.equals(SQLSERVERJDBCDRIVER))
		{
			// TODO: Make setTransactionIsolation configurable, UNCOMMITED should not be default
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			dbtype = dbtype.MSSQL;
		}
		else if (driverstr != null && driverstr.equals(MYSQLJDBCDRIVER))
		{
			execsql("set names utf8 collate utf8_bin");
			execsql("set session sql_mode=''");
			quote = "`";
			dbtype = dbtype.MYSQL;
		}
		else if (driverstr != null && driverstr.equals(DB2JDBCDRIVER))
		{
			dbtype = dbtype.DB2;
		}

		XML[] xmlinit = xml.getElements("initsql");
		for(XML el:xmlinit)
			execsql(el.getValue());
	}

	public void checkConnectionState(boolean force) throws Exception
	{
		if (!force && !conn.isClosed()) return;

		Misc.log(1,"Database connection " + name + " closed, trying reconnect");

		try
		{
			try
			{
				Misc.invoke(conn,"abort");
				Misc.log(3,"Connection aborted");
			}
			catch(Throwable th)
			{
			}
			conn.close();
			Misc.log(3,"Connection closed");
		}
		catch(Exception ex)
		{
		}

		init(xml);
	}

	public void close() throws SQLException
	{
		conn.close();
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return conn.prepareStatement(sql);
	}

	public TimeZone getTimeZone()
	{
		return timezone;
	}

	public Calendar getCalendar()
	{
		return calendar;
	}

	public dbtype getType()
	{
		return dbtype;
	}

	public String getQuote()
	{
		return quote;
	}

	public String getName()
	{
		return name;
	}
}

class DBComparator implements Comparator<String>
{
	@Override
	public int compare(String a,String b)
	{
		return a.compareTo(b);
	}
}

class DBComparatorIgnoreCase implements Comparator<String>
{
	@Override
	public int compare(String a,String b)
	{
		return a.compareToIgnoreCase(b);
	}
}

class DBOper
{
	private PreparedStatement stmt;
	private ResultSet rset;
	private int columncount = 0;
	protected String[] columnnames;
	private int[] columntypes;
	private DBConnection dbc;
	private boolean totrim = true;
	protected int resultcount = 0;
	public static final Pattern replacementPattern = Pattern.compile("\\*@\\?@\\!");

	protected DBOper() { }

	private void makeStatement(String sql,List<String> list) throws Exception
	{
		sql = sql.trim();
		if (list != null) sql = replacementPattern.matcher(sql).replaceAll("?");

		String liststr = list == null ? "" : "; " + Misc.implode(list);
		if (Misc.isLog(5)) Misc.log("SQL query [" + dbc.getName() + "]: " + sql + liststr);

		stmt = dbc.prepareStatement(sql);

		if (stmt.getQueryTimeout() <= 1) stmt.setQueryTimeout(3600);

		if (list != null)
		{
			int x = 1;
			for(String value:list)
			{
				if (value == null)
					stmt.setString(x,null);
				else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
				{
					java.util.Date date = Misc.gmtdateformat.parse(value);
					stmt.setTimestamp(x,new Timestamp(date.getTime()),dbc.getCalendar());
				}
				else
					stmt.setString(x,value);
				x++;
			}
		}

		if (stmt.execute())
		{
			rset = stmt.getResultSet();
			ResultSetMetaData rsmd = rset.getMetaData();

			columncount = rsmd.getColumnCount();
			columnnames = new String[columncount];
			columntypes = new int[columncount];

			for(int i = 1;i <= columncount;i++)
			{
				columnnames[i-1] = rsmd.getColumnName(i);
				columntypes[i-1] = rsmd.getColumnType(i);
			}
		}
		else
		{
			resultcount = stmt.getUpdateCount();
			if (Misc.isLog(15)) Misc.log("" + resultcount + " row updated");
			close();
		}
	}

	public DBOper(DBConnection dbc,String sql,boolean totrim) throws Exception
	{
		this(dbc,sql,null,totrim);
	}

	public DBOper(DBConnection dbc,String sql,List<String> list,boolean totrim) throws Exception
	{
		if (javaadapter.isShuttingDown()) return;
		dbc.checkConnectionState(false);
		this.dbc = dbc;
		this.totrim = totrim;

		try
		{
			makeStatement(sql,list);
		}
		catch(SQLException ex)
		{
			int code = ex.getErrorCode();
			String message = ex.getMessage().toLowerCase();
			final String errors[] =  {"packet","shutdown","gone","closed","socket"," state","pipe","timeout","timed out","connection","connexion"};
			for(int i = 0;i < errors.length;i++)
			{
				if (message.indexOf(errors[i]) != -1)
				{
					Misc.log(1,"Database connection " + dbc.getName() + " closed, statement retried. Error: " + ex);
					// Do not close statement since it may hang
					// close();

					try
					{
						dbc.checkConnectionState(true);
						makeStatement(sql,list);
					}
					catch(SQLException ex2)
					{
						Misc.log(1,"2nd SQL error " + ex2.getErrorCode() + " [" + dbc.getName() + "]: " + sql);
						close();
						Misc.rethrow(ex2);
					}
					catch(Exception ex2)
					{
						Misc.log(1,"2nd SQL error [" + dbc.getName() + "]: " + sql);
						close();
						Misc.rethrow(ex2);
					}

					return;
				}
			}

			String liststr = list == null ? "" : "; " + Misc.implode(list);
			Misc.log(1,"SQL error " + code + " [" + dbc.getName() + "]: " + sql + liststr);
			close();
			Misc.rethrow(ex);
		}
		catch(Exception ex)
		{
			String liststr = list == null ? "" : "; " + Misc.implode(list);
			Misc.log(1,"SQL error [" + dbc.getName() + "]: " + sql + liststr);
			close();
			Misc.rethrow(ex);
		}
	}

	public Set<String> getHeader() throws Exception
	{
		if (rset == null) return null;

		Set<String> headers = new LinkedHashSet<String>();

		for(String name:columnnames)
			headers.add(name);

		return headers;
	}

	public int getResultCount()
	{
		return resultcount;
	}

	static public boolean isCompressed(byte[] bytes)
	{
		if ((bytes == null) || (bytes.length < 2)) return false;
		return ((bytes[0] == (byte)(GZIPInputStream.GZIP_MAGIC)) && (bytes[1] == (byte)(GZIPInputStream.GZIP_MAGIC >> 8)));
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();

		if (stmt == null) return null;
		if (javaadapter.isShuttingDown() || rset == null || !rset.next())
		{
			if (stmt.getMoreResults()) throw new AdapterException("Multiple result sets are not supported since they might have different columns. Join multiple selects with UNION instead");
			close();
			return null;
		}

		for(int i = 0;i < columnnames.length;i++)
		{
			if (rset.getObject(i+1) == null)
			{
				row.put(columnnames[i],"");
				continue;
			}

			String value = null;
			java.util.Date date = null;

			switch(columntypes[i])
			{
			case Types.TIME:
				date = rset.getTime(i+1,dbc.getCalendar());
				break;
			case Types.DATE:
				date = rset.getDate(i+1,dbc.getCalendar());
				break;
			case Types.TIMESTAMP:
				date = rset.getTimestamp(i+1,dbc.getCalendar());
				break;
			case Types.BLOB:
			case Types.LONGVARBINARY:
				Blob blob = rset.getBlob(i+1);
				byte[] bytes = blob.getBytes(1,(int)blob.length());
				if (isCompressed(bytes))
				{
					ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
					GZIPInputStream gzis = new GZIPInputStream(bis);
					ByteArrayOutputStream out = new ByteArrayOutputStream();

					int len;
					byte[] buffer = new byte[1024];
					while ((len = gzis.read(buffer)) > 0)
						out.write(buffer,0,len);

					gzis.close();
					out.close();

					bytes = out.toByteArray();
				}

				for(String name:dbc.getProcessors())
				{
					DBProcessor processor = DBProcessorManager.get(name);
					value = processor.getFieldValue(bytes);
					if (value != null) break;
				}
				if (value == null) value = new String(bytes);
				break;
			default:
				value = rset.getString(i+1);
			}

			if (date != null) value = Misc.gmtdateformat.format(date);
			if (value == null) value = "";

			row.put(columnnames[i],totrim ? value.trim() : value);
		}

		if (Misc.isLog(15)) Misc.log("row [" + dbc.getName() + "]: " + row);

		return row;
	}

	public void close() throws Exception
	{
		if (rset != null) rset.close();
		if (stmt != null) stmt.close();
		stmt = null;
	}
}

class DB
{
	public static final String replacement = "*@?@!";
	private final Comparator<String> collator;
	private final Comparator<String> collator_ignore_case;

	private static DB instance;

	private HashMap<String,DBConnection> db;
	private XML[] xmlconn;

	protected DB()
	{
		collator = new DBComparator();
		collator_ignore_case = new DBComparatorIgnoreCase();
		db = new HashMap<String,DBConnection>();
	}

	public DB(XML xmlcfg) throws Exception
	{
		this();

		System.out.print("Connection to database... ");

		xmlconn = xmlcfg.getElements("connection");

		boolean withdriver = false;
		boolean withbd = false;
		for(XML el:xmlconn)
		{
			String type = el.getAttribute("type");
			if (type != null && !type.equals("db")) continue;

			withbd = true;
			String driver = el.getValue("driver",null);
			if (driver != null)
			{
				withdriver = true;
				Class.forName(driver);
			}

			XML[] xmlprolist = el.getElements("processor");
			for(XML xmlpro:xmlprolist)
				Class.forName(xmlpro.getValue());
		}

		if (withbd && !withdriver) Class.forName(DBConnection.ORACLEJDBCDRIVER);

		for(XML el:xmlconn)
		{
			String type = el.getAttribute("type");
			if (type != null && !type.equals("db")) continue;
			String name = el.getAttribute("name");
			db.put(name,new DBConnection(el));
		}

		System.out.println("Done");
	}

	public Comparator<String> getCollator()
	{
		return collator;
	}

	public Comparator<String> getCollatorIgnoreCase()
	{
		return collator_ignore_case;
	}

	public Set<String> getConnectionInstances()
	{
		return db.keySet();
	}

	public synchronized static DB getInstance() throws Exception
	{
		if (instance == null)
		{
			instance = new DB(javaadapter.getConfiguration());
			javaadapter.setForShutdown(instance);
		}
		return instance;
	}

	protected String getDate(String value) throws Exception
	{
		return "{ ts '" + value + "'}";
	}

	public String getFieldValue(String value) throws Exception
	{
		return getValue(value);
	}

	public String getValue(String value,String name) throws Exception
	{
		return getValue(value);
	}

	public String getValue(String value) throws Exception
	{
		if (value == null) return "null";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
			return getDate(value);
		value = value.replace("'","''");
		return "'" + value + "'";
	}

	public String getValue(XML xml) throws Exception
        {
		if (xml == null) return null;

		String value = xml.getValue();
		return getValue(value);
	}

	public String getConcat(String conn,String field,String addedfield) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);

		switch(dbc.getType())
		{
			case MYSQL:
				return "concat(" + field + "," + addedfield + ")";
			case MSSQL:
				return field + " + " + addedfield;
		}
		return field + " || " + addedfield;
	}

	public String getFieldEqualsValue(String field,String value) throws Exception
	{
		if (value == null) return field + " is null";

		String[] valuesplit = value.split("\n");
		if (valuesplit.length == 1)
			return field + " = " + getValue(value,field);

		StringBuilder sql = new StringBuilder(field + " in (");
		String sep = "";
		for(int i = 0;i < valuesplit.length;i++)
		{
			sql.append(sep + getValue(valuesplit[i],field));
			sep = ",";
		}

		sql.append(")");
		return sql.toString();
	}

	public DBConnection getConnectionByName(String name) throws Exception
	{
		DBConnection dbc = db.get(name);
		if (dbc == null)
			throw new AdapterException("Connection " + name + " doesn't exist");
		return dbc;
	}

	public String getOrderBy(String conn,String[] keys,boolean ignore_case) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);

		StringBuilder sql = new StringBuilder("(");
		boolean first = true;

		switch(dbc.getType())
		{
		case MYSQL:
			sql.append("concat(");
			break;
		case DB2:
			sql.append("collation_key_bit(");
			break;
		}

		for(String keyfield:keys)
		{
			keyfield = dbc.getQuote() + keyfield + dbc.getQuote();
			if (ignore_case) keyfield = "upper(" + keyfield + ")";
			keyfield = "replace(replace(rtrim(ltrim(replace(coalesce(" + keyfield + ",''),'\t',' '))),' ','!'),'_','!')";
			switch(dbc.getType())
			{
			case MYSQL:
				if (!first) sql.append(",");
				sql.append(keyfield + ",'!'");
				break;
			case MSSQL:
				if (!first) sql.append(" + ");
				sql.append(keyfield + " + '!'");
				break;
			default:
				// Oracle, DB2, Sybase
				if (!first) sql.append(" || ");
				sql.append(keyfield + " || '!'");
			}
			first = false;
		}

		switch(dbc.getType())
		{
		case MYSQL:
			sql.append(")");
			break;
		case DB2:
			sql.append(",'UCA400R1')");
			break;
		}
		sql.append(")");
		switch(dbc.getType())
		{
		case MSSQL:
			sql.append(" collate latin1_general_bin2");
			break;
		case MYSQL:
			break;
		}

		return " order by " + sql;
	}

	public DBOper makesqloper(String conn,String sql) throws Exception
	{
		return makesqloper(conn,sql,true);
	}

	public DBOper makesqloper(String conn,String sql,boolean totrim) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);
		return new DBOper(dbc,sql,totrim);
	}

	public DBOper makesqloper(String conn,XML xml) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);
		String sql = xml.getValue();
		return new DBOper(dbc,sql,true);
	}

	public int execsqlresult(String conn,String sql,List<String> list) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);
		DBOper oper = new DBOper(dbc,sql,list,true);
		return oper.getResultCount();
	}

	public int execsqlresult(String conn,String sql) throws Exception
	{
		return execsqlresult(conn,sql,null);
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(String conn,String sql) throws Exception
	{
		return execsql(conn,sql,null);
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(String conn,String sql,List<String> list) throws Exception
	{
		DBConnection dbc = getConnectionByName(conn);
		DBOper oper = null;
		ArrayList<LinkedHashMap<String,String>> result = null;

		oper = new DBOper(dbc,sql,list,true);
		result = new ArrayList<LinkedHashMap<String,String>>();
		LinkedHashMap<String,String> row;

		while((row = oper.next()) != null)
			result.add(row);

		if (Misc.isLog(result.size() > 0 ? 9 : 10)) Misc.log("SQL result [" + conn + "]: " + result);

		return result;
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(XML xml) throws Exception
	{
		String conn = xml.getAttribute("instance");
		return execsql(conn,xml);
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(String conn,XML xml) throws Exception
	{
		String sql = xml.getValue();
		return execsql(conn,sql);
	}

	public String getQuote(String instance) throws Exception
	{
		DBConnection dbc = getConnectionByName(instance);
		return dbc.getQuote();
	}

	public void close()
	{
		try
		{
			for(DBConnection dbc:db.values())
				dbc.close();
		}
		catch(Exception ex)
		{
		}
	}

	public String substitute(final String conn,String str,final LinkedHashMap<String,String> row) throws Exception
	{
		return Misc.substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				DBConnection dbc = getConnectionByName(conn);
				if (param.startsWith("$")) return Misc.substituteGet(param,null,dbc);
				String value = Misc.substituteGet(param,row == null ? null : row.get(param),dbc);
				return getFieldValue(value);
			}
		});
	}

	public void lookup(XML xmlconfig,XML xml) throws Exception
	{
		final String conn = xmlconfig.getAttribute("instance");
		XML xmlsql = xmlconfig.getElement("sql");
		String sql = xmlsql.getValue();

		final XML finalxml = xml;
		DBOper oper = makesqloper(conn,Misc.substitute(sql,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				DBConnection dbc = getConnectionByName(conn);
				if (param.startsWith("$")) return Misc.substituteGet(param,null,dbc);
				String value = Misc.substituteGet(param,finalxml.getStringByPath(param),dbc);
				return getFieldValue(value);
			}
		}));

		LinkedHashMap<String,String> row;
		XML xmltable = xml.add(conn);

		while((row = oper.next()) != null)
		{
			XML xmlrow = xmltable.add("row");

			for(String key:oper.getHeader())
			{
				String value = row.get(key);
				xmlrow.add(key,value);
			}
		}
	}

	public dbtype getType(String instance) throws Exception
	{
		DBConnection dbc = getConnectionByName(instance);
		if (dbc == null) return null;
		return dbc.getType();
	}
}

public class dbsql
{
	private static DB db;

	private static void Export(String instance,String table) throws Exception
	{
		DBOper oper = db.makesqloper(instance,"select * from " + table);
		CsvWriter csvout = new CsvWriter("select_" + table + ".csv");

		LinkedHashMap<String,String> row;

		while((row = oper.next()) != null)
			csvout.write(row);

		csvout.flush();
	}

	private static void Import(String instance,String table) throws Exception
	{
		ReaderCSV csvin = new ReaderCSV("select_" + table + ".csv");

		LinkedHashMap<String,String> row;

		while((row = csvin.next()) != null)
		{
			Set<String> headers = csvin.getHeader();
			String sql = "insert into " + table + " (";
			String sep = "";
			for(String header:headers)
			{
				sql += sep + header;
				sep = ",";
			}
			sql += ") values (";
			sep = "";
			ArrayList<String> list = new ArrayList<String>();
			for(String header:headers)
			{
				String value = row.get(header);
				if (value.length() > 3000) value = value.substring(0,3000);
				sql += sep + DB.replacement;
				list.add(value);
				sep = ",";
			}
			sql += ")";
			try
			{
				db.execsql(instance,sql,list);
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		javaadapter.initShutdownHook();
		javaadapter.dohooks = false;
		String filename = javaadapter.DEFAULTCFGFILENAME;
		if (args.length > 0) filename = args[0];
		javaadapter.init(filename);
		db = DB.getInstance();
		XML xmlcfg = javaadapter.getConfiguration();
		XML[] conns = xmlcfg.getElements("connection");
		for(XML conn:conns)
		{
			XML extractxml = conn.getElement("extract");
			if (extractxml == null) continue;

			String extract = extractxml.getValue();
			if (extract == null) continue;

			String type = extractxml.getAttribute("type");
			boolean isimport = "import".equals(type);

			String name = conn.getAttribute("name");
			String oper = isimport ? "Exporting" : "Importing";
			System.out.println(oper + " " + name + "...");

			ArrayList<LinkedHashMap<String,String>> tables = db.execsql(name,extract);

			for(LinkedHashMap<String,String> table:tables)
			{
				String tablename = Misc.getFirstValue(table);
				System.out.print("    " + tablename + "... ");

				try
				{
					if (isimport)
						Import(name,tablename);
					else
						Export(name,tablename);
					System.out.println("done");
				}
				catch(Exception ex)
				{
					System.out.println("error");
					//System.out.println("error: " + ex.getMessage());
				}
			}
		}
	}
}
