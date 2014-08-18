import java.util.*;
import java.sql.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.text.Collator;

interface ComparatorIgnoreCase<T> extends Comparator<T>
{
	public int compareIgnoreCase(T a,T b);
}

class DB
{
	class DBComparator implements ComparatorIgnoreCase<String>
	{
		@Override
		public int compare(String a,String b)
		{
			return a.compareTo(b);
		}

		public int compareIgnoreCase(String a,String b)
		{
			return a.compareToIgnoreCase(b);
		}
	}

	class DBOper
	{
		private PreparedStatement stmt;
		private ResultSet rset;
		private String name;
		private boolean isselect = false;
		private int columncount = 0;
		protected String[] columnnames;
		private int[] columntypes;
		private Connection conn;
		private Calendar calendar;
		private int resultcount = 0;

		protected DBOper() { }

		private void makeStatement(String sql,List<String> list) throws Exception
		{
			sql = sql.trim();
			if (list != null) sql = replacementPattern.matcher(sql).replaceAll("?");

			if (sql.startsWith("select") || sql.startsWith("SELECT"))
				isselect = true;
			if (Misc.isLog(5)) Misc.log("SQL " + (isselect ? "SELECT " : "") + "query [" + name + "]: " + sql);

			stmt = conn.prepareStatement(sql);

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
						stmt.setTimestamp(x,new Timestamp(date.getTime()),calendar);
					}
					else
						stmt.setString(x,value);
					x++;
				}
			}

			if (isselect)
			{
				rset = stmt.executeQuery();
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
				resultcount = stmt.executeUpdate();
				if (Misc.isLog(15)) Misc.log("" + resultcount + " row updated");
				close();
			}
		}

		public DBOper(String name,String sql) throws Exception
		{
			this(name,sql,null);
		}

		public DBOper(String name,String sql,List<String> list) throws Exception
		{
			if (javaadapter.isShuttingDown()) return;

			DBConnection dbc = db.get(name);
			if (dbc == null)
				throw new AdapterException("Connection " + name + " doesn't exist");
			conn = dbc.conn;
			calendar = dbc.calendar;

			if (conn.isClosed())
			{
				Misc.log(1,"Database connection " + name + " closed, trying reconnect");
				conn = reinitdb(name);
			}

			this.name = name;

			try
			{
				makeStatement(sql,list);
			}
			catch(SQLException ex)
			{
				int code = ex.getErrorCode();
				String message = ex.getMessage().toLowerCase();
				final String errors[] =  {"packet","shutdown","gone","closed","socket","state","pipe","timeout","timed out","connection","connexion"};
				for(int i = 0;i < errors.length;i++)
				{
					if (message.indexOf(errors[i]) != -1)
					{
						Misc.log(1,"Database connection " + name + " closed, statement retried");
						close();

						try
						{
							conn = reinitdb(name);
							makeStatement(sql,list);
						}
						catch(SQLException ex2)
						{
							Misc.log(1,"2nd SQL error " + ex2.getErrorCode() + " [" + name + "]: " + sql);
							close();
							Misc.rethrow(ex2);
						}
						catch(Exception ex2)
						{
							Misc.log(1,"2nd SQL error [" + name + "]: " + sql);
							close();
							Misc.rethrow(ex2);
						}

						return;
					}
				}

				Misc.log(1,"SQL error " + code + " [" + name + "]: " + sql);
				close();
				Misc.rethrow(ex);
			}
			catch(Exception ex)
			{
				Misc.log(1,"SQL error [" + name + "]: " + sql);
				close();
				Misc.rethrow(ex);
			}
		}

		public ArrayList<String> getHeader() throws Exception
		{
			if (rset == null) return null;

			ArrayList<String> headers = new ArrayList<String>();

			for(String name:columnnames)
				headers.add(name);

			return headers;
		}

		public int getResultCount()
		{
			return resultcount;
		}

		public LinkedHashMap<String,String> next() throws Exception
		{
			LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();

			if (stmt == null) return null;
			if (javaadapter.isShuttingDown() || rset == null || !rset.next())
			{
				close();
				return null;
			}

			for(int i = 0;i < columnnames.length;i++)
			{
				String value = null;
				java.util.Date date = null;

				switch(columntypes[i])
				{
				case Types.TIME:
					date = rset.getTime(i+1,calendar);
					break;
				case Types.DATE:
					date = rset.getDate(i+1,calendar);
					break;
				case Types.TIMESTAMP:
					date = rset.getTimestamp(i+1,calendar);
					break;
				case Types.BLOB:
					long length = rset.getBlob(i+1).length();
					byte[] bytes = rset.getBlob(i+1).getBytes(1,(int)length);
					value = new String(bytes);
					break;
				default:
					value = rset.getString(i+1);
				}

				if (date != null) value = Misc.gmtdateformat.format(date);
				if (value == null) value = "";

				row.put(columnnames[i],value.trim());
			}

			if (Misc.isLog(15)) Misc.log("row [" + name + "]: " + row);

			return row;
		}

		public void close() throws Exception
		{
			if (rset != null) rset.close();
			if (stmt != null) stmt.close();
			stmt = null;
		}
	}

	class DBConnection
	{
		Connection conn;
		String quote = "\"";
		dbtype dbtype;
		Calendar calendar;
	}

	public static final String replacement = "*@?@!";
	public static final Pattern replacementPattern = Pattern.compile("\\*@\\?@\\!");
	private static final String SQLDATEFORMAT = "'YYYY-MM-DD HH24:MI:SS'";
	private static final String ORACLEJDBCDRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String SQLSERVERJDBCDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String MYSQLJDBCDRIVER = "com.mysql.jdbc.Driver";
	private static final String DB2JDBCDRIVER = "com.ibm.db2.jcc.DB2Driver";
	private static final String SYBASEJDBCDRIVER = "com.sybase.jdbc4.jdbc.SybDriver";
	enum dbtype { MYSQL, MSSQL, ORACLE, DB2, OTHER };
	public ComparatorIgnoreCase<String> collator;

	private static DB instance;

	private HashMap<String,DBConnection> db;
	private XML[] xmlconn;

	protected DB() { }

	public DB(XML xmlcfg) throws Exception
	{
		init(xmlcfg);
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

	private Connection initdb(XML xml) throws Exception
	{
		String type = xml.getAttribute("type");
		if (type != null && !type.equals("db")) return null;

		String name = xml.getAttribute("name");
		String urlstr = xml.getValue("url",null);
		String driverstr = xml.getValue("driver",null);
		String connstr = (urlstr == null) ? "jdbc:oracle:thin:@" + xml.getValue("server") + ":" + xml.getValue("port","1521") + ":" + xml.getValue("instance") : urlstr;

		DBConnection dbc = new DBConnection();

		try
		{
			String username = xml.getValue("username",null);
			if (username == null)
				dbc.conn = DriverManager.getConnection(connstr);
			else
				dbc.conn = DriverManager.getConnection(connstr,username,xml.getValueCrypt("password"));
		}
		catch(SQLException ex)
		{
			Misc.rethrow(ex,"ERROR: Connecting to database " + name);
		}

		dbc.dbtype = dbtype.OTHER;

		dbc.calendar = Calendar.getInstance();
		String timezone = xml.getValue("timezone","UTC");
		if (!timezone.equals("local"))
			dbc.calendar.setTimeZone(TimeZone.getTimeZone(timezone));

		db.put(name,dbc);

		if (urlstr == null || (driverstr != null && driverstr.equals(ORACLEJDBCDRIVER)))
		{
			execsql(name,"alter session set NLS_DATE_FORMAT = " + SQLDATEFORMAT);
			execsql(name,"alter session set NLS_TIMESTAMP_FORMAT = " + SQLDATEFORMAT);
			execsql(name,"alter session set NLS_SORT = unicode_binary");
			dbc.dbtype = dbtype.ORACLE;
		}
		else if (driverstr != null && driverstr.equals(SQLSERVERJDBCDRIVER))
		{
			// TODO: Make setTransactionIsolation configurable, UNCOMMITED should not be default
			dbc.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			dbc.dbtype = dbtype.MSSQL;
		}
		else if (driverstr != null && driverstr.equals(MYSQLJDBCDRIVER))
		{
			execsql(name,"set names utf8 collate utf8_bin");
			dbc.quote = "`";
			dbc.dbtype = dbtype.MYSQL;
		}
		else if (driverstr != null && driverstr.equals(DB2JDBCDRIVER))
		{
			dbc.dbtype = dbtype.DB2;
		}

		XML[] xmlinit = xml.getElements("initsql");
		for(XML el:xmlinit)
			execsql(name,el.getValue());

		return dbc.conn;
	}

	private Connection reinitdb(String name) throws Exception
	{
		for(XML el:xmlconn)
		{
			String type = el.getAttribute("type");
			if (type != null && !type.equals("db")) continue;

			if (name.equals(el.getAttribute("name")))
			{
				try
				{
					Connection conn = db.get(name).conn;
					if (conn != null)
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
				}
				catch(Exception ex)
				{
				}

				return initdb(el);
			}
		}

		return null;
	}

	private void init(XML xmlcfg) throws Exception
	{
		collator = new DBComparator();

		db = new HashMap<String,DBConnection>();

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
		}

		if (withbd && !withdriver) Class.forName(ORACLEJDBCDRIVER);

		for(XML el:xmlconn)
			initdb(el);

		System.out.println("Done");
	}

	public String getorderby(String conn,String[] keys,boolean ignore_case) throws Exception
	{
		DBConnection dbc = db.get(conn);
		if (dbc == null)
			throw new AdapterException("Connection " + conn + " doesn't exist");

		StringBuilder sql = new StringBuilder("(");
		boolean first = true;

		switch(dbc.dbtype)
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
			keyfield = dbc.quote + keyfield + dbc.quote;
			if (ignore_case) keyfield = "upper(" + keyfield + ")";
			keyfield = "replace(replace(rtrim(ltrim(coalesce(" + keyfield + ",''))),' ','!'),'_','!')";
			switch(dbc.dbtype)
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

		switch(dbc.dbtype)
		{
		case MYSQL:
			sql.append(")");
			break;
		case DB2:
			sql.append(",'UCA400R1')");
			break;
		}
		sql.append(")");
		switch(dbc.dbtype)
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
		return new DBOper(conn,sql);
	}

	public DBOper makesqloper(String conn,XML xml) throws Exception
	{
		String sql = xml.getValue();
		return new DBOper(conn,sql);
	}

	public int execsqlresult(String conn,String sql) throws Exception
	{
		DBOper oper = new DBOper(conn,sql,null);
		return oper.getResultCount();
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(String conn,String sql) throws Exception
	{
		return execsql(conn,sql,null);
	}

	public ArrayList<LinkedHashMap<String,String>> execsql(String conn,String sql,List<String> list) throws Exception
	{
		DBOper oper = null;
		ArrayList<LinkedHashMap<String,String>> result = null;

		oper = new DBOper(conn,sql,list);
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

	public String getQuote(String instance)
	{
		DBConnection dbc = db.get(instance);
		return dbc.quote;
	}

	public void close()
	{
		try
		{
			Iterator<String> itr = db.keySet().iterator();
			while(itr.hasNext())
			{
				String key = itr.next();
				db.get(key).conn.close();
			}
		}
		catch(Exception ex)
		{
		}
	}

	public void lookup(XML xmlconfig,XML xml) throws Exception
	{
		String conn = xmlconfig.getAttribute("instance");
		XML xmlsql = xmlconfig.getElement("sql");
		String sql = xmlsql.getValue();

		final XML finalxml = xml;
		DBOper oper = makesqloper(conn,Misc.substitute(sql,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				String value = Misc.substituteGet(param,finalxml.getStringByPath(param));
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

	public dbtype getType(String instance)
	{
		DBConnection dbc = db.get(instance);
		if (dbc == null) return null;
		return dbc.dbtype;
	}
}

public class dbsql
{
	private static DB db;

	private static void Export(String instance,String table) throws Exception
	{
		DB.DBOper oper = db.makesqloper(instance,"select * from " + table);
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
			ArrayList<String> headers = csvin.getHeader();
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
