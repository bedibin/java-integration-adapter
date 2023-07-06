import com.peregrine.ac.api.*;
import com.peregrine.ac.AmException;
import java.util.*;
import java.util.regex.*;
import java.text.*;
import org.json.JSONObject;

class AMDBOper extends DBOper
{
	private int pos = 0;
	private XML[] rowresult;
	private AMDB db;

	public AMDBOper(AMDB db,String sql,List<DBField> list) throws AdapterDbException
	{
		if (list == null || list.isEmpty())
			init(db,sql);
		else
		{
			Matcher matcher = replacementPattern.matcher(sql);
			StringBuffer sb = new StringBuffer();
			int x = 0;
			while(matcher.find())
			{
				if (x >= list.size()) throw new AdapterRuntimeException("Too many replacement characters: " + sql + implode(list));
				DBField field = list.get(x);
				String value = field.getValue();
				String tablename = field.getTableName();
				String fieldname = field.getFieldName();
				if (value == null)
					value = "null";
				else if (tablename != null && fieldname != null)
				{
					// https://docs.microfocus.com/itom/Asset_Manager:9.70/PrgmRefAmGetFieldUserType
					// https://docs.microfocus.com/itom/Asset_Manager:9.70/PrgmRefAmGetFieldType
					String attrtype = db.getType(tablename,fieldname);
					if (attrtype == null)
					{
						try {
							long tableid = AmApi.AmGetTableFromName(db.getAMConnection(),tablename);
							long fieldid = AmApi.AmGetFieldFromName(tableid,fieldname);
							attrtype = AmApi.AmGetFieldUserType(fieldid) + ":" + AmApi.AmGetFieldType(fieldid);
							db.setType(tablename,fieldname,attrtype);
							if (Misc.isLog(35))
								Misc.log("AQLTYPE: " + tablename + ":" + fieldname + ":" + attrtype + ": " + value);
							try { AmApi.AmReleaseHandle(fieldid); } catch(AmException ex) {}
							try { AmApi.AmReleaseHandle(tableid); } catch(AmException ex) {}
						} catch(AmException ex) {
							throw new AdapterDbException(ex);
						}
					}
					switch(attrtype) {
					case "0:6": // String
					case "0:12": // Memo
					case "8:6": // Custom list
						value = "'" + value.replace("'","''") + "'";
						break;
					case "0:10": // Date
						value = "#" + value.substring(0,10) + "#";
						break;
					case "0:1": // Byte
					case "0:2": // Short
					case "0:3": // Long
					case "0:4": // Float
					case "0:5": // Double
					case "3:5": // Money
					case "7:2": // System list
						break;
					case "0:7": // TimeStamp
					case "0:11": // Time
						try {
							Date date = Misc.getGmtDateFormat().parse(value);
							value = "#" + db.dateformat.format(date) + "#";
						} catch(ParseException ex) {
							throw new AdapterDbException(ex);
						}
						break;
					default:
						throw new AdapterRuntimeException("Unsupported AM type " + attrtype + " for field " + fieldname + " table " + tablename + ": " + value);
					}
				}
				else
					// Use default string processing
					value = "'" + value.replace("'","''") + "'";

				matcher.appendReplacement(sb,Matcher.quoteReplacement(value));
				x++;
			}
			if (x < list.size()) throw new AdapterRuntimeException("Not enough replacement characters: " + sql + implode(list));

			matcher.appendTail(sb);
			init(db,sb.toString());
		}
	}

	public AMDBOper(AMDB db,String sql) throws AdapterException
	{
		init(db,sql);
	}

	private void init(AMDB db,String sql) throws AdapterDbException
	{
		if (Misc.isLog(8)) Misc.log("AQL: " + sql);

		this.db = db;

		if (sql.startsWith("select") || sql.startsWith("SELECT"))
		{
			try {
				String out  = AmApi.AmQuery(db.getAMConnection(),sql,0,0,true);
				StringBuilder sb = new StringBuilder(out);
				XML xml = new XML(sb);

				XML[] columnlist = xml.getElement("Schema").getElements("Column");
				columnnames = new String[columnlist.length];
				for(int i = 0;i < columnlist.length;i++)
				{
					int index = Integer.parseInt(columnlist[i].getAttribute("Index"));
					columnnames[index] = columnlist[i].getAttribute("Name");
				}

				rowresult = xml.getElement("Result").getElements("Row");
				resultcount = rowresult.length;
				if (Misc.isLog(15)) Misc.log("Number of entries returned: " + resultcount);
				return;
			} catch(AmException | AdapterException ex) {
				throw new AdapterDbException(ex);
			}
		}

		try
		{
			AmApi.AmStartTransaction(db.getAMConnection());
			AmApi.AmDbExecAql(db.getAMConnection(),sql);
			AmApi.AmCommit(db.getAMConnection());
		}
		catch(AmException ex)
		{
			try {
				AmApi.AmRollback(db.getAMConnection());
			} catch(AmException ex2) {
				throw new AdapterDbException(ex2);
			}
			String message = ex.getMessage();
			if (message.indexOf("Impossible de changer de type de gestion") != -1) // TODO: Add English translation
			{
				throw new AdapterDbException(message + ": unique constraint");
			}

			throw new AdapterDbException(ex);
		}
	}

	@Override
	public Map<String,String> next() throws AdapterDbException
	{
		if (rowresult == null || pos >= rowresult.length) return null;

		Map<String,String> row = new LinkedHashMap<>();

		for(int i = 0;i < columnnames.length;i++)
		{
			try {
				String value = rowresult[pos].getValueByPath("Column[@Index='"+i+"']");
				if (value == null) value = "";
				if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
				{
					Date date = db.dateformat.parse(value);
					value = Misc.getGmtDateFormat().format(date);
				}
				row.put(columnnames[i],value);
			} catch(ParseException | AdapterException ex) {
				throw new AdapterDbException(ex);
			}
		}

		pos++;
		return row;
	}

	@Override
	public void close() throws AdapterDbException
	{
	}
}

class AMDB extends DB
{
	private long amconn = 0;
	private String username;
	private static AMDB instance;
	private HashMap<String,String> attrtypes = new HashMap<>();
	public SimpleDateFormat dateformat;
	public DecimalFormat currencyformat;

	private AMDB() throws AdapterException
	{
		System.out.print("Connection to AM... ");
		XML xml = javaadapter.getConfiguration().getElementByPath("/configuration/connection[@type='am']");
		if (xml == null) throw new AdapterException("No connection element with type 'am' specified");

		String instanceval = xml.getValue("instance");
		username = xml.getValue("username");
		String password = xml.getValueCrypt("password");

		try {
			amconn = AmApi.AmGetConnection(instanceval,username,password,"");
			if (amconn == 0)
				throw new AdapterException(xml,"AM connection parameters are incorrect");
			AmApi.AmAuthenticateUser(amconn,username,password);
		} catch(AmException ex) {
			throw new AdapterException(ex);
		}

		dateformat = new SimpleDateFormat(Misc.DATEFORMAT);
		String timezone = xml.getValue("timezone","UTC");
		if (!timezone.equals("local"))
			dateformat.setTimeZone(TimeZone.getTimeZone(timezone));

		currencyformat = (DecimalFormat)NumberFormat.getCurrencyInstance();

		System.out.println("Done");
	}

	@Override
	public String getConcat(String conn,String field,String addedfield)
	{
		return field + " + " + addedfield;
	}

	public static synchronized AMDB getInstance() throws AdapterException
        {
		if (instance == null)
		{
			instance = new AMDB();
			javaadapter.setForShutdown(instance);
		}
		return instance;
	}

	@Override
	public int execsqlresult(String conn,String sql,List<DBField> list) throws AdapterDbException
	{
		AMDBOper oper = new AMDBOper(this,sql,list);
		return oper.getResultCount();
	}

	@Override
	public int execsqlresult(String conn,String sql) throws AdapterDbException
	{
		return execsqlresult(conn,sql,null);
	}

	@Override
	public List<Map<String,String>> execsql(String conn,String sql) throws AdapterDbException
	{
		return execsql(conn,sql,null);
	}

	@Override
	public List<Map<String,String>> execsql(String conn,String sql,List<DBField> list) throws AdapterDbException
	{
		AMDBOper oper = null;
		List<Map<String,String>> result = null;

		oper = new AMDBOper(this,sql,list);
		result = new ArrayList<>();
		Map<String,String> row;

		while((row = oper.next()) != null)
			result.add(row);

		if (Misc.isLog(result.isEmpty() ? 10 : 9)) Misc.log("AQL result [" + conn + "]: " + result);

		return result;
	}

	public long getAMConnection()
	{
		return amconn;
	}

	public String getType(String tablename,String fieldname)
	{
		return attrtypes.get(tablename + ":" + fieldname);
	}

	public void setType(String tablename,String fieldname,String type)
	{
		attrtypes.put(tablename + ":" + fieldname,type);
	}
}

class AssetManagerUpdateSubscriber extends DatabaseUpdateSubscriber
{
	public AssetManagerUpdateSubscriber(XML xml) throws AdapterException
	{
		super(xml);
		db = AMDB.getInstance();
		setQuoteField("");
	}
}

class AssetManagerRestSubscriber extends UpdateSubscriber
{
	protected String getDate(String value) throws AdapterException
	{
		try {
			Date date = Misc.getGmtDateFormat().parse(value);
			return "" + date.getTime();
		} catch(java.text.ParseException ex) {
			throw new AdapterException(ex);
		}
	}

	protected String getValue(String value) throws AdapterException
	{
		if (value == null) return "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
			return getDate(value);
		if (value.matches("\\d{4}-\\d{2}-\\d{2}"))
			return getDate(value + " 00:00:00");
		value = value.replace("\r","");
		value = value.replace("\n","\r\n");
		value = value.replace("'","''");
		return value;
	}

	protected void add(XML xmloper) throws AdapterException
	{
		oper("post",xmloper);
	}

	protected void remove(XML xmloper) throws AdapterException
	{
		oper("delete",xmloper);
	}

	protected void update(XML xmloper) throws AdapterException
	{
		oper("put",xmloper);
	}

	protected void start(XML xmloper) throws AdapterException {}
	protected void end(XML xmloper) throws AdapterException {}

	protected void oper(String httpoper,XML xmloper) throws AdapterException
	{
		XML xml = javaadapter.getConfiguration().getElementByPath("/configuration/connection[@type='am']");
		if (xml == null) throw new AdapterException("No connection element with type 'am' specified");

		XML pubxml = new XML();
		XML pub = pubxml.add("publisher");
		pub.setAttribute("name",xmloper.getParent().getAttribute("name"));
		pub.setAttribute("username",xml.getValue("username",null));
		pub.setAttribute("password",xml.getValue("password",null));
		pub.setAttribute("type","http");
		pub.setAttribute("content_type","application/json");

		UpdateDestInfo destinfo = getDestInfo();
		String table = destinfo.getTableName();
		if (table == null) throw new AdapterException("dbsync: destination 'table' attribute required");

		ArrayList<XML> customs = null;
		SyncOper oper = Enum.valueOf(SyncOper.class,xmloper.getTagName().toUpperCase());
		if (oper == SyncOper.ADD) customs = destinfo.getCustomList(SyncOper.ADD);
		else if (oper == SyncOper.REMOVE) customs = destinfo.getCustomList(SyncOper.REMOVE);

		JSONObject js = new JSONObject();

		String id = null;
		XML idxml = xmloper.getElement("ID");
		if (idxml != null)
		{
			id = idxml.getValue();
			if (id == null) id = idxml.getValue("oldvalue",null);
		}
		if (id == null && oper != SyncOper.ADD) throw new AdapterException(xmloper,"ID value required");
		if (id == null) id = "";

		XML[] fields = xmloper.getElements();
		for(XML field:fields)
		{
			String name = field.getTagName();
			String value = field.getValue();
			if (value == null) value = "";
			String type = field.getAttribute("type");
			if (type != null)
			{
				if (type.equals("info")) continue;
				if (type.equals("infoapi")) continue;
				if (type.equals("key"))
				{
					js.put(name,getValue(value));
					continue;
				}
			}

			if (oper == SyncOper.REMOVE) continue;
			if (oper == SyncOper.UPDATE)
			{
				XML old = field.getElement("oldvalue");
				if (old == null) continue;
				String oldvalue = old.getValue();
				if (type != null && type.equals("initial") && oldvalue != null) continue;
			}
			js.put(name,getValue(value));
		}

		if (customs != null && customs.size() > 0)
		{
			httpoper = "put";
			for(XML custom:customs)
			{
				String namecust = custom.getAttribute("name");
				if (namecust == null) throw new AdapterException(custom,"Attribute 'name' required");
				String valuecust = custom.getAttribute("value");
				if (valuecust == null) valuecust = "";
				js.put(namecust,valuecust.replace("'","''"));
			}
		}

		pub.setAttribute("method",httpoper);
		//pub.setAttribute("url",destinfo.getUrl() + table + "/" + java.net.URLEncoder.encode(where.toString(),"UTF-8").replace("%","\\%"));
		pub.setAttribute("url",xml.getValue("url") + table + "/" + id);

		// Support for session persistency?
		Publisher publisher = Publisher.getInstance();
		publisher.publish(js.toString(),pubxml);
	}
}

public class assetmanagerdbsync
{
	public static void main(String[] args) throws Exception
	{
		long amconn = AmApi.AmGetConnection("db","Admin","Optimum987","");
		if (amconn == 0)
			throw new AdapterException("AM connection paramaters are incorrect");

		String result = AmApi.AmQuery(amconn,"select Name,ComputerDesc from amComputer where AssetTag like 'PC112%'",0,0,true);
		System.out.println("Query " + result);
		AmApi.AmDbExecAql(amconn,"update amComputer set ComputerDesc = 'test' where AssetTag = 'PC112811'");
	}
}
