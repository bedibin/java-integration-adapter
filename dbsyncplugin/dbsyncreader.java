import java.util.*;
import java.io.*;
import java.text.*;
import java.util.regex.*;
import java.nio.file.Path;

interface Reader
{
	public LinkedHashMap<String,String> next() throws Exception;
	public Set<String> getHeader();
	public String getName();
	public boolean isSorted();
}

abstract class ReaderUtil implements Reader
{
	private XML xmlreader;
	private Set<String> keyfields;
	private boolean issorted = false;
	private boolean totrim = true;
	private LinkedHashMap<String,String> last;
	private OnOper onempty;
	private int rowcount;
	private String colname;
	private String pathcol;
	protected String pathrow;
	protected DB db;
	protected Set<String> headers;
	protected boolean skipnormalize = true;
	protected String instance;

	static final public Reader getReader(XML xml) throws Exception
	{
		return getReader(xml,xml);
	}

	static final public Reader getReader(XML xml,XML xmlsource) throws Exception
	{
		Reader reader = null;
		String type = xml.getAttribute("type");

		if (type == null)
			reader = new ReaderRow(xml);
		else if (type.equals("db"))
			reader = new ReaderSQL(xml);
		else if (type.equals("csv"))
			reader = new ReaderCSV(xml);
		else if (type.equals("ldap"))
			reader = new ReaderLDAP(xml);
		else if (type.equals("xml"))
			reader = new ReaderXML(xml,xmlsource);
		else if (type.equals("jms"))
			reader = new ReaderJMS(xml);
		else if (type.equals("class"))
		{
			String name = xml.getAttribute("class");
			Object objects[] = new Object[1];
			objects[0] = xml;
			Class<?> types[] = new Class[1];
			types[0] = xml.getClass();
			reader = (Reader)Class.forName(name).getConstructor(types).newInstance(objects);
		}
		else
			throw new AdapterException(xml,"Unsupported reader type " + type);

		return reader;
	}

	public ReaderUtil() throws Exception
	{
 		db = DB.getInstance();
	}

	public ReaderUtil(boolean issorted) throws Exception
	{
 		db = DB.getInstance();
		this.issorted = issorted;
	}

	public ReaderUtil(Set<String> keyfields,boolean issorted,boolean totrim) throws Exception
	{
 		db = DB.getInstance();
		this.keyfields = keyfields;
		this.issorted = issorted;
		this.totrim = totrim;
	}

	public ReaderUtil(XML xml) throws Exception
	{
 		db = DB.getInstance();
		xmlreader = xml;

		String tagname = xml.getTagName();
		if (!(tagname.equals("source") || tagname.equals("destination")))
			instance = xml.getAttribute("name");

		String keyfield = xml.getAttribute("keyfield");
		if (keyfield == null) keyfield = xml.getAttribute("keyfields");
		if (keyfield == null) keyfield = xml.getParent().getAttribute("keyfield");
		if (keyfield == null) keyfield = xml.getParent().getAttribute("keyfields");
		if (keyfield != null) keyfields = Misc.arrayToSet(keyfield.split("\\s*,\\s*"));

		String sorted = xml.getAttribute("sorted");
		issorted = sorted != null && sorted.equals("true");

		String trim = xml.getAttribute("trim");
		if (trim != null && trim.equals("false")) totrim = false;

		String fields = xml.getAttribute("fields");
		if (fields != null)
		{
			headers = Misc.arrayToSet(fields.split("\\s*,\\s*"));
			skipnormalize = false;
		}

		onempty = Field.getOnOper(xml,"on_no_row",OnOper.IGNORE,EnumSet.of(OnOper.EXCEPTION,OnOper.IGNORE));
		rowcount = 0;

		pathcol = xml.getAttribute("resultpathcolumn");
		colname = xml.getAttribute("resultattributename");
		pathrow = xml.getAttribute("resultpathrow");
	}

	static final public void pushCurrent(Map<String,String> row,Map<String,Set<String>> map,boolean issorted) throws Exception
	{
		for(String keyrow:row.keySet())
		{
			Set<String> set = map.get(keyrow);
			if (set == null) set = issorted ? new LinkedHashSet<String>() : new TreeSet<String>(DB.getInstance().getCollatorIgnoreCase());

			String rowvalue = row.get(keyrow);
			if (!rowvalue.isEmpty()) // Do not insert an empty value
			{
				rowvalue = XML.fixValue(rowvalue.replace("\r\n","\n"));
				set.add(rowvalue);
			}
			map.put(keyrow,set);
		}
	}

	abstract public LinkedHashMap<String,String> nextRaw() throws Exception;

	private void CheckEmpty() throws AdapterException
	{
		if (rowcount == 0 && onempty == OnOper.EXCEPTION)
			throw new AdapterException("Reader " + getName() + " didn't return any record");
	}

	public final LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row;
		if (keyfields == null)
		{
			row = nextRaw();
			if (row != null) rowcount++;
			CheckEmpty();
			return row;
		}

		LinkedHashMap<String,String> current = last;
		HashMap<String,Set<String>> currentmap = new HashMap<String,Set<String>>();
		if (current != null) pushCurrent(current,currentmap,issorted);

		while((row = nextRaw()) != null)
		{
			rowcount++;
			if (!row.keySet().containsAll(keyfields))
			{
				if (Misc.isLog(3)) Misc.log("WARNING: Keys '" + Misc.implode(keyfields) + "' missing in extraction. Cannot merge multiple records sharing the same key: " + Misc.implode(row.keySet()));
				keyfields = null;
				return row;
			}

			if (current == null)
			{
				current = row;
				pushCurrent(current,currentmap,issorted);
				continue;
			}

			boolean samekeys = true;
			for(String key:keyfields)
			{
				String value = current.get(key);
				if (!value.equalsIgnoreCase(row.get(key)))
				{
					samekeys = false;
					break;
				}
			}

			if (!samekeys) break;

			pushCurrent(row,currentmap,issorted);
		}

		last = row;
		if (current == null)
		{
			CheckEmpty();
			return null;
		}

		for(String keyrow:current.keySet())
			current.put(keyrow,Misc.implode(currentmap.get(keyrow),"\n"));

		if (Misc.isLog(18)) Misc.log("row [merge/" + getName() + "]: " + current);

		return current;
	}

	public final LinkedHashMap<String,String> normalizeFields(LinkedHashMap<String,String> row) throws Exception
	{
		if (row == null) return null;
		if (skipnormalize || headers == null) return row;

		LinkedHashMap<String,String> result = new LinkedHashMap<String,String>();
		for(String name:headers)
		{
			String value = row.get(name);
			result.put(name,value == null ? "" : value);
		}
		return result;
	}

	public final void ReadXML(XML xml,XML xmlsource) throws Exception
	{
		Reader reader = ReaderUtil.getReader(xml,xmlsource);

		LinkedHashMap<String,String> row;

		XML xmltable = xmlsource.add(reader.getName());
		xml.copyAttributes(xmltable);

		while((row = reader.next()) != null)
			if (row.size() > 0)
				xmltable.add("row",row);
	}

	public final XML ProcessXML(XML xml,XML xmlsource) throws Exception
	{
		String filename = xml.getAttribute("filename");
		if (filename == null)
		{
			if (xmlsource == null)
			{
				xmlsource = new XML();
				xmlsource.add("root");
			}
		}
		else
			xmlsource = new XML(filename);

		XML[] elements = xml.getElements(null);
		for(XML element:elements)
		{
			String tagname = element.getTagName();

			if (tagname.equals("element"))
				xmlsource.add(element);
			else if (tagname.equals("function"))
			{
				Subscriber sub = new Subscriber(element);

				Operation.ResultTypes resulttype = Operation.getResultType(element);
				XML xmlresult = sub.run(xmlsource);
				switch(resulttype)
				{
				case LAST:
					xmlsource = xmlresult;
					break;
				case MERGE:
					xmlsource.add(xmlresult);
					break;
				}
			}
			else if (tagname.equals("read"))
				ReadXML(element,xmlsource);

			if (Misc.isLog(9)) Misc.log("XML reader " + tagname + ": " + xmlsource);
		}

		return xmlsource;
	}

	public final void getSubXML(LinkedHashMap<String,String> row,String prefix,XML xml) throws Exception
	{
		HashMap<String,String> attributes = xml.getAttributes();
		if (attributes == null) return;

		for(Map.Entry<String,String> entry:attributes.entrySet())
		{
			String value = entry.getValue();
			if (value == null) value = "";
			String name = entry.getKey();
			if (prefix != null) name = prefix + "_" + name;
			if (headers != null && !headers.contains(name)) headers.add(name);
			row.put(name,totrim ? value.trim() : value);
		}

		XML[] elements = prefix == null ? xml.getElements() : xml.getElementsByPath(pathcol);
		for(XML element:elements)
		{
			String name = element.getTagName();
			if (name == null) continue;
			if (colname != null) name = name + "_" + element.getAttribute(colname);
			if (prefix != null) name = prefix + "_" + name;
			if (headers != null && !headers.contains(name)) headers.add(name);
			String value = element.getValue();
			if (value == null) value = "";
			row.put(name,totrim ? value.trim() : value);
			getSubXML(row,name,element);
		}
	}

	public final Set<String> getHeader()
	{
		return headers;
	}

	public final String getName()
	{
		return instance;
	}

	public final boolean isSorted()
	{
		return issorted;
	}

	public final boolean toTrim()
	{
		return totrim;
	}

	public final XML getXML()
	{
		return xmlreader;
	}
}

class ReaderRow extends ReaderUtil
{
	XML[] rows;
	int rowpos;
	private String default_name;

	public ReaderRow(XML xml) throws Exception
	{
		super(xml);

		if (instance == null) instance = "row";
		rowpos = 0;
		default_name = xml.getAttribute("default_field_name");

		if (headers == null) headers = new HashSet<String>();
		if (default_name != null) headers.add(default_name);
		rows = xml.getElements("row");
		if (rows.length > 0) headers.addAll(rows[0].getAttributes().keySet());
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		if (rowpos >= rows.length) return null;
		LinkedHashMap<String,String> attributes = rows[rowpos].getAttributes();
		LinkedHashMap<String,String> result = normalizeFields(attributes);
		if (default_name != null && !attributes.containsKey(default_name))
		{
			String default_value = rows[rowpos].getValue();
			result.put(default_name,default_value == null ? "" : default_value);
		}

		rowpos++;
		return result;
	}
}

class ReaderCSV extends ReaderUtil
{
	private char enclosure = '"';
	private char delimiter = ',';
	private BufferedReader in;
	private Set<String> csvheaders;

	private void setValue(ArrayList<String> result,StringBuilder sb) throws Exception
	{
		String value = sb.toString();
		if (value.equals("\"")) value = "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2}"))
			value += " 00:00:00";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?"))
		{
			// Dates in CSV files are local
			Date date = Misc.dateformat.parse(value);
			value = Misc.gmtdateformat.format(date);
		}

		result.add(value);
	}

	public ArrayList<String> readCSV() throws Exception
	{
		if (in == null) return null;

		boolean inquote = false;
		ArrayList<String> result = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();

		do
		{
			String line = in.readLine();
			while(line != null && !inquote && line.trim().isEmpty())
				line = in.readLine();
			if (line == null)
			{
				in.close();
				in = null;
				return null;
			}

			if (inquote) sb.append("\n");

			char last = 0;
			for(int i = 0;i < line.length();i++)
			{
				char c = line.charAt(i);

				if (c == enclosure)
				{
					if (last == enclosure)
					{
						sb.append(c);
						c = 0;
					}

					inquote = !inquote;
					last = c;
					continue;
				}

				if (!inquote && c == delimiter)
				{
					setValue(result,sb);
					sb = new StringBuilder();
					last = 0;
					continue;
				}

				sb.append(c);
				last = c;
			}
		} while(inquote);

		setValue(result,sb);

		return result;
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		ArrayList<String> csv = readCSV();
		if (csv == null) return null;

		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		int size = csv.size();
		int i = 0;
		for(String name:csvheaders)
		{
			String value = (i >= size) ? "" : XML.fixValue(toTrim() ? csv.get(i).trim() : csv.get(i));
			row.put(name,value);
			i++;
		}

		if (Misc.isLog(15)) Misc.log("row [csv]: " + row);
		return row;
	}

	private void initFile(String filename,String charset) throws Exception
	{
		if (filename == null)
			throw new AdapterException("Filename is mandatory");

		Set<Path> paths = Misc.glob(filename);
		if (paths.size() != 1) throw new FileNotFoundException("File not found or multiple match: " + filename);
		File file = new File(paths.iterator().next().toString());
		initFile(file,charset);
	}

	private void initFile(File file,String charset) throws Exception
	{
		in = Misc.getFileReader(file,charset);

		ArrayList<String> row = readCSV();
		if (row != null && row.size() > 0)
		{
			// Ignore last empty columns
			for(int x = row.size() - 1;x >= 0;x--)
				if (row.get(x).trim().isEmpty()) row.remove(x); else break;

			if (headers == null)
			{
				// Autodetect headers
				headers = csvheaders = new LinkedHashSet<String>(row);
				if (row.size() != csvheaders.size())
					throw new AdapterException("Duplicated header values in CSV file " + file.getName() + ": " + Misc.findDuplicates(row));
			}
			else
			{
				ArrayList<String> commonheaders = new ArrayList<String>(row);
				commonheaders.retainAll(headers);
				if (commonheaders.size() == 0)
				{
					// No common header, file has no header, use fields attribute based on positions
					in = Misc.getFileReader(file,charset);
					csvheaders = headers;
					
				}
				else
				{
					// Case where fields attribute is for limiting extracted columns
					csvheaders = new LinkedHashSet<String>(row);
					if (row.size() != csvheaders.size())
						throw new AdapterException("Duplicated header values in CSV file " + file.getName() + ": " + Misc.findDuplicates(row));
				}
			}
		}
		else if (headers == null)
			throw new AdapterException("CSV file \"" + file.getName() + "\" requires at least one header line or \"fields\" attribute must be specified");
		else csvheaders = headers;

		if (instance == null) instance = file.getName();
	}

	public ReaderCSV(java.io.Reader reader) throws Exception
	{
		in = new BufferedReader(reader);
		if (headers == null)
		{
			ArrayList<String> row = readCSV();
			headers = csvheaders = new LinkedHashSet<String>(row);
			if (row.size() != headers.size())
				throw new AdapterException("Duplicated header values in CSV reader: " + Misc.findDuplicates(row));
		}
		if (headers == null)
			throw new AdapterException("CSV reader requires at least one header line or \"fields\" attribute");

		if (instance == null) instance = reader.getClass().getName();
	}

	public ReaderCSV(XML xml) throws Exception
	{
		super(xml);

		String filename = xml.getAttribute("filename");

		String delimiter = Misc.unescape(xml.getAttribute("delimiter"));
		if (delimiter != null) this.delimiter = delimiter.charAt(0);
		String enclosure = Misc.unescape(xml.getAttribute("enclosure"));
		if (enclosure != null) this.enclosure = enclosure.charAt(0);

		String charset = xml.getAttribute("charset");
		initFile(filename,charset);
	}

	public ReaderCSV(String filename,char delimiter,char enclosure) throws Exception
	{
		if (delimiter != 0) this.delimiter = delimiter;
		if (enclosure != 0) this.enclosure = enclosure;
		initFile(filename,null);
	}

	public ReaderCSV(String filename) throws Exception
	{
		initFile(filename,null);
	}

	public ReaderCSV(File file,String charset) throws Exception
	{
		initFile(file,charset);
	}

	public void close() throws Exception
	{
		if (in != null) in.close();
		in = null;
	}
}

class ReaderLDAP extends ReaderUtil
{
	private directory ld;
	private LinkedHashMap<String,String> first;

	private void init(String url,String context,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref,boolean notrust) throws Exception
	{
		if (instance == null) instance = url;

		Misc.log(5,"Searching for " + search + " on " + url + " base " + basedn);

		ld = context == null ? new ldap(url,username,password,sortattrs,auth,referral,deref,notrust) : new directory(url,context,username,password,auth);

		if (headers == null)
		{
			ld.search(basedn,search,null);
			first = next();
			if (first == null)
				throw new AdapterException("Processing empty LDAP content is not supported. Please set \"fields\" attribute");

			headers = new HashSet<String>(first.keySet());
		}
		else
			ld.search(basedn,search,headers.toArray(new String[headers.size()]));
	}

	public ReaderLDAP(XML xml) throws Exception
	{
		super(xml);

		String url = Misc.substitute(xml.getAttribute("url"));
		String username = xml.getAttribute("username");
		String password = xml.getAttributeCrypt("password");
		String auth = xml.getAttribute("authentication");
		String basedn = Misc.substitute(xml.getAttribute("basedn"));
		String search = Misc.substitute(xml.getAttribute("query"));
		String context = xml.getAttribute("context");
		String referral = xml.getAttribute("referral");
		String deref = xml.getAttribute("derefAliases");
		String sortfields = xml.getAttribute("sort_fields");
		String notrustssl = xml.getAttribute("notrustssl");
		boolean notrust = notrustssl != null && "true".equals(notrustssl);

		String[] sortattrs = sortfields == null ? null :  sortfields.split("\\s*,\\s*");

		init(url,context,username,password,basedn,search,sortattrs,auth,referral,deref,notrust);
	}

	public ReaderLDAP(String url,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref,boolean notrust) throws Exception
	{
		init(url,null,username,password,basedn,search,sortattrs,auth,referral,deref,notrust);
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		LinkedHashMap<String,String> row;

		if (first == null)
		{
			if (ld == null) return null;
			row = ld.searchNext();
			if (row == null)
			{
				ld.disconnect();
				return null;
			}
		}
		else
		{
			row = first;
			first = null;
		}

		row = normalizeFields(row);
		if (Misc.isLog(15)) Misc.log("row [ldap]: " + row);
		return row;
	}
}

class ReaderSQL extends ReaderUtil
{
	private DBOper oper = null;

	private void init(String conn,String sql) throws Exception
	{
		String sqlsub = Misc.substitute(sql,db.getConnectionByName(conn));
		oper = db.makesqloper(conn,sqlsub,toTrim());

		if (headers == null) headers = oper.getHeader();
		if (instance == null) instance = conn;
	}

	public ReaderSQL(XML xml) throws Exception
	{
		super(xml);

		String conn = xml.getAttribute("instance");
		XML sqlxml = xml.getElement("extractsql");
		String sql = sqlxml == null ? xml.getValue() : sqlxml.getValue();
		init(conn,sql);
	}

	public ReaderSQL(String conn,String sql) throws Exception
	{
		init(conn,sql);
	}

	public ReaderSQL(String conn,String sql,Set<String> keyfields,boolean issorted,boolean totrim) throws Exception
	{
		super(keyfields,issorted,totrim);
		init(conn,sql);
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		LinkedHashMap<String,String> row = oper.next();
		if (row == null) return null;

		row = normalizeFields(row);
		if (Misc.isLog(15)) Misc.log("row [sql]: " + row);
		return row;
	}
}

class ReaderXML extends ReaderUtil
{
	protected XML[] xmltable;
	protected int position = 0;

	protected LinkedHashMap<String,String> getXML(int pos) throws Exception
	{
		if (pos >= xmltable.length) return null;

		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		getSubXML(row,null,xmltable[pos]);
		return row;
	}

	public ReaderXML() throws Exception
	{
	}

	public ReaderXML(XML xml) throws Exception
	{
		init(xml);
	}

	public ReaderXML(XML xml,XML xmlsource) throws Exception
	{
		super(xml);
		init(ProcessXML(xml,xmlsource));
	}

	private void init(XML xml) throws Exception
	{
		xmltable = xml.getElementsByPath(pathrow);
		if (Misc.isLog(5)) Misc.log("Found " + xmltable.length + " elements with path " + pathrow);
		if (instance == null) instance = xml.getTagName();

		if (headers != null) return;

		LinkedHashMap<String,String> first = getXML(0);
		if (first == null)
			throw new AdapterException("Processing empty XML content is not supported. Please set \"fields\" attribute");

		headers = new LinkedHashSet<String>(first.keySet());
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		LinkedHashMap<String,String> row = getXML(position);
		if (row == null) return null;

		position++;

		row = normalizeFields(row);
		if (Misc.isLog(15)) Misc.log("row [xml]: " + row);
		return row;
	}
}

class CacheReader implements Reader
{
	private ReaderCSV csvreader;
	private Reader sourcereader;
	private File csvfile;

	public CacheReader(XML xml,Reader reader) throws Exception
	{
		javaadapter.setForShutdown(this);

		String cachedir = xml.getAttribute("cached_directory");
		String prefix = "javaadapter_" + reader.getName() + "_";
		String suffix = ".csv";
		if (cachedir == null)
			csvfile = File.createTempFile(prefix,suffix);
		else
		{
			File dir = new File(javaadapter.getCurrentDir(),cachedir);
			csvfile = File.createTempFile(prefix,suffix,dir);
		}

		if (Misc.isLog(10)) Misc.log("Cached file is " + csvfile.getAbsolutePath());
		sourcereader = reader;

		try
		{
			Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvfile),"UTF8"));
			CsvWriter csv = new CsvWriter(writer,reader.getHeader());

			LinkedHashMap<String,String> row;
			while((row = reader.next()) != null)
				csv.write(row);

			csv.flush();
			csvreader = new ReaderCSV(csvfile,"UTF8");
		} catch(Exception ex) {
			Misc.rethrow(ex);
		}
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> result = csvreader.next();
		if (result == null) csvfile.delete();
		return result;
	}

	public Set<String> getHeader()
	{
		return sourcereader.getHeader();
	}

	public String getName()
	{
		return "cache/" + sourcereader.getName();
	}

	public boolean isSorted()
	{
		return sourcereader.isSorted();
	}

	public void close() throws Exception
	{
		if (csvreader != null) csvreader.close();
		if (csvfile != null) csvfile.delete();
	}
}

class SortTable implements Reader
{
	private String tablename;
	private String conn;
	private DB db = DB.getInstance();
	private DBOper oper;
	private TreeMap<String,LinkedHashMap<String,Set<String>>> sortedmap;
	private Iterator<String> iterator;
	private String instance;
	private Set<String> header;
	private DBSyncOper dbsync;

	public SortTable(XML xml,Sync sync) throws Exception
	{
		dbsync = sync.getDBSync();
		Reader reader = sync.getReader();
		this.header = reader.getHeader();

		XML sortxml = xml.getElement("dbsyncsorttable");
		if (sortxml == null)
		{
			xml = xml.getParent();
			sortxml = xml.getElement("dbsyncsorttable");
		}
		if (sortxml == null)
			sortxml = xml.getParent().getElement("dbsyncsorttable");
		if (sortxml != null)
		{
			tablename = sortxml.getAttribute("name");
			conn = sortxml.getAttribute("instance");
		}

		if (tablename == null || conn == null)
		{
			instance = "memsort/" + reader.getName();
			sortedmap = new TreeMap<String,LinkedHashMap<String,Set<String>>>(dbsync.getIgnoreCaseKeys() ? db.getCollatorIgnoreCase() : db.getCollator());
			Misc.log(7,"Initializing memory sort");
		}
		else
		{
			instance = "dbsort/" + reader.getName();
			Misc.log(7,"Initializing temporary DB table sort");
		}

		put(sync);
	}

	public void put(Sync sync) throws Exception
	{
		LinkedHashMap<String,String> row;
		Fields fields = dbsync.getFields();
		while((row = fields.getNext(sync)) != null)
		{
			if (!row.keySet().containsAll(fields.getKeys()))
				throw new AdapterException("Sort operation requires all keys [" + Misc.implode(fields.getKeys()) + "] while reading " + sync.getDescription() + ": " + Misc.implode(row));
			put(row);
		}
	}

	public void put(LinkedHashMap<String,String> row) throws Exception
	{
		String key = dbsync.getKey(row);
		if (key.length() == dbsync.getFields().getKeys().size()) return; // An empty key contains one ! per element

		if (sortedmap != null)
		{
			LinkedHashMap<String,Set<String>> prevmap = sortedmap.get(key);
			if (prevmap == null)
			{
				prevmap = new LinkedHashMap<String,Set<String>>();
				sortedmap.put(key,prevmap);
			}
			ReaderUtil.pushCurrent(row,prevmap,false);
			return;
		}

		XML xml = new XML();
		xml.add("row",row);

		String value = xml.rootToString();
		String sql = "insert into " + tablename + " values (" + DB.replacement + "," + DB.replacement + ")";

		ArrayList<String> list = new ArrayList<String>();
		list.add(key);
		list.add(value);

		db.execsql(conn,sql,list);
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();

		if (sortedmap != null)
		{
			if (iterator == null)
				iterator = sortedmap.keySet().iterator();
			if (!iterator.hasNext()) return null;
			String key = iterator.next();
			LinkedHashMap<String,Set<String>> map = sortedmap.get(key);
			for(String keyrow:map.keySet())
				row.put(keyrow,Misc.implode(map.get(keyrow),"\n"));

			if (Misc.isLog(15)) Misc.log("row [" + instance + "]: " + row);
			return row;
		}

		if (oper == null)
		{
			String sql = "select key,value from " + tablename + db.getOrderBy(conn,new String[]{"key"},dbsync.getIgnoreCaseKeys());
			oper = db.makesqloper(conn,sql);
		}

		LinkedHashMap<String,String> result = oper.next();
		if (result == null)
		{
			db.execsql(conn,"truncate table " + tablename);
			return null;
		}

		XML xml = new XML(new StringBuilder(result.get("VALUE")));
		XML[] elements = xml.getElements(null);

		for(XML el:elements)
		{
			String value = el.getValue();
			if (value == null) value = "";
			row.put(el.getTagName(),value);
		}

		if (Misc.isLog(15)) Misc.log("row [" + instance + "]: " + row);
		return row;
	}

	public Set<String> getHeader()
	{
		return header;
	}

	public String getName()
	{
		return instance;
	}

	public boolean isSorted()
	{
		return true;
	}

	public TreeMap<String,LinkedHashMap<String,Set<String>>> getSortedMap()
	{
		return sortedmap;
	}
}

class ReaderJMS extends ReaderUtil
{
	private AdapterExtendBase jmsbase;
	private String name;

	public ReaderJMS(XML xml) throws Exception
	{
		super(xml);

		jmsbase = JMS.getInstance().getInstance(xml);
		name = xml.getAttribute("name");
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		String text = jmsbase.read(name);
		if (text == null) return null;
		XML xml = text.startsWith("{") && text.endsWith("}") ? new XML(new org.json.JSONObject(text)) : new XML(new StringBuilder(text));
		xml = ProcessXML(getXML(),xml);
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		getSubXML(row,null,xml);
		return row;
	}

}
