import java.util.*;
import java.io.*;
import java.text.*;
import java.util.regex.*;
import java.nio.file.Path;

interface Reader extends Closeable
{
	public Map<String,String> next() throws AdapterException;
	public Set<String> getHeader();
	public String getName();
	public boolean isSorted();
	public boolean toTrim();
}

abstract class ReaderUtil implements Reader
{
	private XML xmlreader;
	private Set<String> keyfields;
	private boolean issorted = false;
	private boolean totrim = true;
	private Map<String,String> last;
	private OnOper onempty;
	private int rowcount;
	private String colname;
	private String pathcol;
	protected String pathrow;
	protected DB db;
	protected Set<String> headers;
	protected boolean skipnormalize = true;
	protected String instance;

	static final public Reader getReader(XML xml) throws AdapterException
	{
		return getReader(xml,xml);
	}

	static final public Reader getReader(XML xml,XML xmlsource) throws AdapterException
	{
		Reader reader = null;
		String type = xml.getAttribute("type");

		if (type == null)
			reader = new ReaderRow(xml);
		else if (type.equals("db"))
			reader = new ReaderSQL.Builder(xml).build();
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
			reader = (Reader)Misc.newObject(name,xml);
		}
		else
			throw new AdapterException(xml,"Unsupported reader type " + type);

		return reader;
	}

	public ReaderUtil() throws AdapterException
	{
 		db = DB.getInstance();
	}

	public final void setSorted(boolean issorted) { this.issorted = issorted; }
	public final void setTrim(boolean totrim) { this.totrim = totrim; }
	public final void setKeys(Set<String> keyfields) { this.keyfields = keyfields;; }

	public ReaderUtil setXML(XML xml) throws AdapterException
	{
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

		return this;
	}

	static final public void pushCurrent(Map<String,String> row,Map<String,Set<String>> map,boolean issorted) throws AdapterException
	{
		for(Map.Entry<String,String> entry:row.entrySet())
		{
			String keyrow = entry.getKey();
			Set<String> set = map.get(keyrow);
			if (set == null) set = issorted ? new LinkedHashSet<>() : new TreeSet<>(DB.getInstance().getCollatorIgnoreCase());

			String rowvalue = entry.getValue();
			if (!rowvalue.isEmpty()) // Do not insert an empty value
			{
				rowvalue = XML.fixValue(rowvalue.replace("\r\n","\n"));
				set.add(rowvalue);
			}
			map.put(keyrow,set);
		}
	}

	abstract public Map<String,String> nextRaw() throws AdapterException;

	private final void CheckEmpty() throws AdapterException
	{
		if (rowcount == 0 && onempty == OnOper.EXCEPTION)
			throw new AdapterException("Reader " + getName() + " didn't return any record");
	}

	public final Map<String,String> next() throws AdapterException
	{
		Map<String,String> row;
		if (keyfields == null)
		{
			row = nextRaw();
			if (row != null) rowcount++;
			CheckEmpty();
			return row;
		}

		Map<String,String> current = last;
		HashMap<String,Set<String>> currentmap = new HashMap<>();
		if (current != null) pushCurrent(current,currentmap,issorted);

		while((row = nextRaw()) != null)
		{
			rowcount++;
			for(String key:keyfields)
			{
				String value = row.get(key);
				if (value == null || value.isEmpty())
				{
					if (Misc.isLog(3)) Misc.log("WARNING: Keys '" + Misc.implode(keyfields) + "' missing in extraction. Cannot merge multiple records sharing the same key: " + Misc.implode(row.keySet()));
					keyfields = null;
					return row;
				}
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

	public final Map<String,String> normalizeFields(Map<String,String> row) throws AdapterException
	{
		if (row == null) throw new AdapterRuntimeException("Cannot normalize null entries");
		if (skipnormalize || headers == null) return row;

		LinkedHashMap<String,String> result = new LinkedHashMap<>();
		for(String name:headers)
		{
			String value = row.get(name);
			result.put(name,value == null ? "" : value);
		}
		return result;
	}

	public final void ReadXML(XML xml,XML xmlsource) throws AdapterException
	{
		Reader reader = ReaderUtil.getReader(xml,xmlsource);

		Map<String,String> row;

		XML xmltable = xmlsource.add(reader.getName());
		xml.copyAttributes(xmltable);

		while((row = reader.next()) != null)
			if (row.size() > 0)
				xmltable.add("row",row);
	}

	public final XML ProcessXML(XML xml,XML xmlsource) throws AdapterException
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

	public final void getSubXML(LinkedHashMap<String,String> row,String prefix,XML xml) throws AdapterException
	{
		Map<String,String> attributes = xml.getAttributes();
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
			String oldvalue = row.get(name);
			String newvalue = totrim ? value.trim() : value;
			row.put(name,oldvalue == null ? newvalue : oldvalue + "\n" + newvalue);
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

	public void close() throws IOException
	{
		// Nothing to close
	}
}

class ReaderRow extends ReaderUtil
{
	XML[] rows;
	int rowpos;
	private String default_name;

	public ReaderRow(XML xml) throws AdapterException
	{
		setXML(xml);

		if (instance == null) instance = "row";
		rowpos = 0;
		default_name = xml.getAttribute("default_field_name");

		if (headers == null) headers = new HashSet<>();
		if (default_name != null) headers.add(default_name);
		rows = xml.getElements("row");
		if (rows.length > 0) headers.addAll(rows[0].getAttributes().keySet());
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		if (rowpos >= rows.length) return null;
		Map<String,String> attributes = rows[rowpos].getAttributes();
		Map<String,String> result = normalizeFields(attributes);
		if (default_name != null && !attributes.containsKey(default_name))
		{
			String default_value = rows[rowpos].getValue();
			result.put(default_name,default_value == null ? "" : default_value);
		}

		rowpos++;
		return result;
	}
}

class ReaderCSV extends ReaderUtil implements Closeable
{
	private char enclosure = '"';
	private char delimiter = ',';
	private BufferedReader in;
	private Set<String> csvheaders;

	private void setValue(ArrayList<String> result,StringBuilder sb) throws AdapterException
	{
		String value = sb.toString();
		if (value.equals("\"")) value = "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2}"))
			value += " 00:00:00";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?"))
		{
			// Dates in CSV files are local
			try {
				Date date = Misc.getLocalDateFormat().parse(value);
				value = Misc.getGmtDateFormat().format(date);
			} catch(ParseException ex) {
				throw new AdapterException(ex);
			}
		}

		result.add(value);
	}

	public ArrayList<String> readCSV() throws AdapterException
	{
		if (in == null) return null;

		boolean inquote = false;
		ArrayList<String> result = new ArrayList<>();
		StringBuilder sb = new StringBuilder();

		try {
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
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}

		setValue(result,sb);

		return result;
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		ArrayList<String> csv = readCSV();
		if (csv == null) return null;

		LinkedHashMap<String,String> row = new LinkedHashMap<>();
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

	private void initFile(String filename,String charset,CryptoBase crypto) throws AdapterException
	{
		if (filename == null)
			throw new AdapterException("Filename is mandatory");

		Set<Path> paths = Misc.glob(filename);
		if (paths.size() != 1) throw new AdapterNotFoundException("File not found or multiple match: " + filename);
		File file = new File(paths.iterator().next().toString());
		initFile(file,charset,crypto);
	}

	private void initFile(File file,String charset,CryptoBase crypto) throws AdapterException
	{
		if (crypto == null)
			in = Misc.getFileReader(file,charset);
		else
		{
			String content = Misc.readFile(file,charset);
			String crypted_content = crypto.decrypt(content);
			in = new BufferedReader(new StringReader(crypted_content));
		}

		ArrayList<String> row = readCSV();
		if (row != null && row.size() > 0)
		{
			// Ignore last empty columns
			for(int x = row.size() - 1;x >= 0;x--)
				if (row.get(x).trim().isEmpty()) row.remove(x); else break;

			if (headers == null)
			{
				// Autodetect headers
				headers = csvheaders = new LinkedHashSet<>(row);
				if (row.size() != csvheaders.size())
					throw new AdapterException("Duplicated header values in CSV file " + file.getName() + ": " + Misc.findDuplicates(row));
			}
			else
			{
				ArrayList<String> commonheaders = new ArrayList<>(row);
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
					csvheaders = new LinkedHashSet<>(row);
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

	public ReaderCSV(java.io.Reader reader) throws AdapterException
	{
		in = new BufferedReader(reader);
		if (headers == null)
		{
			ArrayList<String> row = readCSV();
			if (row == null)
				throw new AdapterException("CSV reader requires at least one header line or \"fields\" attribute");
			headers = csvheaders = new LinkedHashSet<>(row);
			if (row.size() != headers.size())
				throw new AdapterException("Duplicated header values in CSV reader: " + Misc.findDuplicates(row));
		}

		if (instance == null) instance = reader.getClass().getName();
	}

	public ReaderCSV(XML xml) throws AdapterException
	{
		setXML(xml);

		String filename = xml.getAttribute("filename");

		String delimiter = Misc.unescape(xml.getAttribute("delimiter"));
		if (delimiter != null) this.delimiter = delimiter.charAt(0);
		String enclosure = Misc.unescape(xml.getAttribute("enclosure"));
		if (enclosure != null) this.enclosure = enclosure.charAt(0);

		String charset = xml.getAttribute("charset");
		CryptoBase crypto = Misc.getCipher(xml);
		initFile(filename,charset,crypto);
	}

	public ReaderCSV(String filename,char delimiter,char enclosure) throws AdapterException
	{
		if (delimiter != 0) this.delimiter = delimiter;
		if (enclosure != 0) this.enclosure = enclosure;
		initFile(filename,null,null);
	}

	public ReaderCSV(String filename) throws AdapterException
	{
		initFile(filename,null,null);
	}

	public ReaderCSV(File file,String charset) throws AdapterException
	{
		initFile(file,charset,null);
	}

	@Override
	public void close() throws IOException
	{
		if (in != null) in.close();
		in = null;
	}
}

class ReaderLDAP extends ReaderUtil
{
	private directory ld;
	private Map<String,String> first;

	private void init(String url,String context,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref,boolean notrust) throws AdapterException
	{
		if (instance == null) instance = url;

		Misc.log(5,"Searching for " + search + " on " + url + " base " + basedn);

		try {
			ld = context == null ? new ldap(url,username,password,sortattrs,auth,referral,deref,notrust) : new directory(url,context,username,password,auth);

			if (headers == null)
			{
				ld.search(basedn,search,null);
				first = next();
				if (first == null)
					throw new AdapterException("Processing empty LDAP content is not supported. Please set \"fields\" attribute");

				headers = new HashSet<>(first.keySet());
			}
			else
				ld.search(basedn,search,headers.toArray(new String[headers.size()]));
		} catch(javax.naming.NamingException | IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public ReaderLDAP(XML xml) throws AdapterException
	{
		setXML(xml);

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

	public ReaderLDAP(String url,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref,boolean notrust) throws AdapterException
	{
		init(url,null,username,password,basedn,search,sortattrs,auth,referral,deref,notrust);
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		Map<String,String> row;

		if (first == null)
		{
			try {
				if (ld == null) return null;
				row = ld.searchNext();
				if (row == null)
				{
					ld.disconnect();
					return null;
				}
			} catch(javax.naming.NamingException | IOException ex) {
				throw new AdapterException(ex);
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

	static class Builder
	{
		String conn;
		String sql;
		List<DBField> list;
		Set<String> keyfields;
		boolean issorted = false;
		boolean totrim = true;
		XML xml;

		Builder(String conn,String sql)
		{
			this.conn = conn;
			this.sql = sql;
		}

		Builder(XML xml) throws AdapterException
		{
			this.xml = xml;
			conn = xml.getAttribute("instance");
			XML sqlxml = xml.getElement("extractsql");
			sql = sqlxml == null ? xml.getValue() : sqlxml.getValue();
			XML endxml = xml.getElement("endingsql");
			if (endxml != null) sql = sql + " " + endxml.getValue();
		}

		ReaderSQL build() throws AdapterException
		{
 			DB db = DB.getInstance();
			String sqlsub = Misc.substitute(sql,db.getConnectionByName(conn));
			return new ReaderSQL(this);
		}

		Builder setDBFields(List<DBField> list) { this.list = list; return this; };
		Builder setTrim(boolean totrim) { this.totrim = totrim; return this; };
		Builder setSorted(boolean issorted) { this.issorted = issorted; return this; };
		Builder setKeys(Set<String> keyfields) { this.keyfields = keyfields; return this; };
	}

	private ReaderSQL(ReaderSQL.Builder builder) throws AdapterException
	{
		if (builder.xml == null)
		{
			setKeys(builder.keyfields);
			setSorted(builder.issorted);
			setTrim(builder.totrim);
		}
		else
			setXML(builder.xml);

		String sqlsub = Misc.substitute(builder.sql,db.getConnectionByName(builder.conn));
		oper = db.makesqloper(builder.conn,sqlsub,builder.list,toTrim());

		if (headers == null) headers = oper.getHeader();
		if (instance == null) instance = builder.conn;
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		Map<String,String> row = oper.next();
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

	protected LinkedHashMap<String,String> getXML(int pos) throws AdapterException
	{
		if (pos >= xmltable.length) return null;

		LinkedHashMap<String,String> row = new LinkedHashMap<>();
		getSubXML(row,null,xmltable[pos]);
		return row;
	}

	public ReaderXML() throws AdapterException
	{
	}

	public ReaderXML(XML xml) throws AdapterException
	{
		init(xml);
	}

	public ReaderXML(XML xml,XML xmlsource) throws AdapterException
	{
		setXML(xml);
		init(ProcessXML(xml,xmlsource));
	}

	private void init(XML xml) throws AdapterException
	{
		xmltable = xml.getElementsByPath(pathrow);
		if (Misc.isLog(5)) Misc.log("Found " + xmltable.length + " elements with path " + pathrow);
		if (instance == null) instance = xml.getTagName();

		if (headers != null) return;

		LinkedHashMap<String,String> first = getXML(0);
		if (first == null)
			throw new AdapterException("Processing empty XML content is not supported. Please set \"fields\" attribute");

		headers = new LinkedHashSet<>(first.keySet());
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		Map<String,String> row = getXML(position);
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
	private FileOutputStream fileoutput;

	public CacheReader(XML xml,Reader reader) throws AdapterException
	{
		javaadapter.setForShutdown(this);

		String cachedir = xml.getAttribute("cached_directory");
		String prefix = "javaadapter_" + reader.getName() + "_";
		String suffix = ".csv";

		try {
			if (cachedir == null)
				csvfile = File.createTempFile(prefix,suffix);
			else
			{
				File dir = new File(javaadapter.getCurrentDir(),cachedir);
				csvfile = File.createTempFile(prefix,suffix,dir);
			}
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}

		if (Misc.isLog(10)) Misc.log("Cached file is " + csvfile.getAbsolutePath());
		sourcereader = reader;

		try
		{
			fileoutput = new FileOutputStream(csvfile);
			Writer writer = new BufferedWriter(new OutputStreamWriter(fileoutput,"UTF8"));
			CsvWriter csv = new CsvWriter(writer,reader.getHeader());

			Map<String,String> row;
			while((row = reader.next()) != null)
				csv.write(row);

			csv.flush();
			csvreader = new ReaderCSV(csvfile,"UTF8");
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public Map<String,String> next() throws AdapterException
	{
		Map<String,String> result = csvreader.next();
		try {
			if (result == null) close();
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
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

	public boolean toTrim()
	{
		return sourcereader.toTrim();
	}

	@Override
	public void close() throws IOException
	{
		if (csvreader != null) csvreader.close();
		if (fileoutput != null) fileoutput.close();
		if (csvfile != null) csvfile.delete();
		sourcereader.close();
	}
}

class SortTable implements Reader
{
	private String tablename;
	private String conn;
	private DB db = DB.getInstance();
	private DBOper oper;
	private TreeMap<String,Map<String,Set<String>>> sortedmap;
	private Iterator<String> iterator;
	private String instance;
	private Set<String> header;
	private DBSyncOper dbsync;
	private boolean totrim;

	public SortTable(XML xml,Sync sync) throws AdapterException
	{
		dbsync = sync.getDBSync();
		Reader reader = sync.getReader();
		this.header = reader.getHeader();
		totrim = reader.toTrim();

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
			sortedmap = new TreeMap<>(dbsync.getIgnoreCaseKeys() ? db.getCollatorIgnoreCase() : db.getCollator());
			Misc.log(7,"Initializing memory sort");
		}
		else
		{
			instance = "dbsort/" + reader.getName();
			Misc.log(7,"Initializing temporary DB table sort");
		}

		put(sync);
	}

	public void put(Sync sync) throws AdapterException
	{
		Map<String,String> row;
		Fields fields = dbsync.getFields();
		boolean first = true;
		boolean allempty = true;

		while((row = fields.getNext(sync)) != null)
		{
			if (first && !row.keySet().containsAll(fields.getKeys()))
				throw new AdapterException("Sort operation requires all keys [" + Misc.implode(fields.getKeys()) + "] while reading " + sync.getDescription() + ": " + Misc.implode(row));
			boolean isempty = put(row);
			if (!isempty) allempty = false;
			first = false;
		}

		if (!first && allempty)
			throw new AdapterException("All records returned an empty key during sort operation while reading " + sync.getName() + ": " + Misc.implode(fields.getKeys()));
	}

	public boolean put(Map<String,String> row) throws AdapterException
	{
		String key = dbsync.getKey(row);
		if (key.length() == dbsync.getFields().getKeys().size()) return true; // An empty key contains one ! per element

		if (sortedmap != null)
		{
			Map<String,Set<String>> prevmap = sortedmap.get(key);
			if (prevmap == null)
			{
				prevmap = new LinkedHashMap<>();
				sortedmap.put(key,prevmap);
			}
			ReaderUtil.pushCurrent(row,prevmap,false);
			return false;
		}

		XML xml = new XML();
		xml.add("row",row);

		String value = xml.rootToString();
		String sql = "insert into " + tablename + " values (" + DB.replacement + "," + DB.replacement + ")";

		ArrayList<DBField> list = new ArrayList<>();
		list.add(new DBField(tablename,"key",key));
		list.add(new DBField(tablename,"value",value));

		db.execsql(conn,sql,list);

		return false;
	}

	public Map<String,String> next() throws AdapterException
	{
		Map<String,String> row = new LinkedHashMap<>();

		if (sortedmap != null)
		{
			if (iterator == null)
				iterator = sortedmap.keySet().iterator();
			if (!iterator.hasNext()) return null;
			String key = iterator.next();
			Map<String,Set<String>> map = sortedmap.get(key);
			for(Map.Entry<String,Set<String>> entry:map.entrySet())
				row.put(entry.getKey(),Misc.implode(entry.getValue(),"\n"));

			if (Misc.isLog(15)) Misc.log("row [" + instance + "]: " + row);
			return row;
		}

		if (oper == null)
		{
			String sql = "select key,value from " + tablename + db.getOrderBy(conn,new String[]{"key"},dbsync.getIgnoreCaseKeys());
			oper = db.makesqloper(conn,sql);
		}

		Map<String,String> result = oper.next();
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

	public boolean toTrim()
	{
		return totrim;
	}

	public TreeMap<String,Map<String,Set<String>>> getSortedMap()
	{
		return sortedmap;
	}

	public void close()
	{
		// Nothing to close
	}
}

class ReaderJMS extends ReaderUtil
{
	private JMSBase jmsbase;
	private String name;

	public ReaderJMS(XML xml) throws AdapterException
	{
		setXML(xml);

		jmsbase = (JMSBase)JMS.getInstance().getInstance(xml);
		name = xml.getAttribute("name");
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		String text = jmsbase.read(name);
		if (text == null) return null;
		XML xml = text.startsWith("{") && text.endsWith("}") ? new XML(new org.json.JSONObject(text)) : new XML(new StringBuilder(text));
		xml = ProcessXML(getXML(),xml);
		LinkedHashMap<String,String> row = new LinkedHashMap<>();
		getSubXML(row,null,xml);
		return row;
	}

}
