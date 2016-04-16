import java.util.*;
import java.io.*;
import java.text.*;
import java.util.regex.*;

interface Reader
{
	public LinkedHashMap<String,String> next() throws Exception;
	public Set<String> getHeader();
	public String getName();
}

class ReaderUtil implements Reader
{
	private Set<String> keyfields;
	private boolean issorted = false;
	private LinkedHashMap<String,String> last;
	protected DB db;
	protected Set<String> headers;
	protected boolean skipnormalize = true;
	protected String instance;

	static public Reader getReader(XML xml) throws Exception
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
			reader = new ReaderXML(xml,xml);
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

	public ReaderUtil(Set<String> keyfields,boolean issorted) throws Exception
	{
 		db = DB.getInstance();
		this.keyfields = keyfields;
		this.issorted = issorted;
	}

	public ReaderUtil(XML xml) throws Exception
	{
 		db = DB.getInstance();

		String tagname = xml.getTagName();
		if (!(tagname.equals("source") || tagname.equals("destination")))
			instance = xml.getAttribute("name");

		String keyfield = xml.getAttribute("keyfield");
		if (keyfield == null) xml.getAttribute("keyfields");
		if (keyfield == null) xml.getParent().getAttribute("keyfield");
		if (keyfield == null) xml.getParent().getAttribute("keyfields");

		String sorted = xml.getAttribute("sorted");
		if (keyfield != null) keyfields = Misc.arrayToSet(keyfield.split("\\s*,\\s*"));
		issorted = sorted != null && sorted.equals("true");

		String fields = xml.getAttribute("fields");
		if (fields != null)
		{
			headers = Misc.arrayToSet(fields.split("\\s*,\\s*"));
			skipnormalize = false;
		}
	}

	static public void pushCurrent(Map<String,String> row,Map<String,Set<String>> map,boolean issorted) throws Exception
	{
		for(String keyrow:row.keySet())
		{
			Set<String> set = map.get(keyrow);
			if (set == null) set = issorted ? new LinkedHashSet<String>() : new TreeSet<String>(DB.getInstance().collator);

			String rowvalue = row.get(keyrow);
			rowvalue = XML.fixValue(rowvalue.replace("\r\n","\n"));
			set.add(rowvalue);
			map.put(keyrow,set);
		}
	}

	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		throw new AdapterException("nextRaw() called from ReadUtil class. It must be overwritten");
	}

	public final LinkedHashMap<String,String> next() throws Exception
	{
		if (keyfields == null)
			return nextRaw();

		LinkedHashMap<String,String> row;
		LinkedHashMap<String,String> current = last;
		HashMap<String,Set<String>> currentmap = new HashMap<String,Set<String>>();
		if (current != null) pushCurrent(current,currentmap,issorted);

		while((row = nextRaw()) != null)
		{
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
		if (current == null) return null;
		for(String keyrow:current.keySet())
			current.put(keyrow,Misc.implode(currentmap.get(keyrow),"\n"));

		if (Misc.isLog(18)) Misc.log("row [merge/" + getName() + "]: " + current);

		return current;
	}

	public LinkedHashMap<String,String> normalizeFields(LinkedHashMap<String,String> row) throws Exception
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

	public Set<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return instance;
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

	private void setValue(ArrayList<String> result,StringBuffer sb) throws Exception
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
		StringBuffer sb = new StringBuffer();

		do
		{
			String line = in.readLine();
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
					sb = new StringBuffer();
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
		for(String name:headers)
		{
			String value = (i >= size) ? "" : XML.fixValue(csv.get(i).trim());
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

		File file = new File(javaadapter.getCurrentDir(),filename);
		in = new BufferedReader(new InputStreamReader(new FileInputStream(file),charset == null ? "ISO-8859-1" : charset));
		if (headers == null)
		{
			ArrayList<String> row = readCSV();
			if (row == null)
				throw new AdapterException("CSV file \"" + filename + "\" requires at least one header line or \"fields\" attribute must be specified");
			headers = new LinkedHashSet<String>(row);
			if (row.size() != headers.size())
				throw new AdapterException("Duplicated header values in CSV file " + filename + ": " + Misc.findDuplicates(row));
		}

		if (instance == null) instance = file.getName();
	}

	public ReaderCSV(java.io.Reader reader) throws Exception
	{
		in = new BufferedReader(reader);
		if (headers == null)
		{
			ArrayList<String> row = readCSV();
			headers = new LinkedHashSet<String>(row);
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
}

class ReaderLDAP extends ReaderUtil
{
	private directory ld;
	private LinkedHashMap<String,String> first;

	private void init(String url,String context,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref) throws Exception
	{
		if (instance == null) instance = url;

		Misc.log(5,"Searching for " + search + " on " + url + " base " + basedn);

		ld = context == null ? new ldap(url,username,password,sortattrs,auth,referral,deref) : new directory(url,context,username,password,auth);

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

		String url = xml.getAttribute("url");
		String username = xml.getAttribute("username");
		String password = xml.getAttributeCrypt("password");
		String auth = xml.getAttribute("authentication");
		String basedn = Misc.substitute(xml.getAttribute("basedn"));
		String search = Misc.substitute(xml.getAttribute("query"));
		String context = xml.getAttribute("context");
		String referral = xml.getAttribute("referral");
		String deref = xml.getAttribute("derefAliases");
		String sortfields = xml.getAttribute("sort_fields");

		String[] sortattrs = sortfields == null ? null :  sortfields.split("\\s*,\\s*");

		init(url,context,username,password,basedn,search,sortattrs,auth,referral,deref);
	}

	public ReaderLDAP(String url,String username,String password,String basedn,String search,String[] sortattrs,String auth,String referral,String deref) throws Exception
	{
		init(url,null,username,password,basedn,search,sortattrs,auth,referral,deref);
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
		oper = db.makesqloper(conn,sqlsub);

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

	public ReaderSQL(String conn,String sql,Set<String> keyfields,boolean issorted) throws Exception
	{
		super(keyfields,issorted);
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
	private String pathcol;

	private void getSubXML(LinkedHashMap<String,String> row,String prefix,XML xml) throws Exception
	{
		XML[] elements = xml.getElements();
		for(XML element:elements)
		{
			String name = element.getTagName();
			if (name == null) continue;
			name = prefix + "_" + name;
			if (headers != null && !headers.contains(name)) continue;
			String value = element.getValue();
			if (value == null) value = "";
			row.put(name,value.trim());
			getSubXML(row,name,element);
		}
	}

	protected LinkedHashMap<String,String> getXML(int pos) throws Exception
	{
		if (pos >= xmltable.length) return null;
		XML[] elements = xmltable[pos].getElementsByPath(pathcol);
		HashMap<String,String> attributes = xmltable[pos].getAttributes();

		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();

		for(Map.Entry<String,String> entry:attributes.entrySet())
		{
			String value = entry.getValue();
			if (value == null) value = "";
			row.put(entry.getKey(),value.trim());
		}

		for(XML element:elements)
		{
			String name = element.getTagName();
			if (name == null) continue;
			if (headers != null && !headers.contains(name)) continue;
			String value = element.getValue();
			if (value == null) value = "";
			row.put(name,value.trim());
			getSubXML(row,name,element);
		}

		return row;
	}

	public ReaderXML() throws Exception
	{
	}

	public ReaderXML(XML xml) throws Exception
	{
		init(xml,null,null);
	}

	public ReaderXML(XML xml,String pathrow,String pathcol) throws Exception
	{
		init(xml,pathrow,pathcol);
	}

	public ReaderXML(XML xml,XML xmlsource) throws Exception
	{
		super(xml);

		String filename = xml.getAttribute("filename");
		if (filename != null && (xmlsource == null || xml == xmlsource))
			xmlsource = new XML(filename);

		init(xmlsource,xml.getAttribute("resultpathrow"),xml.getAttribute("resultpathcolumn"));
	}

	private void init(XML xml,String pathrow,String pathcol) throws Exception
	{
		this.pathcol = pathcol;
		xmltable = xml.getElementsByPath(pathrow);
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
