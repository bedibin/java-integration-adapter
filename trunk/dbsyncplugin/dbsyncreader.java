import java.util.*;
import java.io.*;
import java.util.regex.*;

interface Reader
{
	public LinkedHashMap<String,String> next() throws Exception;
	public ArrayList<String> getHeader();
	public String getName();
}

class ReaderUtil
{
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
}

class ReaderRow implements Reader
{
	XML[] rows;
	int rowpos;
	private ArrayList<String> headers;

	public ReaderRow(XML xml) throws Exception
	{
		rowpos = 0;
		rows = xml.getElements("row");
		if (rows.length > 0)headers = new ArrayList<String>(rows[0].getAttributes().keySet());
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		if (rowpos >= rows.length) return null;
		LinkedHashMap<String,String> result = rows[rowpos].getAttributes();
		rowpos++;
		return result;
	}

	public String getName()
	{
		return "row";
	}
}

class ReaderCSV implements Reader
{
	private ArrayList<String> headers;
	private char enclosure;
	private char delimiter;
	private BufferedReader in;
	private String instance;

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
					result.add(sb.toString());
					sb = new StringBuffer();
					last = 0;
					continue;
				}

				sb.append(c);
				last = c;
			}
		} while(inquote);

		result.add(sb.toString());

		return result;
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		ArrayList<String> csv = readCSV();
		if (csv == null) return null;

		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		int size = csv.size();
		for(int i = 0;i < headers.size();i++)
		{
			if (i >= size)
				row.put(headers.get(i),"");
			else
				row.put(headers.get(i),XML.fixValue(csv.get(i).trim()));
		}

		if (Misc.isLog(15)) Misc.log("row [csv]: " + row);
		return row;
	}

	private void init(String name,String filename,char delimiter,char enclosure,String charset) throws Exception
	{
		if (filename == null)
			throw new AdapterException("Filename is mandatory");

		this.delimiter = delimiter == 0 ? ',' : delimiter;
		this.enclosure = enclosure == 0 ? '"' : enclosure;

		File file = new File(javaadapter.getCurrentDir(),filename);
		in = new BufferedReader(new InputStreamReader(new FileInputStream(file),charset == null ? "ISO-8859-1" : charset));
		headers = readCSV();
		if (headers == null)
			throw new AdapterException("CSV file " + filename + " requires at least one header line");

		instance = (name == null) ? file.getName() : name;
	}

	public ReaderCSV(XML xml) throws Exception
	{
		String name = xml.getAttribute("name");
		String filename = xml.getAttribute("filename");
		String delimiter = Misc.unescape(xml.getAttribute("delimiter"));
		String enclosure = Misc.unescape(xml.getAttribute("enclosure"));
		String charset = xml.getAttribute("charset");

		init(name,filename,delimiter == null ? 0 : delimiter.charAt(0),enclosure == null ? 0 : enclosure.charAt(0),charset);
	}

	public ReaderCSV(String name,String filename,char delimiter,char enclosure) throws Exception
	{
		init(name,filename,delimiter,enclosure,null);
	}

	public ReaderCSV(String filename) throws Exception
	{
		init(null,filename,(char)0,(char)0,null);
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return instance;
	}
}

class ReaderLDAP implements Reader
{
	private ArrayList<String> headers;
	private directory ld;
	private LinkedHashMap<String,String> first;
	private String instance;

	private void init(String name,String url,String context,String username,String password,String basedn,String search,String[] attrs,String[] sortattrs) throws Exception
	{
		instance = (name == null) ? "ldap" : name;

		Misc.log(5,"Searching for " + search + " on " + url + " base " + basedn);

		ld = context == null ? new ldap(url,username,password,sortattrs) : new directory(url,context,username,password);
		ld.search(basedn,search,attrs);

		if (attrs != null)
		{
			headers = new ArrayList<String>();
			for(String attr:attrs)
				headers.add(attr);
		}
		else
		{
			first = next();
			if (first == null)
				throw new AdapterException("Processing empty LDAP content is not supported. Please set \"fields \"attribute");

			headers = new ArrayList<String>(first.keySet());
		}
	}

	public ReaderLDAP(XML xml) throws Exception
	{
		String name = xml.getAttribute("name");
		String url = xml.getAttribute("url");
		String username = xml.getAttribute("username");
		String password = xml.getAttributeCrypt("password");
		String basedn = xml.getAttribute("basedn");
		String search = xml.getAttribute("query");
		String fields = xml.getAttribute("fields");
		String context = xml.getAttribute("context");
		String sortfields = xml.getAttribute("sort_fields");

		String[] attrs = fields == null ? null :  fields.split("\\s*,\\s*");
		String[] sortattrs = sortfields == null ? null :  sortfields.split("\\s*,\\s*");

		init(name,url,context,username,password,basedn,search,attrs,sortattrs);
	}

	public ReaderLDAP(String name,String url,String username,String password,String basedn,String search,String[] attrs,String[] sortattrs) throws Exception
	{
		init(name,url,null,username,password,basedn,search,attrs,sortattrs);
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row;

		if (first != null)
		{
			row = first;
			first = null;
			return row;
		}

		if (ld == null) return null;
		row = ld.searchNext();
		if (row == null)
		{
			ld.disconnect();
			return null;
		}

		if (Misc.isLog(15)) Misc.log("row [ldap]: " + row);
		return row;
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return instance;
	}
}

class ReaderSQL implements Reader
{
	private ArrayList<String> headers;
	private DB.DBOper oper = null;
	private LinkedHashMap<String,String> last;
	private Set<String> keyfields;
	private DB db;
	private String instance;
	private boolean issorted;
	private static final Pattern listPattern = Pattern.compile("LIST_.+-.+");

	private void init(String name,String conn,String sql,Set<String> keyfields,boolean issorted) throws Exception
	{
		db = DB.getInstance();
		this.keyfields = keyfields;
		String sqlsub = Misc.substitute(sql);
		oper = db.makesqloper(conn,sqlsub);
		headers = oper.getHeader();
		instance = (name == null) ? conn : name;
		this.issorted = issorted;
	}

	public ReaderSQL(XML xml) throws Exception
	{
		String name = xml.getAttribute("name");
		String conn = xml.getAttribute("instance");
		XML sqlxml = xml.getElement("extractsql");
		String sql = sqlxml == null ? xml.getValue() : sqlxml.getValue();
		String keyfield = xml.getAttribute("keyfield");
		String sorted = xml.getAttribute("sorted");

		init(name,conn,sql,keyfield == null ? null : Misc.arrayToSet(keyfield.split("\\s*,\\s*")),sorted != null && sorted.equals("true"));
	}

	public ReaderSQL(String conn,String sql) throws Exception
	{
		init(null,conn,sql,null,false);
	}

	public ReaderSQL(String conn,String sql,Set<String> keyfields,boolean issorted) throws Exception
	{
		init(null,conn,sql,keyfields,issorted);
	}

	public ReaderSQL(String name,String conn,String sql,Set<String> keyfields,boolean issorted) throws Exception
	{
		init(name,conn,sql,keyfields,issorted);
	}

	private void pushCurrent(LinkedHashMap<String,String> row,HashMap<String,List<String>> set)
	{
		for(String keyrow:row.keySet())
		{
			List<String> list = set.get(keyrow);
			if (list == null) list = new ArrayList<String>();

			String rowvalue = row.get(keyrow);
			rowvalue = XML.fixValue(rowvalue.replace("\r\n","\n"));
			if (issorted && listPattern.matcher(keyrow).matches())
				list.add(rowvalue);
			else if (rowvalue.length() != 0 && !list.contains(rowvalue))
				list.add(rowvalue);

			set.put(keyrow,list);
		}
	}

	private void sort(List<String> list)
	{
		Collections.sort(list,db.collator);
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		if (keyfields == null)
			return oper.next();

		LinkedHashMap<String,String> row;
		LinkedHashMap<String,String> current = last;
		HashMap<String,List<String>> currentlist = new HashMap<String,List<String>>();
		if (current != null) pushCurrent(current,currentlist);

		while((row = oper.next()) != null)
		{
			if (current == null)
			{
				current = row;
				pushCurrent(current,currentlist);
				continue;
			}

			boolean samekeys = true;
			for(String key:keyfields)
			{
				String value = current.get(key);
				if (value == null) throw new AdapterException("Key '" + key + "' missing in SQL statement");
				if (!value.equals(row.get(key)))
				{
					samekeys = false;
					break;
				}
			}

			if (!samekeys) break;

			pushCurrent(row,currentlist);
		}

		last = row;
		if (current == null) return null;

		for(String keyrow:current.keySet())
		{
			List<String> list = currentlist.get(keyrow);
			if (!issorted) sort(list);
			current.put(keyrow,Misc.implode(list,"\n"));
		}

		if (Misc.isLog(18)) Misc.log("row [sql]: " + current);

		return current;
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return instance;
	}
}

class ReaderXML implements Reader
{
	protected ArrayList<String> headers;
	protected XML[] xmltable;
	protected int position = 0;
	private String pathcol;
	protected String instance;

	private void getSubXML(LinkedHashMap<String,String> row,String prefix,XML xml) throws Exception
	{
		XML[] elements = xml.getElements(null);
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

	public ReaderXML()
	{
	}

	public ReaderXML(XML xml) throws Exception
	{
		init(null,xml,null,null,null);
	}

	public ReaderXML(String name,XML xml,String pathrow,String pathcol,String[] attrs) throws Exception
	{
		init(name,xml,pathrow,pathcol,attrs);
	}

	public ReaderXML(XML xml,XML xmlsource) throws Exception
	{
		String name = xml.getAttribute("name");
		String fields = xml.getAttribute("fields");
		String[] attrs = fields == null ? null :  fields.split("\\s*,\\s*");

		init(name,xmlsource,xml.getAttribute("resultpathrow"),xml.getAttribute("resultpathcolumn"),attrs);
	}

	private void init(String name,XML xml,String pathrow,String pathcol,String[] attrs) throws Exception
	{
		this.pathcol = pathcol;
		xmltable = xml.getElementsByPath(pathrow);
		instance = (name == null) ? xml.getTagName() : name;

		if (attrs != null)
		{
			headers = new ArrayList<String>();
			for(String attr:attrs)
				headers.add(attr);
			return;
		}

		LinkedHashMap<String,String> first = getXML(0);
		if (first == null)
			throw new AdapterException("Processing empty XML content is not supported. Please set \"fields\" attribute");

		headers = new ArrayList<String>(first.keySet());
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row = getXML(position);
		if (row == null) return null;

		position++;

		if (Misc.isLog(15)) Misc.log("row [xml]: " + row);
		return row;
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return instance;
	}
}
