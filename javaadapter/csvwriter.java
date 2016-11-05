import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.reflect.*;
import java.util.regex.*;

class CsvWriter
{
	private Writer defaultout;
	private HashMap<String,Writer> outlist;
	private String filename;
	private Collection<String> headers;
	private char enclosure = '"';
	private char delimiter = ',';
	private String charset = "ISO-8859-1";
	private boolean do_header = true;

	public CsvWriter(String filename,String charset) throws Exception
	{
		this.filename = filename;
		if (charset != null) this.charset = charset;
		outlist = new HashMap<String,Writer>();
		if (Misc.isSubstituteDefault(filename)) return;
		defaultout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),this.charset));
	}

	public CsvWriter(String filename) throws Exception
	{
		this(filename,(String)null);
	}

	public CsvWriter(String filename,XML xml) throws Exception
	{
		this(filename,xml.getAttribute("charset"));
		setDelimiters(xml);
	}

	public CsvWriter(String filename,Collection<String> headers) throws Exception
	{
		this(filename);
		this.headers = headers;
		if (defaultout != null) write(headers);
	}

	public CsvWriter(String filename,Collection<String> headers,XML xml) throws Exception
	{
		this(filename,xml.getAttribute("charset"));
		setDelimiters(xml);
		this.headers = headers;
		if (defaultout != null) write(headers);
	}

	public CsvWriter(Writer writer,Collection<String> headers) throws Exception
	{
		outlist = new HashMap<String,Writer>();
		this.headers = headers;
		defaultout = writer;
		write(headers);
	}

	public CsvWriter(Writer writer) throws Exception
	{
		outlist = new HashMap<String,Writer>();
		defaultout = writer;
	}

	public void setDelimiters(XML xml) throws Exception
	{
		String delimiter = Misc.unescape(xml.getAttribute("delimiter"));
		if (delimiter != null) this.delimiter = delimiter.charAt(0);

		String enclosure = Misc.unescape(xml.getAttribute("enclosure"));
		if (enclosure != null) this.enclosure = enclosure.charAt(0);

		String header = xml.getAttribute("header");
		if (header != null && header.equals("false"))
			do_header = false;
	}

	public void setDelimiters(char enclosure,char delimiter)
	{
		this.enclosure = enclosure;
		this.delimiter = delimiter;
	}

	private String escape(String value) throws Exception
	{
		if (value == null) return "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
		{
			// Dates in CSV files are local
			Date date = Misc.gmtdateformat.parse(value);
			value = Misc.dateformat.format(date);
		}

		value = value.replace("" + enclosure,"" + enclosure + enclosure);
		if (value.contains("" + delimiter) || value.contains("\n") || value.contains("\""))
			value = enclosure + value + enclosure;
		return value;
	}

	private Writer getOut(Map<String,String> row) throws Exception
	{
		if (headers == null)
		{
			headers = row.keySet();
			if (do_header && defaultout != null) write(defaultout,headers);
		}

		if (defaultout != null) return defaultout;
		if (row == null) throw new AdapterException("Cannot use CSV writer on collections with parameterized filename");

		String filename = Misc.substitute(this.filename,row);
		Writer out = outlist.get(filename);
		if (out == null)
		{
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1"));
			outlist.put(filename,out);
			if (do_header) write(out,headers);
		}

		return out;
	}

	public void write(Collection<String> row) throws Exception
	{
		write(getOut(null),row);
	}

	private void write(Writer out,Collection<String> row) throws Exception
	{
		if (row == null)
		{
			flush();
			return;
		}

		String line = null;

		for(String value:row)
		{
			String entry = escape(value);
			if (line == null)
				line = entry;
			else
				line += delimiter + entry;
		}

		if (line == null) return;
		line += Misc.CR;

		out.write(line,0,line.length());
	}

	public void write(Map<String,String> row) throws Exception
	{
		if (row == null)
		{
			flush();
			return;
		}

		Writer out = getOut(row);
		Iterator<String> it = headers.iterator();

		String line = null;
		while(it.hasNext())
		{
			String key = it.next();
			String entry = escape(row.get(key));
			if (line == null)
				line = entry;
			else
				line += delimiter + entry;
		}

		if (line == null) return;
		line += Misc.CR;

		out.write(line,0,line.length());
	}

	public void flush() throws Exception
	{
		for(Map.Entry<String,Writer> entry:outlist.entrySet())
		{
			Writer out = entry.getValue();
			out.close();
		}
		if (defaultout != null) defaultout.close();
	}
}
