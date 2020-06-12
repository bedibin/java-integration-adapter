import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.reflect.*;
import java.util.regex.*;

class CsvWriter
{
	class WriterOper
	{
		private boolean firstline = true;
		private Writer writer;

		WriterOper(Writer writer)
		{
			this.writer = writer;
		}

		void writeLine(String line) throws IOException
		{
			if (firstline)
				firstline = false;
			else
				writer.write(Misc.CR);
			writer.write(line);
		}

		void close() throws IOException
		{
			writer.close();
		}
	}

	private WriterOper defaultout;
	private HashMap<String,WriterOper> outlist;
	private String filename;
	private Collection<String> headers;
	private char enclosure = '"';
	private char delimiter = ',';
	private char list_delimiter = '\n';
	private boolean forceenclosure = false;
	private String charset = "ISO-8859-1";
	private boolean do_header = true;

	public CsvWriter(String filename,String charset) throws AdapterException
	{
		this.filename = filename;
		if (charset != null) this.charset = charset;
		outlist = new HashMap<String,WriterOper>();
		if (Misc.isSubstituteDefault(filename)) return;
		try {
			defaultout = new WriterOper(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),Misc.substitute(filename))),this.charset)));
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public CsvWriter(String filename) throws AdapterException
	{
		this(filename,(String)null);
	}

	public CsvWriter(String filename,XML xml) throws AdapterException
	{
		this(filename,xml.getAttribute("charset"));
		setDelimiters(xml);
	}

	public CsvWriter(String filename,String... headers) throws AdapterException
	{
		this(filename,Arrays.asList(headers));
	}

	public CsvWriter(String filename,Collection<String> headers) throws AdapterException
	{
		this(filename);
		this.headers = headers;
		if (defaultout != null) write(headers);
	}

	public CsvWriter(String filename,Collection<String> headers,XML xml) throws AdapterException
	{
		this(filename,xml.getAttribute("charset"));
		setDelimiters(xml);
		this.headers = headers;
		if (defaultout != null && headers != null && do_header) write(headers);
	}

	public CsvWriter(Writer writer,Collection<String> headers) throws AdapterException
	{
		outlist = new HashMap<String,WriterOper>();
		this.headers = headers;
		defaultout = new WriterOper(writer);
		write(headers);
	}

	public CsvWriter(Writer writer) throws AdapterException
	{
		outlist = new HashMap<String,WriterOper>();
		defaultout = new WriterOper(writer);
	}

	public void setDelimiters(XML xml) throws AdapterException
	{
		final String DELIMITER = "delimiter";
		final String ENCLOSURE = "enclosure";
		final String LIST_DELIMITER = "list_delimiter";

		if (xml.isAttribute(DELIMITER))
		{
			this.delimiter = 0;
			String delimiter = Misc.unescape(xml.getAttribute(DELIMITER));
			if (delimiter != null) this.delimiter = delimiter.charAt(0);
		}

		if (xml.isAttribute(ENCLOSURE))
		{
			this.enclosure = 0;
			String enclosure = Misc.unescape(xml.getAttribute(ENCLOSURE));
			if (enclosure != null) this.enclosure = enclosure.charAt(0);
		}

		if (xml.isAttribute(LIST_DELIMITER))
		{
			this.list_delimiter = 0;
			String list_delimiter = Misc.unescape(xml.getAttribute(LIST_DELIMITER));
			if (list_delimiter != null) this.list_delimiter = list_delimiter.charAt(0);
		}

		String forceenclosure = xml.getAttribute("force_enclosure");
		this.forceenclosure = forceenclosure != null && forceenclosure.equals("true");

		String header = xml.getAttribute("header");
		if (header != null && header.equals("false"))
			do_header = false;
	}

	public void setDelimiters(char enclosure,char delimiter,char list_delimiter)
	{
		this.enclosure = enclosure;
		this.delimiter = delimiter;
		this.list_delimiter = list_delimiter;
	}

	private String escape(String value) throws AdapterException
	{
		return escape(value,enclosure,delimiter,list_delimiter,forceenclosure);
	}

	public static String escape(String value,char enclosure,char delimiter,char list_delimiter,boolean forceenclosure) throws AdapterException
	{
		if (value == null) return "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
		{
			// Dates in CSV files are local
			try {
				Date date = Misc.gmtdateformat.parse(value);
				value = Misc.dateformat.format(date);
			} catch(ParseException ex) {
				throw new AdapterException(ex);
			}
		}

		if (enclosure != 0)
		{
			value = value.replace("" + enclosure,"" + enclosure + enclosure);
			if (forceenclosure || (value.contains("" + delimiter) || value.contains("" + enclosure) || value.contains("\n") || value.contains("\r")))
				value = enclosure + value + enclosure;
		}
		if (list_delimiter == 0) value = value.replace("\n","");
		else if (list_delimiter != '\n') value = value.replace('\n',list_delimiter);
		return value;
	}

	private WriterOper getOut(Map<String,String> row) throws AdapterException
	{
		if (headers == null)
		{
			headers = row.keySet();
			if (do_header && defaultout != null) write(defaultout,headers);
		}

		if (defaultout != null) return defaultout;
		if (row == null) throw new AdapterException("Cannot use CSV writer on collections with parameterized filename");

		String filename = Misc.substitute(this.filename,row);
		WriterOper out = outlist.get(filename);
		if (out == null)
		{
			try {
				out = new WriterOper(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1")));
				outlist.put(filename,out);
				if (do_header) write(out,headers);
			} catch(IOException ex) {
				throw new AdapterException(ex);
			}
		}

		return out;
	}

	public void write(String... row) throws AdapterException
	{
		write(getOut(null),Arrays.asList(row));
	}

	public void write(Collection<String> row) throws AdapterException
	{
		write(getOut(null),row);
	}

	private void write(WriterOper out,Collection<String> row) throws AdapterException
	{
		if (row == null)
		{
			flush();
			return;
		}

		StringBuilder line = new StringBuilder();
		boolean isempty = true;

		for(String value:row)
		{
			String entry = escape(value);
			if (!isempty && delimiter != 0) line.append(delimiter);
			line.append(entry);
			isempty = false;
		}

		if (isempty) return;

		try {
			out.writeLine(line.toString());
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public void write(Map<String,String> row) throws AdapterException
	{
		if (row == null)
		{
			flush();
			return;
		}

		WriterOper out = getOut(row);
		Iterator<String> it = headers.iterator();

		StringBuilder line = new StringBuilder();
		boolean isempty = true;

		while(it.hasNext())
		{
			String key = it.next();
			String entry = escape(row.get(key));
			if (!isempty && delimiter != 0) line.append(delimiter);
			line.append(entry);
			isempty = false;
		}

		if (isempty) return;

		try {
			out.writeLine(line.toString());
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public void flush() throws AdapterException
	{
		try {
			for(WriterOper out:outlist.values())
				out.close();
			if (defaultout != null) defaultout.close();
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}
}
