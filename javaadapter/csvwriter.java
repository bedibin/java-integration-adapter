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
		private Writer origwriter;

		WriterOper(Writer writer)
		{
			this.writer = writer;
			if (crypto == null) return;
			origwriter = writer;
			this.writer = new StringWriter();
		}

		void writeLine(String line) throws IOException
		{
			if (crafter)
			{
				writer.write(line);
				writer.write(Misc.CR);
			}
			else
			{
				if (firstline)
					firstline = false;
				else
					writer.write(Misc.CR);
				writer.write(line);
			}
		}

		void close() throws IOException
		{
			if (crypto != null)
			{
				String text = ((StringWriter)writer).toString();
				String crypt_text= crypto.encrypt(text);
				origwriter.write(crypt_text);
				origwriter.close();
			}

			writer.close();
		}
	}

	private WriterOper defaultout;
	private HashMap<String,WriterOper> outlist;
	private String filename;
	private Collection<String> headers;
	private char enclosure = '"';
	private char delimiter = ',';
	private char listdelimiter = '\n';
	private boolean forceenclosure = false;
	private String charset = "ISO-8859-1";
	private boolean doheader = true;
	private boolean crafter = false;
	private CryptoBase crypto;

	private void init(String filename,String charset) throws AdapterException
	{
		this.filename = filename;
		if (charset != null) this.charset = charset;
		outlist = new HashMap<>();
		if (Misc.isSubstituteDefault(filename)) return;
		try {
			defaultout = new WriterOper(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),Misc.substitute(filename))),this.charset)));
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public CsvWriter(String filename,String charset) throws AdapterException
	{
		init(filename,charset);
	}

	public CsvWriter(String filename) throws AdapterException
	{
		init(filename,(String)null);
	}

	public CsvWriter(String filename,XML xml) throws AdapterException
	{
		setDelimiters(xml);
		crypto = Misc.getCipher(xml);
		init(filename,xml.getAttribute("charset"));
	}

	public CsvWriter(String filename,String... headers) throws AdapterException
	{
		this.headers = Arrays.asList(headers);
		init(filename,(String)null);
		if (defaultout != null) write(headers);
	}

	public CsvWriter(String filename,Collection<String> headers) throws AdapterException
	{
		this.headers = headers;
		init(filename,(String)null);
		if (defaultout != null) write(headers);
	}

	public CsvWriter(String filename,Collection<String> headers,XML xml) throws AdapterException
	{
		setDelimiters(xml);
		crypto = Misc.getCipher(xml);
		init(filename,xml.getAttribute("charset"));
		this.headers = headers;
		if (defaultout != null && headers != null && doheader) write(headers);
	}

	public CsvWriter(Writer out,Collection<String> headers) throws AdapterException
	{
		outlist = new HashMap<>();
		this.headers = headers;
		defaultout = new WriterOper(out);
		write(headers);
	}

	public CsvWriter(Writer out)
	{
		outlist = new HashMap<>();
		defaultout = new WriterOper(out);
	}

	void setDelimiters(XML xml) throws AdapterException
	{
		final String DELIMITER = "delimiter";
		final String ENCLOSURE = "enclosure";
		final String LIST_DELIMITER = "list_delimiter";

		if (xml.isAttribute(DELIMITER))
		{
			this.delimiter = 0;
			String delimiterattr = Misc.unescape(xml.getAttribute(DELIMITER));
			if (delimiterattr != null) delimiter = delimiterattr.charAt(0);
		}

		if (xml.isAttribute(ENCLOSURE))
		{
			this.enclosure = 0;
			String enclosureattr = Misc.unescape(xml.getAttribute(ENCLOSURE));
			if (enclosureattr != null) enclosure = enclosureattr.charAt(0);
		}

		if (xml.isAttribute(LIST_DELIMITER))
		{
			this.listdelimiter = 0;
			String listdelimiterattr = Misc.unescape(xml.getAttribute(LIST_DELIMITER));
			if (listdelimiterattr != null) listdelimiter = listdelimiterattr.charAt(0);
		}

		String forceenclosureattr = xml.getAttribute("force_enclosure");
		forceenclosure = forceenclosureattr != null && forceenclosureattr.equals("true");

		String header = xml.getAttribute("header");
		if (header != null && header.equals("false"))
			doheader = false;

		String newline = xml.getAttribute("newline_suffix");
		if (newline != null && newline.equals("true"))
			crafter = true;
	}

	void setDelimiters(char enclosure,char delimiter,char listdelimiter)
	{
		this.enclosure = enclosure;
		this.delimiter = delimiter;
		this.listdelimiter = listdelimiter;
	}

	String escape(String value) throws AdapterException
	{
		return escape(value,enclosure,delimiter,listdelimiter,forceenclosure);
	}

	public static String escape(String value,char enclosure,char delimiter,char listdelimiter,boolean forceenclosure) throws AdapterException
	{
		if (value == null) return "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
		{
			// Dates in CSV files are local
			try {
				Date date = Misc.getGmtDateFormat().parse(value);
				value = Misc.getLocalDateFormat().format(date);
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
		if (listdelimiter == 0) value = value.replace("\n","");
		else if (listdelimiter != '\n') value = value.replace('\n',listdelimiter);
		return value;
	}

	private WriterOper getOut(Map<String,String> row) throws AdapterException
	{
		if (headers == null && row != null)
		{
			headers = row.keySet();
			if (doheader && defaultout != null) write(defaultout,headers);
		}

		if (defaultout != null) return defaultout;
		if (row == null) throw new AdapterException("Cannot use CSV writer on collections with parameterized filename");

		String filenameattr = Misc.substitute(filename,row);
		WriterOper out = outlist.get(filenameattr);
		if (out == null)
		{
			try {
				out = new WriterOper(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filenameattr)),charset)));
				outlist.put(filenameattr,out);
				if (doheader) write(out,headers);
			} catch(IOException ex) {
				throw new AdapterException(ex);
			}
		}

		return out;
	}

	public void write(String... row) throws AdapterException
	{
		WriterOper out = getOut(null);
		write(out,Arrays.asList(row));
	}

	public void write(Collection<String> row) throws AdapterException
	{
		WriterOper out = getOut(null);
		write(out,row);
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
