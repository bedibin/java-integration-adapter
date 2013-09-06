import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.regex.*;

class AdapterException extends Exception
{
	private static final long serialVersionUID = -6121704205129410513L;

	public AdapterException(String message,Object... args)
	{
		super(Misc.getMessage(message,args));
	}

	public AdapterException(XML xml,String message,Object... args)
	{
		super(Misc.getMessage(xml,message,args));
	}
}

class Rate
{
	private int counter;
	private long start_time;
	private double max;
	private NumberFormat formatter = new DecimalFormat("#0.0000");

	public Rate()
	{
		counter = 0;
		max = 0;
		start_time = (new Date()).getTime();
	}

	public String toString()
	{
		counter++;
		double delay = ((new Date()).getTime() - start_time) / 1000;
		if (delay < 1) delay = 1;
		double current_rate = counter / delay;
		if (current_rate > max) max = current_rate;
		return formatter.format(current_rate);
	}

	public String getMax()
	{
		return formatter.format(max);
	}

	public String getCount()
	{
		return (new Integer(counter)).toString();
	}
}

class Tuple
{
	private Object[] list = null;

	public Tuple(Object... list)
	{
		this.list = list;
	}

	public Object get(int idx)
	{
		return this.list[idx];
	}
}

class CsvWriter
{
	private Writer out;
	private Collection<String> headers;
	private char enclosure = '"';
	private char delimiter = ',';
	private boolean do_header = true;

	public CsvWriter(String filename) throws Exception
	{
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1"));
	}

	public CsvWriter(String filename,XML xml) throws Exception
	{
		setDelimiters(xml);
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1"));
	}

	public CsvWriter(String filename,Collection<String> headers) throws Exception
	{
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1"));
		this.headers = headers;
		write(headers);
	}

	public CsvWriter(String filename,Collection<String> headers,XML xml) throws Exception
	{
		setDelimiters(xml);
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1"));
		this.headers = headers;
		write(headers);
	}

	public CsvWriter(Writer writer,Collection<String> headers) throws Exception
	{
		out = writer;
		this.headers = headers;
		write(headers);
	}

	public CsvWriter(Writer writer) throws Exception
	{
		out = writer;
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

	private String escape(String value)
	{
		if (value == null) return "";
		value = value.replace("" + enclosure,"" + enclosure + enclosure);
		if (value.contains("" + delimiter) || value.contains("\n"))
			value = enclosure + value + enclosure;
		return value;
	}

	public void write(Collection<String> row) throws Exception
	{
		if (row == null)
		{
			out.close();
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
			out.close();
			return;
		}

		String line = null;
		if (headers == null)
		{
			headers = row.keySet();
			if (do_header) write(headers);
		}

		Iterator<String> it = headers.iterator();

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
		out.flush();
	}
}

class StreamGobbler implements Runnable
{
	private String name;
	private InputStream is;
	private Thread thread;
	private int loglevel;
	private String charset;

	public StreamGobbler(String name,InputStream is)
	{
		this.name = name;
		this.is = is;
		loglevel = 5;
	}

	public StreamGobbler(String name,InputStream is,String charset)
	{
		this.name = name;
		this.is = is;
		loglevel = 5;
		this.charset = charset;
	}

	public StreamGobbler(String name,InputStream is,int loglevel)
	{
		this.name = name;
		this.is = is;
		this.loglevel = loglevel;
	}

	public StreamGobbler(String name,InputStream is,String charset,int loglevel)
	{
		this.name = name;
		this.is = is;
		this.loglevel = loglevel;
		this.charset = charset;
	}

	public void start()
	{
		thread = new Thread(this);
		thread.start();
	}

	public void run()
	{
		try
		{
			InputStreamReader isr;
			if (charset == null)
				isr = new InputStreamReader(is);
			else
				isr = new InputStreamReader(is,charset);

			BufferedReader br = new BufferedReader(isr);

			while (true)
			{
				String s = br.readLine();
				if (s == null) break;
				if (Misc.isLog(loglevel)) Misc.log("[" + name + "] " + s);
			}

			is.close ();

		}
		catch (Exception ex)
		{
			 if (Misc.isLog(5)) Misc.log("Problem reading stream " + name + ":" + ex);
		}
	}
}

class Misc
{
	public static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
	public static SimpleDateFormat dateformat = new SimpleDateFormat(DATEFORMAT);
	public static final String CR = System.getProperty("line.separator");
	public static final Pattern substitutepattern = Pattern.compile("%([^%\n\\\\]*(?:\\\\.[^%\n\\\\]*)*)%");

	private static int loglevel = 1;
	private static long maxsize = 0;
	private static String logcharset;
	private static String logfile;
	private static String hostname = System.getProperty("javaadapter.hostname");
	private static String logtag;

	private static String[] env;

	private Misc() {}

	static public String getHostName()
	{
		try
		{
			String host = InetAddress.getLocalHost().getHostName();
			String[] hostpart = host.split("\\.");
			return hostpart[0];
		}
		catch (UnknownHostException e)
		{
			return "unknown";
		}
	}

	static private void setLogInfo(XML xmlconfig) throws Exception
	{
		XML logxml = xmlconfig.getElement("logfile");
		if (logxml == null) return;

		logfile = logxml.getValue();
		String level = logxml.getAttribute("level");
		if (level != null)
			loglevel = new Integer(level);

		String max = logxml.getAttribute("maxsize");
		if (max != null)
			maxsize = new Long(max);

		logcharset = logxml.getAttribute("charset");
	}

	static public void initXML(XML xmlconfig) throws Exception
	{
		if (hostname == null) hostname = getHostName();

		setLogInfo(xmlconfig);
		XML.setDefaults(xmlconfig);

		XML[] xmllist = xmlconfig.getElements("property");
		for(XML xml:xmllist)
		{
			String name = xml.getAttribute("name");
			if (name == null) continue;
			String value = xml.getValueCrypt();
			if (Misc.isLog(5)) Misc.log("Setting property " + name + " with value " + value);
			System.setProperty(name,value);
		}
	}

	static public String getDate(java.util.Date date)
	{
		return dateformat.format(date);
	}

	static public String getDate()
	{
		return getDate(new java.util.Date());
	}

	static public void setLogTag(String tag)
	{
		logtag = tag;
	}

	static public boolean isLog(int level)
	{
		return (level <= loglevel);
	}

	static public void log(String message,Object... args)
	{
		log(1,message,args);
	}

	static public void log(int level,String message,Object... args)
	{
		if (level > loglevel) return;
		if (message == null) return;

		String text = message;
		if (args.length != 0)
			text = String.format(message,args);

		text = text.trim();

		if (logfile == null || level == 0)
			System.out.println(text);

		if (logfile == null)
			return;

		String threadinfo = "";
		if (loglevel > 1)
			threadinfo = " [" + Thread.currentThread().getName() + "]";

		String taginfo = "";
		if (logtag != null)
			taginfo = " [" + logtag + "]";

		text = getDate() + threadinfo + taginfo + " " + text + CR;

		try
		{
			String filename = strftime(logfile,new Date());
			File file = new File(javaadapter.getCurrentDir(),filename);
			if (maxsize > 0 && file.length() > maxsize)
			{
				File newfile = new File(javaadapter.getCurrentDir(),filename + ".old");
				file.renameTo(newfile);
				file = new File(javaadapter.getCurrentDir(),filename);
			}

			FileOutputStream stream = new FileOutputStream(file,true);
			OutputStreamWriter out = logcharset == null ? new OutputStreamWriter(stream) : new OutputStreamWriter(stream,logcharset);
			out.write(text,0,text.length());
			out.close();
			stream.close();
		}
		catch(IOException ex)
		{
		}
	}

	static public void log(Thread thread)
	{
		String error = "Current stack trace:" + CR;

		StackTraceElement el[] = thread.getStackTrace();
		for(int i = 1;i < el.length;i++)
			error += "\t" + el[i].toString() + CR;

		log(1,error);
	}

	static public void log(Throwable ex)
	{
		String error = "";

		for(Throwable cause = ex;cause != null;cause = cause.getCause())
		{
			if (cause != ex) error += CR + "Caused by:" + CR;
			error += cause.toString() + CR;
			StackTraceElement el[] = cause.getStackTrace();
			for(int i = 0;i < el.length;i++)
				error += "\t" + el[i].toString() + CR;
		}

		if (loglevel >= 20)
		{
			error += CR + "Catched by:" + CR;
			StackTraceElement el[] = Thread.currentThread().getStackTrace();
			for(int i = 1;i < el.length;i++)
				error += "\t" + el[i].toString() + CR;
		}

		log(1,error);
	}

	static public void toFile(String filename,String text,String charset,boolean append) throws Exception
	{
		File file = new File(javaadapter.getCurrentDir(),filename);
		FileOutputStream stream = new FileOutputStream(file,append);
		OutputStreamWriter out = charset == null ? new OutputStreamWriter(stream) : new OutputStreamWriter(stream,charset);
		out.write(text,0,text.length());
		out.close();
		stream.close();
	}

	static public String readFile(String filename)
	{
		try
		{
			InputStreamReader in = new InputStreamReader(new FileInputStream(new File(javaadapter.getCurrentDir(),filename)),"ISO-8859-1");
			StringBuffer text = new StringBuffer();

			int ch = in.read();
			while (ch != -1)
			{
				text.append((char)ch);
				ch = in.read();
			}

			in.close();

			return text.toString();
		}
		catch(IOException ex)
		{
			return null;
		}
	}

	static public String implode(String sep,String... strings)
	{
		String out = "";
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			out = out + currentsep + string;
			currentsep = sep;
		}

		return out;
	}

	static public String implode(String[] strings)
	{
		return implode(strings,",");
	}

	static public String implode(String[] strings,String sep)
	{
		String out = "";
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			out += currentsep + string;
			currentsep = sep;
		}

		return out;
	}

	static public String implode(Collection<String> strings)
	{
		return implode(strings,",");
	}

	static public String implode(Collection<String> strings,String sep)
	{
		String out = "";
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			out += currentsep + string;
			currentsep = sep;
		}

		return out;
	}

	static public String implode(Map<String,String> map)
	{
		return implode(map,",");
	}

	static public String implode(Map<String,String> map,String sep)
	{
		String out = "";
		String currentsep = "";

		Set<String> keys = map.keySet();
		for(String key:keys)
		{
			String value = map.get(key);
			if (value == null) value = "";

			out += currentsep + key + "=" + value;
			currentsep = sep;
		}

		return out;
	}

	static public int indexOf(Object[] list,Object value)
	{
		for(int i = 0;i < list.length;i++)
		{
			if (list[i].equals(value)) return i;
		}
		return -1;
	}

	static public int indexOfIgnoreCase(String[] list,String value)
	{
		for(int i = 0;i < list.length;i++)
		{
			if (list[i].equalsIgnoreCase(value)) return i;
		}
		return -1;
	}

	static public boolean endsWithIgnoreCase(String str,String end)
	{
		int str_size = str.length();
		int end_size = end.length();
		if (end_size > str_size) return false;
		return str.substring(str_size - end_size).equalsIgnoreCase(end);
	}

	static public Object newObject(String object,Object... args) throws Exception
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		Class<?> cl = Class.forName(object);
		Constructor<?> ct = cl.getConstructor(types);
		return ct.newInstance(args);
	}

	static public Object invokeStatic(String object,String name,Object... args) throws Exception
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		Class<?> cl = Class.forName(object);
		Method method = cl.getDeclaredMethod(name,types);
		if (method == null)
			throw new NoSuchMethodException("Method " + name + " not found on object " + cl.getName());

		return method.invoke(null,args);
	}

	static public Object invoke(String object,String name,Object... args) throws Exception
	{
		return invoke(Class.forName(object).newInstance(),name,args);
	}

	static public Object invoke(Object object,String name,Object... args) throws Exception
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		Method method = getMethod(object,name,args);
		if (method == null)
			throw new NoSuchMethodException("Method " + name + " not found on object " + object.getClass().getName());

		return method.invoke(object,args);
	}

	static public Method getMethod(Object object,String name,Object... args)
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		try
		{
			Method method = object.getClass().getMethod(name,types);
			return method;
		}
		catch(NoSuchMethodException ex)
		{
			Method methods[] = object.getClass().getMethods();
			for(Method method:methods)
			{
				if (!name.equals(method.getName())) continue;
				if (!Modifier.isPublic(method.getModifiers())) continue;
				Class<?>[] params = method.getParameterTypes();
				if (params.length != args.length) continue;

				int j;
				for(j = 0;j < args.length;j++)
					if (!params[j].isInstance(args[j]))
						break;
				if (j != args.length) continue;

				if (Misc.isLog(10)) Misc.log("Method found: " + name + "," + object.getClass().getName() + "," + params[0].getName());
				return method;
			}
			return null;
		}
		catch(NoClassDefFoundError ex)
		{
			return null;
		}
	}

	static public XML checkActivate(XML xml) throws Exception
	{
		if (xml == null) return null;

		String activate = xml.getAttribute("activate");
		if (activate != null && activate.equals("no")) return null;

		String hostact = xml.getAttribute("activate_hostname");
		if (hostact == null) return xml;

		String[] hosts = hostact.split("\\s*,\\s*");
		boolean has_positive = false;

		for(String host:hosts)
		{
			if (host.startsWith("!"))
			{
				if (host.substring(1).equalsIgnoreCase(hostname)) return null;
			}
			else
			{
				if (host.equalsIgnoreCase(hostname)) return xml;
				has_positive = true;
			}
		}

		return has_positive ? null : xml;
	}

	static public boolean isFilterPass(XML xmlelement,XML xml) throws Exception
	{
		if (xmlelement == null) return true;

		String filter = xmlelement.getAttribute("filter");
		if (filter == null) return true;

		String filterresult = xmlelement.getAttribute("filter_result");
		boolean result = filterresult == null ? true : !filterresult.equals("false");
		XML[] xmlresults = xml.getElementsByPath(filter);
		if (result) return xmlresults.length > 0;

		return xmlresults.length == 0;
	}

	static public boolean isFilterPass(XML xmlelement,String value) throws Exception
	{
		if (xmlelement == null) return true;

		String filter = xmlelement.getAttribute("filter");
		if (filter == null) return true;

		String filterresult = xmlelement.getAttribute("filter_result");
		boolean result = filterresult == null ? true : !filterresult.equals("false");

		if (Misc.isLog(30)) Misc.log("isFilterPass checking " + filter + " against: " + value);
		Pattern pattern = Pattern.compile(filter);
		Matcher matcher = pattern.matcher(value);
		if (matcher.find() != result) return false;

		return true;
	}

	static public boolean isFilterPass(XML xmlelement,Map<String,String> map) throws Exception
	{
		return isFilterPass(xmlelement,implode(map));
	}

	static public ArrayList<Subscriber> initSubscribers(XML xml) throws Exception
	{
		ArrayList<Subscriber> sublist = new ArrayList<Subscriber>();

		XML[] classlist = xml.getElements("class");
		for(XML el:classlist)
		{
			String classname = el.getValue();
			Subscriber subscriber = new Subscriber(classname,el);
			sublist.add(subscriber);
		}

		XML[] functionlist = xml.getElements("function");
		for(XML function:functionlist)
		{
			Subscriber subscriber = new Subscriber(function);
			sublist.add(subscriber);
		}

		return sublist;
	}

	static public void initHooks(XML xml,ArrayList<Hook> hooklist) throws Exception
	{
		String interval = xml.getAttribute("interval");

		XML[] classlist = xml.getElements("class");
		for(XML el:classlist)
		{
			String classname = el.getValue();
			Hook hook = new Hook(classname,el);
			hook.setInterval(interval);
			hooklist.add(hook);
		}

		XML[] functionlist = xml.getElements("function");
		for(XML function:functionlist)
		{
			Hook hook = new Hook(function);
			hook.setInterval(interval);
			hooklist.add(hook);
		}
	}

	static public void activateSubscribers(ArrayList<Subscriber> sublist) throws Exception
	{
		for(int i = 0;i < sublist.size();i++)
		{
			// Instanciate external named class
			Subscriber sub = sublist.get(i);
			String classname = sub.getClassName();
			if (classname == null) continue;

			Subscriber newsub = (Subscriber)Class.forName(classname).newInstance();
			newsub.setOperation(sub);
			sublist.set(i,newsub);
		}

		javaadapter.setForShutdown(sublist);
	}

	static public void rethrow(Exception ex,String message,Object... args) throws Exception
	{
		log(1,message,args);
		rethrow(ex);
	}

	static public void rethrow(Exception ex) throws Exception
	{
		throw ex;
	}

	static public void activateHooks(ArrayList<Hook> hooklist) throws Exception
	{
		for(int i = 0;i < hooklist.size();i++)
		{
			// Instanciate external named class
			Hook hook = hooklist.get(i);
			String classname = hook.getClassName();
			if (classname == null) continue;

			Hook newhook = (Hook)Class.forName(classname).newInstance();
			newhook.setOperation(hook);
			hooklist.set(i,newhook);
		}

		for(Hook hook:hooklist)
			hook.start();

		javaadapter.setForShutdown(hooklist);
	}

	static public Process exec(String cmd) throws Exception
	{
		return exec(cmd,null);
	}

	static public String exec(String cmd,String charset,String input) throws Exception
	{
		Process process = Misc.execProcess(cmd);
		if (process == null) return null;

		BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream(),charset));
		BufferedReader stderr = new BufferedReader(new InputStreamReader(process.getErrorStream(),charset));

		if (input != null)
		{
			PrintWriter stdin = new PrintWriter(new OutputStreamWriter(process.getOutputStream(),charset));
			stdin.println(input);
			stdin.close();
		}

		String line;
		StringBuffer sbout = new StringBuffer();
		while((line = stdout.readLine()) != null)
			sbout.append(line);

		StringBuffer sberr = new StringBuffer();
		while((line = stderr.readLine()) != null)
			sberr.append(line);

		int exitval = process.waitFor();

		if (Misc.isLog(12)) Misc.log("[out:" + cmd + ":code=" + exitval + "] " + sbout);
		if (sberr.length() > 0 || exitval != 0)
		{
			Misc.log("ERROR: [" + cmd + "] " + sberr.toString());
			return null;
		}

		return(new String(sbout));
	}

	static public Process exec(String cmd,String charset) throws Exception
	{
		Process process = execProcess(cmd);
		if (process == null) return null;

		StreamGobbler out = new StreamGobbler("out:" + cmd,process.getInputStream(),charset);
		StreamGobbler err = new StreamGobbler("err:" + cmd,process.getErrorStream(),charset,1);
		out.start();
		err.start();

		return process;
	}

	static public Process execProcess(String cmd) throws Exception
	{
		if (cmd == null) return null;

		if (env == null)
		{
			Map<String,String> mapenv = System.getenv();
			ArrayList<String> list = new ArrayList<String>();

			for(Map.Entry<String,String> entry:mapenv.entrySet())
			{
				String key = entry.getKey();
				String value = entry.getValue();
				if (!key.equals("LD_PRELOAD"))
					list.add(key + "=" + value);
			}

			env = new String[list.size()];
			env = list.toArray(env);
		}

		if (Misc.isLog(9)) Misc.log("Executing: " + cmd);

		String[] cmds = {"/bin/sh","-c",cmd};
		String currentdir = javaadapter.getCurrentDir();
		Process process = Runtime.getRuntime().exec(cmds,env,currentdir == null ? null : new File(currentdir));

		return process;
	}

	public static String strftime(String str,Date date)
	{
		if (str == null) return null;

		Calendar cal = Calendar.getInstance();

		StringBuffer out = new StringBuffer();
		boolean inPercent = false;

		int sz = str.length();
		for (int i = 0; i < sz; i++)
		{
			char ch = str.charAt(i);

			if (inPercent)
			{
				inPercent = false;
				switch(ch)
				{
					case 'a':
						out.append(new SimpleDateFormat("EEE").format(date));
						break;
					case 'A':
						out.append(new SimpleDateFormat("EEEE").format(date));
						break;
					case 'b':
						out.append(new SimpleDateFormat("MMM").format(date));
						break;
					case 'B':
						out.append(new SimpleDateFormat("MMMM").format(date));
						break;
					case 'd':
						out.append(new SimpleDateFormat("dd").format(date));
						break;
					case 'D':
						out.append(new SimpleDateFormat("MM/dd/yy").format(date));
						break;
					case 'F':
						out.append(new SimpleDateFormat("yyyy-MM-dd").format(date));
						break;
					case 'H':
						out.append(new SimpleDateFormat("HH").format(date));
						break;
					case 'm':
						out.append(new SimpleDateFormat("MM").format(date));
						break;
					case 'M':
						out.append(new SimpleDateFormat("mm").format(date));
						break;
					case 'p':
						out.append(new SimpleDateFormat("a").format(date));
						break;
					case 'S':
						out.append(new SimpleDateFormat("ss").format(date));
						break;
					case 'T':
						out.append(new SimpleDateFormat("HH:mm;ss").format(date));
						break;
					case 'y':
						out.append(new SimpleDateFormat("yy").format(date));
						break;
					case 'Y':
						out.append(new SimpleDateFormat("yyyy").format(date));
						break;
					case 'Z':
						out.append(new SimpleDateFormat("z").format(date));
						break;
					case 'z':
						out.append(new SimpleDateFormat("Z").format(date));
						break;
					default:
						out.append(ch);
						break;
				}
				continue;
			}
			else
			{
				if (ch == '%')
				{
					inPercent = true;
					continue;
				}
			}
			out.append(ch);
		}

		if (inPercent)
			out.append('\\');

		return out.toString();
	}

	public static String unescape(String str)
	{
		if (str == null) return null;

		StringBuffer out = new StringBuffer();

		int sz = str.length();
		StringBuffer unicode = new StringBuffer(4);
		boolean hadSlash = false;
		boolean inUnicode = false;

		for (int i = 0; i < sz; i++)
		{
			char ch = str.charAt(i);
			if (inUnicode)
			{
				unicode.append(ch);
				if (unicode.length() == 4)
				{
					try
					{
						int value = Integer.parseInt(unicode.toString(), 16);
						out.append((char)value);
					}
					catch(Exception ex) { }
					unicode.setLength(0);
					inUnicode = false;
					hadSlash = false;
				}
				continue;
			}

			if (hadSlash)
			{
				hadSlash = false;
				switch(ch)
				{
					case '\\':
						out.append('\\');
						break;
					case '\'':
						out.append('\'');
						break;
					case '\"':
						out.append('"');
						break;
					case 'r':
						out.append('\r');
						break;
					case 'f':
						out.append('\f');
						break;
					case 't':
						out.append('\t');
						break;
					case 'n':
						out.append('\n');
						break;
					case 'b':
						out.append('\b');
						break;
					case 'u':
						inUnicode = true;
						break;
					default:
						out.append(ch);
						break;
				}
				continue;
			}
			else
			{
				if (ch == '\\')
				{
					hadSlash = true;
					continue;
				}
			}
			out.append(ch);
		}

		if (hadSlash)
			out.append('\\');

		return out.toString();
	}

	public static Set<String> arrayToSet(String[] obj)
	{
		if (obj == null) return null;
		return new HashSet<String>(Arrays.asList(obj));
	}

	public static String getKeyValue(Set<String> keys,Map<String,String> map)
	{
		String keyvalue = null;
		Iterator<String> itr = keys.iterator();
		while(itr.hasNext())
		{
			String itrvalue = map.get(itr.next());
			keyvalue = keyvalue == null ? itrvalue : keyvalue + "," + itrvalue;
		}
		return keyvalue;
	}

	public static String getMessage(String message,Object... args)
	{
		if (args.length == 0) return message;
		return String.format(message,args);
	}

	public static String getMessage(XML xml,String message,Object... args)
	{
		String msg = getMessage(message,args);
		msg += " [XML ";
		String line = xml.getLine();
		if (line != null && !"".equals(line))
			msg += "(line: " + line + ") ";
		try
		{
			BufferedReader br = new BufferedReader(new StringReader(xml.toString()));
			line = br.readLine(); // Skip first line <?xml...
			for(int i = 0;i < 3;i++)
			{
				line = br.readLine();
				if (line == null) break;
				msg += line.trim() + " ";
			}
		}
		catch(IOException ex) {}
		if (line != null) msg += "...";
		msg += "]";
		return msg;
	}

	interface Substituer
	{
		String getValue(String value) throws Exception;
	}

	public static String substitute(String str,Substituer sub) throws Exception
	{
		return substitute(substitutepattern,str,sub);
	}

	public static String substituteEscape(String str)
	{
		if (str == null) return null;
		return str.replace("\\\\","\\").replace("\\%","%");
	}

	public static String substitute(Pattern pattern,String str,Substituer sub) throws Exception
	{
		if (str == null) return null;

		Matcher matcher = pattern.matcher(str);
		StringBuffer sb = new StringBuffer();

		while(matcher.find())
		{
			StringBuffer tmpsb = new StringBuffer();
			matcher.appendReplacement(tmpsb,"");
			sb.append(unescape(tmpsb.toString()));

			String subpattern = matcher.group(1);
			if (subpattern.length() > 0)
			{
				String value = sub.getValue(unescape(subpattern));
				if (value == null) value = "";
				sb.append(value);
			}
		}

		StringBuffer tmpsb = new StringBuffer();
		matcher.appendTail(tmpsb);
		sb.append(unescape(tmpsb.toString()));

		return sb.toString();
	}

	public static String substitute(String str) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return XML.getDefaultVariable(param);
			}
		});
	}

	public static String substitute(String str,final Map<String,String> map) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return param.startsWith("$") ? XML.getDefaultVariable(param) : map.get(param);
			}
		});
	}

	public static String substitute(String str,final XML xml) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return param.startsWith("$") ? XML.getDefaultVariable(param) : xml.getStringByPath(param);
			}
		});
	}
}
