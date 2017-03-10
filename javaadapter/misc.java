import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.regex.*;

class AdapterException extends Exception
{
	private static final long serialVersionUID = -6121704205129410513L;

	public AdapterException(String message)
	{
		super(message);
	}

	public AdapterException(Throwable cause)
	{
		super(cause);
	}

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
	public static final SimpleDateFormat dateformat = new SimpleDateFormat(DATEFORMAT);
	public static SimpleDateFormat gmtdateformat;
	public static final TimeZone gmttimezone = TimeZone.getTimeZone("UTC");
	public static final String CR = System.getProperty("line.separator");
	public static final Pattern substitutepattern = Pattern.compile("%([^%\n\\\\]*(?:\\\\.[^%\n\\\\]*)*)%");

	private static int loglevel = 1;
	private static boolean trace = false;
	private static HashSet<String> tracearray = new HashSet<String>();
	private static long maxsize = 0;
	private static String logcharset;
	private static String logfile;
	private static String hostname = System.getProperty("javaadapter.hostname");
	private static String logtag;

	private static String[] env;

	private Misc() {}

	static
	{
		gmtdateformat = new SimpleDateFormat(DATEFORMAT);
		gmtdateformat.setTimeZone(gmttimezone);
	}

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
		if (hostname == null || "".equals(hostname)) hostname = getHostName();

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

	static public String getGMTDate(java.util.Date date)
	{
		return gmtdateformat.format(date);
	}

	static public String getGMTDate()
	{
		return getGMTDate(new java.util.Date());
	}

	static public String getLocalDate(java.util.Date date)
	{
		return dateformat.format(date);
	}

	static public String getLocalDate()
	{
		return getLocalDate(new java.util.Date());
	}

	static public void setTrace(String name)
	{
		tracearray.add(name);
		trace = true;
	}

	static public void clearTrace(String name)
	{
		tracearray.remove(name);
		trace = !tracearray.isEmpty();
	}

	static public void setLogTag(String tag)
	{
		logtag = tag;
	}

	static public boolean isLog(int level)
	{
		return (level <= loglevel || trace);
	}

	static public void log(String message,Object... args)
	{
		log(1,message,args);
	}

	static public void log(int level,String message,Object... args)
	{
		if (level > loglevel && !trace) return;
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

		text = getLocalDate() + (trace ? " TRACE" : "") + threadinfo + taginfo + " " + text + CR;

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
		StringBuilder error = new StringBuilder("Current stack trace:" + CR);

		StackTraceElement el[] = thread.getStackTrace();
		for(int i = 1;i < el.length;i++)
			error.append("\t" + el[i].toString() + CR);

		log(1,error.toString());
	}

	static public void log(Throwable ex)
	{
		StringBuilder error = new StringBuilder();

		for(Throwable cause = ex;cause != null;cause = cause.getCause())
		{
			if (cause != ex) error.append(CR + "Caused by:" + CR);
			error.append(cause.toString() + CR);
			StackTraceElement el[] = cause.getStackTrace();
			for(int i = 0;i < el.length;i++)
				error.append("\t" + el[i].toString() + CR);
		}

		if (loglevel >= 20)
		{
			error.append(CR + "Catched by:" + CR);
			StackTraceElement el[] = Thread.currentThread().getStackTrace();
			for(int i = 1;i < el.length;i++)
				error.append("\t" + el[i].toString() + CR);
		}

		log(1,error.toString());
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
			StringBuilder text = new StringBuilder();

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
		StringBuilder out = new StringBuilder();
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			out.append(currentsep + string);
			currentsep = sep;
		}

		return out.toString();
	}

	static public String implode(String[] strings)
	{
		return implode(strings,",");
	}

	static public String implode(String[] strings,String sep)
	{
		StringBuilder out = new StringBuilder();
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			out.append(currentsep);
			out.append(string);
			currentsep = sep;
		}

		return out.toString();
	}

	static public String implode(Collection<String> strings)
	{
		return implode(strings,",");
	}

	static public String implode(Collection<String> strings,String sep)
	{
		StringBuilder sb = new StringBuilder();
		String currentsep = "";

		for(String string:strings)
		{
			if (string == null) continue;
			sb.append(currentsep);
			sb.append(string);
			currentsep = sep;
		}

		return sb.toString();
	}

	static public String implode(Map<String,String> map)
	{
		return implode(map,null,",");
	}

	static public String implode(Map<String,String> map,String sep)
	{
		return implode(map,null,sep);
	}

	static public String implode(Map<String,String> map,Set<String> keys)
	{
		return implode(map,keys,",");
	}

	static public String implode(Map<String,String> map,Set<String> keys,String sep)
	{
		StringBuilder out = new StringBuilder();
		String currentsep = "";

		if (keys == null) keys = map.keySet();
		for(String key:keys)
		{
			String value = map.get(key);
			if (value == null) value = "";

			out.append(currentsep + key + "=" + value);
			currentsep = sep;
		}

		return out.toString();
	}

	public static <T> Set<T> findDuplicates(Collection<T> list)
	{
		Set<T> duplicates = new HashSet<T>();
		Set<T> uniques = new HashSet<T>();

		for(T t:list)
			if(!uniques.add(t)) duplicates.add(t);

		return duplicates;
	}

	public static String toHexString(byte[] ba)
	{
		StringBuilder str = new StringBuilder();
		for(int i = 0; i < ba.length; i++)
			str.append(String.format("%x",ba[i]) + " ");
		return str.toString();
	}

	public static String toHexString(String text) throws Exception
	{
		return toHexString(text,"ISO-8859-1");
	}

	public static String toHexString(String text,String charset) throws Exception
	{
		StringBuilder str = new StringBuilder();
		for(char ch:text.toCharArray())
		{
			byte[] ba = (""+ch).getBytes(charset);
			for(int i = 0; i < ba.length; i++)
				str.append(String.format("%x",ba[i]));
			str.append(":" + ch + " ");
		}
		return str.toString();
	}

	public static boolean startsWith(byte[] source,byte[] match)
	{
		return startsWith(source,0,match);
	}

	public static boolean startsWith(byte[] source, int offset, byte[] match)
	{
		if (match == null || source == null) return false;

		if (match.length > (source.length - offset))
			return false;

		for (int i = 0; i < match.length; i++)
			if (source[offset + i] != match[i])
				return false;
		return true;
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

	static public XML checkActivate(XML xml) throws AdapterException
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

	static public boolean isFilterPass(XML xmlelement,XML xml) throws AdapterException
	{
		if (xmlelement == null) return true;

		String filter = xmlelement.getAttribute("filter");
		if (filter == null) return true;

		String filterresult = xmlelement.getAttribute("filter_result");
		boolean expectedresult = filterresult == null ? true : !filterresult.equals("false");
		boolean result = xml.matchXPath(filter);
		return result == expectedresult;
	}

	static public boolean isFilterPass(XML xmlelement,String value) throws AdapterException
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

	static public boolean isFilterPass(XML xmlelement,Map<String,String> map) throws AdapterException
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

	static public <T extends Exception> void rethrow(T ex,String message,Object... args) throws T
	{
		log(1,message,args);
		rethrow(ex);
	}

	static public <T extends Exception> void rethrow(T ex) throws T
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
		StringBuilder sbout = new StringBuilder();
		while((line = stdout.readLine()) != null)
			sbout.append(line);

		StringBuilder sberr = new StringBuilder();
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
		return strftime(str,date,null);
	}

	private static String formatdate(String format,Date date,TimeZone tz)
	{
		SimpleDateFormat df = new SimpleDateFormat(format);
		if (tz != null) df.setTimeZone(tz);
		return df.format(date);
	}

	public static String strftime(String str,Date date,TimeZone tz)
	{
		if (str == null) return null;

		StringBuilder out = new StringBuilder();
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
						out.append(formatdate("EEE",date,tz));
						break;
					case 'A':
						out.append(formatdate("EEEE",date,tz));
						break;
					case 'b':
						out.append(formatdate("MMM",date,tz));
						break;
					case 'B':
						out.append(formatdate("MMMM",date,tz));
						break;
					case 'd':
						out.append(formatdate("dd",date,tz));
						break;
					case 'D':
						out.append(formatdate("MM/dd/yy",date,tz));
						break;
					case 'F':
						out.append(formatdate("yyyy-MM-dd",date,tz));
						break;
					case 'H':
						out.append(formatdate("HH",date,tz));
						break;
					case 'm':
						out.append(formatdate("MM",date,tz));
						break;
					case 'M':
						out.append(formatdate("mm",date,tz));
						break;
					case 'p':
						out.append(formatdate("a",date,tz));
						break;
					case 'S':
						out.append(formatdate("ss",date,tz));
						break;
					case 'T':
						out.append(formatdate("HH:mm;ss",date,tz));
						break;
					case 'y':
						out.append(formatdate("yy",date,tz));
						break;
					case 'Y':
						out.append(formatdate("yyyy",date,tz));
						break;
					case 'Z':
						out.append(formatdate("z",date,tz));
						break;
					case 'z':
						out.append(formatdate("Z",date,tz));
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

		StringBuilder out = new StringBuilder();

		int sz = str.length();
		StringBuilder unicode = new StringBuilder(4);
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
		return new LinkedHashSet<String>(Arrays.asList(obj));
	}

	public static ArrayList<String> getKeyValueList(Set<String> keys,Map<String,String> map)
	{
		ArrayList<String> result = new ArrayList<String>();
		for(String key:keys)
		{
			String value = map.get(key);
			if (value == null || value.isEmpty()) continue;
			result.add(value);
		}

		return result;
	}

	public static String getKeyValue(Set<String> keys,Map<String,String> map)
	{
		ArrayList<String> result = getKeyValueList(keys,map);
		return result.size() == 0 ? null : implode(result);
	}

	public static String getStringValue(String text)
	{
		if (text == null) return "";
		return text;
	}

	public static String getMessage(String message,Object... args)
	{
		if (args.length == 0) return message;
		return String.format(message,args);
	}

	public static String getMessage(XML xml,String message,Object... args)
	{
		StringBuilder msg = new StringBuilder(getMessage(message,args));
		msg.append(" [XML ");
		String line = xml.getLine();
		if (line != null && !"".equals(line))
			msg.append("(line: " + line + ") ");
		try
		{
			BufferedReader br = new BufferedReader(new StringReader(xml.toString()));
			line = br.readLine(); // Skip first line <?xml...
			for(int i = 0;i < 3;i++)
			{
				line = br.readLine();
				if (line == null) break;
				msg.append(line.trim() + " ");
			}
		}
		catch(IOException ex) {}
		if (line != null) msg.append("...");
		msg.append("]");
		return msg.toString();
	}

	interface Substituer
	{
		String getValue(String value) throws Exception;
	}

	public static String substitute(String str,Substituer sub) throws Exception
	{
		return substitute(substitutepattern,str,sub);
	}

	private static String substituteEscape(String str)
	{
		if (str == null) return null;
		return str.replace("\\\\","\\").replace("\\%","%");
	}

	public static String substituteGet(String param,String def,VariableContext ctx) throws Exception
	{
		if (param.startsWith("$"))
			return XML.getDefaultVariable(param,ctx);
		else if (param.startsWith("<"))
		{
			String value = readFile(param.substring(1));
			return value == null ? value : value.trim();
		}
		else if (param.startsWith("!"))
		{
			String value = exec(param.substring(1),"ISO-8859-1",null);
			return value == null ? value : value.trim();
		}
		else if (param.startsWith("@@"))
		{
			String value = strftime(param.substring(2),new Date(),ctx == null ? gmttimezone : ctx.getTimeZone());
			return value;
		}
		else if (param.startsWith("@"))
		{
			String value = strftime(param.substring(1),new Date());
			return value;
		}
		else if (def != null)
			return def;
		return null;
	}

	public static boolean isSubstituteDefault(String str)
	{
		if (str == null) return false;

		Matcher matcher = substitutepattern.matcher(str);
		while(matcher.find())
		{
			char prefix = matcher.group(1).charAt(0);
			if (prefix != '@' && prefix != '$' && prefix != '!' && prefix != '<') return true;
		}

		return false;
	}

	public static String substitute(Pattern pattern,String str,Substituer sub) throws Exception
	{
		if (str == null) return null;

		Matcher matcher = pattern.matcher(str);
		StringBuilder sb = new StringBuilder();

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

	public static String substitute(String str,final VariableContext ctx) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return substituteGet(param,null,ctx);
			}
		});
	}

	public static String substitute(String str) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return substituteGet(param,null,null);
			}
		});
	}

	public static String substitute(String str,final Map<String,String> map) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				return substituteGet(param,map.get(param),null);
			}
		});
	}

	public static String substitute(String str,final XML xml) throws Exception
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				String def;
				try {
					def = xml.getStringByPath(param);
				} catch (javax.xml.xpath.XPathExpressionException ex) {
					def = null;
				}
				return substituteGet(param,def,null);
			}
		});
	}

	public static String getFirstValue(Map<String,String> map)
	{
		if (map == null) return null;
		Iterator<String> it = map.values().iterator();
		if (!it.hasNext()) return null;
		return it.next();
	}

	public static String trimLines(String str)
	{
		return trimLines(str,"\\s+");
	}

	public static String trimLines(String str,String trimexpr)
	{
		if (str == null) return null;
		return str.replaceAll(trimexpr + "\n","\n").replaceAll("\n" + trimexpr,"\n").replaceAll(trimexpr + "$|^" + trimexpr,"");
	}

	public static void exit(final int status, long maxDelayMillis)
	{
		System.out.print("[" + status + "] ");
		try {
			Timer timer = new Timer();
			timer.schedule(new TimerTask()
			{
				@Override
				public void run()
				{
					Runtime.getRuntime().halt(status);
				}
			},maxDelayMillis);
			System.exit(status);
      
		} catch (Throwable ex) {
			Runtime.getRuntime().halt(status);
		} finally {
			Runtime.getRuntime().halt(status);
		}
	}
}
