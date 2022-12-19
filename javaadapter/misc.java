import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.regex.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

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

class AdapterRuntimeException extends RuntimeException
{
	private static final long serialVersionUID = -6225744304199313533L;

	public AdapterRuntimeException(String message)
	{
		super(message);
	}

	public AdapterRuntimeException(Throwable cause)
	{
		super(cause);
	}

	public AdapterRuntimeException(String message,Object... args)
	{
		super(Misc.getMessage(message,args));
	}

	public AdapterRuntimeException(XML xml,String message,Object... args)
	{
		super(Misc.getMessage(xml,message,args));
	}
}

class AdapterNotFoundException extends AdapterException
{
	AdapterNotFoundException(String str)
	{
		super(str);
	}

	AdapterNotFoundException(Throwable ex)
	{
		super(ex);
	}
}

class Rate
{
	private int counter;
	private long startTime;
	private double max;
	private NumberFormat formatter = new DecimalFormat("#0.0000");

	public Rate()
	{
		counter = 0;
		max = 0;
		startTime = (new Date()).getTime();
	}

	public String toString()
	{
		counter++;
		long delay = ((new Date()).getTime() - startTime) / 1000;
		if (delay < 1) delay = 1;
		long currentRate = counter / delay;
		if (currentRate > max) max = currentRate;
		return formatter.format(currentRate);
	}

	public String getMax()
	{
		return formatter.format(max);
	}

	public String getCount()
	{
		return String.valueOf(counter);
	}
}

class Tuple<T>
{
	private T[] list = null;

	@SafeVarargs
	public Tuple(T... list)
	{
		this.list = list;
	}

	public T get(int idx)
	{
		return this.list[idx];
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Tuple)) return false;
		return Arrays.asList(list).equals(Arrays.asList(((Tuple)obj).list));
	}

	@Override
	public String toString()
	{
		return "(" + Misc.implode(list) + ")";
	}
}

class CacheManager<T>
{
	private class CacheResult<T>
	{
		long time;
		T result;
	}

	private HashMap<String, CacheResult<T>> cache = new HashMap<>();
	private long time = 15 * 60 * 1000; // Default 15 minutes

	CacheManager() {}
	CacheManager(long time)
	{
		this.time = time;
	}

	T get(String name)
	{
		CacheResult<T> current = cache.get(name);
		if (current == null) return null;
		if (System.currentTimeMillis() - time > current.time) return null;
		current.time = System.currentTimeMillis();
		return current.result;
	}

	void set(String name,T result)
	{
		CacheResult<T> current = cache.get(name);
		if (current == null) current = new CacheResult<T>();
		current.time = System.currentTimeMillis();
		current.result = result;
		cache.put(name,current);
	}
}

class StreamGobbler implements Runnable
{
	private String name;
	private InputStream is;
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
		Thread thread = new Thread(this);
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
		catch (IOException ex)
		{
			 if (Misc.isLog(5)) Misc.log("Problem reading stream " + name + ":" + ex);
		}
	}
}

class Misc
{
	public static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final TimeZone gmttimezone = TimeZone.getTimeZone("UTC");
	public static final String CR = System.getProperty("line.separator");

	// See regexp negative lookbehind:
	// https://stackoverflow.com/questions/2973436/regex-lookahead-lookbehind-and-atomic-groups
	public static final Pattern substitutepattern = Pattern.compile("(?<![^\\\\]\\\\)%(.*?(?<![^\\\\][\\n\\\\]))%");

	private static int loglevel = 1;
	private static boolean trace = false;
	private static HashSet<String> tracearray = new HashSet<>();
	private static long maxsize = 0;
	private static String loglevelprop = System.getProperty("javaadapter.log.level");
	private static String logmaxsizeprop = System.getProperty("javaadapter.log.maxsize");
	private static String logcharset = System.getProperty("javaadapter.log.charset");
	private static String logfile = System.getProperty("javaadapter.log.filename");
	private static String hostname = System.getProperty("javaadapter.hostname");
	private static String logtag;

	private static String[] env;

	private Misc() {}

	private static ThreadLocal<SimpleDateFormat> gmtDateFormat = new ThreadLocal<SimpleDateFormat>()
	{
		@Override
		protected SimpleDateFormat initialValue() {
			SimpleDateFormat gmtdateformat = new SimpleDateFormat(DATEFORMAT);
			gmtdateformat.setTimeZone(gmttimezone);
			return gmtdateformat;
		}
	};

	private static ThreadLocal<SimpleDateFormat> localDateFormat = new ThreadLocal<SimpleDateFormat>()
	{
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat(DATEFORMAT);
		}
	};

	public static SimpleDateFormat getLocalDateFormat() {
		return localDateFormat.get();
	}

	public static SimpleDateFormat getGmtDateFormat() {
		return gmtDateFormat.get();
	}

	public static String getHostName()
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

	private static void setLogInfo(XML xmlconfig) throws AdapterXmlException
	{
		XML logxml = xmlconfig.getElement("logfile");
		if (logxml != null)
		{
			if (logfile == null) logfile = logxml.getValue();
			if (logcharset == null) logcharset = logxml.getAttribute("charset");
			if (loglevelprop == null) loglevelprop = logxml.getAttribute("level");
			if (logmaxsizeprop == null) logmaxsizeprop = logxml.getAttribute("maxsize");
		}

		if (loglevelprop != null) loglevel = Integer.parseInt(loglevelprop);
		if (logmaxsizeprop != null) maxsize = Long.parseLong(logmaxsizeprop);
	}

	public static void setConfig(XML xmlconfig) throws AdapterXmlException
	{
		if (hostname == null || "".equals(hostname)) hostname = getHostName();

		xmlconfig.setInclude();
		setLogInfo(xmlconfig);
		XML.setDefaults(xmlconfig);

		XML[] xmllist = xmlconfig.getElements("property");
		for(XML xml:xmllist)
		{
			String name = xml.getAttribute("name");
			if (name == null) continue;
			String value = xml.getValueCrypt();
			if (Misc.isLog(5)) Misc.log("Setting property " + name + " with value " + value);
			System.setProperty(name,value == null ? "" : value);
		}
	}

	public static String getGMTDate(java.util.Date date)
	{
		return getGmtDateFormat().format(date);
	}

	public static String getGMTDate()
	{
		return getGMTDate(new java.util.Date());
	}

	public static String getLocalDate(Calendar calendar)
	{
		return getLocalDateFormat().format(calendar.getTime());
	}

	public static String getLocalDate(java.util.Date date)
	{
		return getLocalDateFormat().format(date);
	}

	public static String getLocalDate()
	{
		return getLocalDate(new java.util.Date());
	}

	public static void setTrace(String name)
	{
		tracearray.add(name);
		trace = true;
	}

	public static void clearTrace(String name)
	{
		tracearray.remove(name);
		trace = !tracearray.isEmpty();
	}

	public static void setLogTag(String tag)
	{
		logtag = tag;
	}

	public static boolean isLog(int level)
	{
		return (level <= loglevel || trace);
	}

	public static void log(String message,Object... args)
	{
		log(1,message,args);
	}

	public static void log(int level,String message,Object... args)
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

			try (
				FileOutputStream stream = new FileOutputStream(file,true);
				OutputStreamWriter out = logcharset == null ? new OutputStreamWriter(stream) : new OutputStreamWriter(stream,logcharset);
			) {
				out.write(text,0,text.length());
			}
		}
		catch(IOException ex)
		{
		}
	}

	public static void log(Thread thread)
	{
		StringBuilder error = new StringBuilder("Current stack trace:" + CR);

		StackTraceElement[] el = thread.getStackTrace();
		for(int i = 1;i < el.length;i++)
			error.append("\t" + el[i].toString() + CR);

		log(1,error.toString());
	}

	public static void log(Throwable ex)
	{
		StringBuilder error = new StringBuilder();

		for(Throwable cause = ex;cause != null;cause = cause.getCause())
		{
			if (cause != ex) error.append(CR + "Caused by:" + CR);
			error.append(cause.toString() + CR);
			StackTraceElement[] el = cause.getStackTrace();
			for(int i = 0;i < el.length;i++)
				error.append("\t" + el[i].toString() + CR);
		}

		if (loglevel >= 20)
		{
			error.append(CR + "Catched by:" + CR);
			StackTraceElement[] el = Thread.currentThread().getStackTrace();
			for(int i = 1;i < el.length;i++)
				error.append("\t" + el[i].toString() + CR);
		}

		log(1,error.toString());
	}

	public static void toFile(String filename,String text,String charset,boolean append) throws AdapterException
	{
		try {
			File file = new File(javaadapter.getCurrentDir(),filename);
			try (
				FileOutputStream stream = new FileOutputStream(file,append);
				OutputStreamWriter out = charset == null ? new OutputStreamWriter(stream) : new OutputStreamWriter(stream,charset);
			) {
				out.write(text,0,text.length());
			}
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public static BufferedReader getFileReader(File file,String defaultcharset) throws AdapterException
	{
		byte[] bom = new byte[4];

		try (FileInputStream fis = new FileInputStream(file)) {
			// Try to auto-detect character set. See: http://www.unicode.org/faq/utf_bom.html#bom1
			String charset = null;
			int bomsize = 0;

			int read = fis.read(bom);
			if (read >= 4 && bom[0] == (byte)0xFF && bom[1] == (byte)0xFE && bom[2] == (byte)0x00 && bom[3] == (byte)0x00)
			{
				charset = "UTF-32LE";
				bomsize = 4;
			}
			else if (read >= 4 && bom[0] == (byte)0x00 && bom[1] == (byte)0x00 && bom[2] == (byte)0xFE && bom[3] == (byte)0xFF)
			{
				charset = "UTF-32BE";
				bomsize = 4;
			}
			else if (read >= 3 && bom[0] == (byte)0xEF && bom[1] == (byte)0xBB && bom[2] == (byte)0xBF)
			{
				charset = "UTF-8";
				bomsize = 3;
			}
			else if (read >= 2 && bom[0] == (byte)0xFF && bom[1] == (byte)0xFE)
			{
				charset = "UTF-16LE";
				bomsize = 2;
			}
			else if (read >= 2 && bom[0] == (byte)0xFE && bom[1] == (byte)0xFF)
			{
				charset = "UTF-16BE";
				bomsize = 2;
			}

			if (charset != null && defaultcharset != null && !charset.equalsIgnoreCase(defaultcharset))
				throw new AdapterException("Detected charset " + charset + " for file '" + file.getName() + "' but " + defaultcharset + " is specified");

			if (charset == null) charset = defaultcharset;
			if (charset == null) charset = "ISO-8859-1";

			FileInputStream fisnobom = new FileInputStream(file);
			if (bomsize > 0)
			{
				long size = fisnobom.skip(bomsize);
				if (size < bomsize) throw new AdapterException("File '" + file.getName() + "' too small");
			}

			return new BufferedReader(new InputStreamReader(fisnobom,charset));
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public static CryptoBase getCipher(XML xml) throws AdapterException
	{
		String cipher = xml.getAttribute("filename_cipher");
		if (cipher == null) return null;

		CryptoBase crypto = Crypto.getCryptoInstance(cipher);
		if (crypto == null)
			throw new AdapterException(xml,"Cipher '" + cipher + "' not supported");
		return crypto;
	}

	public static String readFile(XML xml) throws AdapterException
	{
		String filename = xml.getAttribute("filename");
		if (filename == null)
			throw new AdapterException(xml,"Reading a file requires a 'filename' attribute");
		String charset = xml.getAttribute("filename_charset");
		if (charset == null) charset = xml.getAttribute("charset");
		String content = readFile(filename,charset);

		CryptoBase crypto = getCipher(xml);
		if (crypto == null) return content;
		return crypto.decrypt(content);
	}

	public static byte[] inputStreamToBytes(InputStream is) throws IOException
	{
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int count;
		byte[] data = new byte[16384];
		while ((count = is.read(data,0,data.length)) != -1)
			buffer.write(data,0,count);
		return buffer.toByteArray();
	}

	public static String readFile(String filename) throws AdapterException
	{
		return readFile(filename,null);
	}

	public static String readFile(String filename,String charset) throws AdapterException
	{
		return readFile(new File(javaadapter.getCurrentDir(),filename),charset);
	}

	public static String readFile(File file,String charset) throws AdapterException
	{
		try (BufferedReader reader = getFileReader(file,charset)) {
			return readFile(reader);
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public static String readFile(BufferedReader reader) throws AdapterException
	{
		try {
			StringBuilder text = new StringBuilder();

			int ch;
			while ((ch = reader.read()) != -1)
				text.append((char)ch);

			return text.toString();
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	public static String implode(String sep,Object... strings)
	{
		StringBuilder out = new StringBuilder();
		String currentsep = "";

		for(Object string:strings)
		{
			if (string == null) continue;
			out.append(currentsep + string.toString());
			currentsep = sep;
		}

		return out.toString();
	}

	public static String implode(Object[] strings)
	{
		return implode(strings,",");
	}

	public static String implode(Object[] strings,String sep)
	{
		StringBuilder out = new StringBuilder();
		String currentsep = "";

		for(Object string:strings)
		{
			if (string == null) continue;
			out.append(currentsep);
			out.append(string.toString());
			currentsep = sep;
		}

		return out.toString();
	}

	public static String implode(Iterator<String> strings)
	{
		return implode(strings,",");
	}

	public static String implode(Iterator<String> strings,String sep)
	{
		StringBuilder sb = new StringBuilder();
		String currentsep = "";

		while(strings.hasNext())
		{
			String string = strings.next();
			sb.append(currentsep);
			sb.append(string);
			currentsep = sep;
		}

		return sb.toString();
	}

	public static String implode(Iterable<String> strings)
	{
		return implode(strings,",");
	}

	public static String implode(Iterable<String> strings,String sep)
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

	public static String implode(Map<String,String> map)
	{
		return implode(map,null,",");
	}

	public static String implode(Map<String,String> map,String sep)
	{
		return implode(map,null,sep);
	}

	public static String implode(Map<String,String> map,Set<String> keys)
	{
		return implode(map,keys,",");
	}

	public static String implode(Map<String,String> map,Set<String> keys,String sep)
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
		Set<T> duplicates = new HashSet<>();
		Set<T> uniques = new HashSet<>();

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

	public static String toHexString(String text) throws UnsupportedEncodingException
	{
		return toHexString(text,"ISO-8859-1");
	}

	public static String toHexString(String text,String charset) throws UnsupportedEncodingException
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

	public static int indexOf(Object[] list,Object value)
	{
		for(int i = 0;i < list.length;i++)
		{
			if (list[i].equals(value)) return i;
		}
		return -1;
	}

	public static int indexOfIgnoreCase(String[] list,String value)
	{
		for(int i = 0;i < list.length;i++)
		{
			if (list[i].equalsIgnoreCase(value)) return i;
		}
		return -1;
	}

	public static boolean endsWithIgnoreCase(String str,String end)
	{
		int strsize = str.length();
		int endsize = end.length();
		if (endsize > strsize) return false;
		return str.substring(strsize - endsize).equalsIgnoreCase(end);
	}

	public static Object newObject(String object) throws AdapterException
	{
		try {
			Class<?> cl = Class.forName(object);
			return cl.getDeclaredConstructor().newInstance();
		} catch(InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException | ClassNotFoundException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Object newObject(String object,Object... args) throws AdapterException
	{
		try {
			Class<?>[] types = new Class[args.length];
			for(int i = 0;i < args.length;i++)
				types[i] = args[i].getClass();

			Class<?> cl = Class.forName(object);
			Constructor<?> ct = cl.getConstructor(types);
			return ct.newInstance(args);
		} catch(InvocationTargetException | IllegalAccessException | InstantiationException | NoSuchMethodException | ClassNotFoundException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Object invokeStatic(String object,String name,Object... args) throws AdapterException
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		try {
			Class<?> cl = Class.forName(object);
			Method method = cl.getDeclaredMethod(name,types);
			if (method == null)
				throw new NoSuchMethodException("Method " + name + " not found on object " + cl.getName());

			return method.invoke(null,args);
		} catch(InvocationTargetException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Object invoke(String object,String name,Object... args) throws AdapterException
	{
		try {
			return invoke(Class.forName(object).getDeclaredConstructor().newInstance(),name,args);
		} catch(InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException | ClassNotFoundException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Object invoke(Object object,String name,Object... args) throws AdapterException
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		try {
			Method method = getMethod(object,name,args);
			if (method == null)
				throw new NoSuchMethodException("Method " + name + " not found on object " + object.getClass().getName());

			return method.invoke(object,args);
		} catch(InvocationTargetException | IllegalAccessException | NoSuchMethodException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Class getClass(String name) throws AdapterException
	{
		try {
			return Class.forName(name);
		} catch(ClassNotFoundException ex) {
			throw new AdapterException(ex);
		}
	}

	public static Method getMethod(Object object,String name,Object... args)
	{
		Class<?>[] types = new Class[args.length];
		for(int i = 0;i < args.length;i++)
			types[i] = args[i].getClass();

		try
		{
			return object.getClass().getMethod(name,types);
		}
		catch(NoSuchMethodException ex)
		{
			Method[] methods = object.getClass().getMethods();
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

	public static XML checkActivate(XML xml) throws AdapterXmlException
	{
		if (xml == null) return null;

		String activate = xml.getAttribute("activate");
		if (activate != null && activate.equals("no")) return null;

		String hostact = xml.getAttribute("activate_hostname");
		if (hostact == null) return xml;

		String[] hosts = hostact.split("\\s*,\\s*");
		boolean haspositive = false;

		for(String host:hosts)
		{
			if (host.startsWith("!"))
			{
				if (host.substring(1).equalsIgnoreCase(hostname)) return null;
			}
			else
			{
				if (host.equalsIgnoreCase(hostname)) return xml;
				haspositive = true;
			}
		}

		return haspositive ? null : xml;
	}

	public static boolean isFilterPass(XML xmlelement,XML xml) throws AdapterXmlException
	{
		if (xmlelement == null) return true;

		String filter = xmlelement.getAttribute("filter");
		if (filter == null) return true;

		String filterresult = xmlelement.getAttribute("filter_result");
		boolean expectedresult = filterresult == null ? true : !filterresult.equals("false");
		boolean result = xml.matchXPath(filter);
		return result == expectedresult;
	}

	public static boolean isFilterPass(XML xmlelement,String value) throws AdapterXmlException
	{
		if (xmlelement == null) return true;

		String filter = xmlelement.getAttribute("filter");
		if (filter == null) return true;

		String filterresult = xmlelement.getAttribute("filter_result");
		boolean result = filterresult == null ? true : !filterresult.equals("false");

		if (Misc.isLog(30)) Misc.log("isFilterPass checking " + filter + " against: " + value);
		Pattern pattern = Pattern.compile(filter);
		Matcher matcher = pattern.matcher(value);

		return matcher.find() == result;
	}

	public static boolean isFilterPass(XML xmlelement,Map<String,String> map) throws AdapterXmlException
	{
		return isFilterPass(xmlelement,implode(map));
	}

	public static List<Subscriber> initSubscribers(XML xml) throws AdapterException
	{
		ArrayList<Subscriber> sublist = new ArrayList<>();

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

	public static void initHooks(XML xml,List<Hook> hooklist) throws AdapterException
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

	public static void activateSubscribers(List<Subscriber> sublist) throws AdapterException
	{
		for(int i = 0;i < sublist.size();i++)
		{
			// Instanciate external named class
			Subscriber sub = sublist.get(i);
			String classname = sub.getClassName();
			if (classname == null) continue;

			try {
				Subscriber newsub = (Subscriber)Class.forName(classname).getDeclaredConstructor().newInstance();
				newsub.setOperation(sub);
				sublist.set(i,newsub);
			} catch(InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException | ClassNotFoundException ex) {
				throw new AdapterException(ex);
			}
		}

		javaadapter.setForShutdown(sublist);
	}

	public static <T extends Exception> void rethrow(T ex,String message,Object... args) throws T
	{
		log(1,message,args);
		rethrow(ex);
	}

	public static <T extends Exception> void rethrow(T ex) throws T
	{
		throw ex;
	}

	public static void activateHooks(List<Hook> hooklist) throws AdapterException
	{
		for(int i = 0;i < hooklist.size();i++)
		{
			// Instanciate external named class
			Hook hook = hooklist.get(i);
			String classname = hook.getClassName();
			if (classname == null) continue;

			try {
				Hook newhook = (Hook)Class.forName(classname).getDeclaredConstructor().newInstance();
				newhook.setOperation(hook);
				hooklist.set(i,newhook);
			} catch(InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException | ClassNotFoundException ex) {
				throw new AdapterException(ex);
			}
		}

		for(Hook hook:hooklist)
			hook.start();

		javaadapter.setForShutdown(hooklist);
	}

	public static Process exec(String cmd) throws AdapterException
	{
		return exec(cmd,null);
	}

	public static String exec(String cmd,String charset,String input) throws IOException
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

		try {
			int exitval = process.waitFor();

			if (Misc.isLog(12)) Misc.log("[out:" + cmd + ":code=" + exitval + "] " + sbout);
			if (sberr.length() > 0 || exitval != 0)
			{
				Misc.log("ERROR: [" + cmd + "] " + sberr.toString());
				return null;
			}
		} catch(InterruptedException ex) {
			throw new IOException(ex);
		}

		return(new String(sbout));
	}

	public static Process exec(String cmd,String charset) throws AdapterException
	{
		try {
			Process process = execProcess(cmd);
			if (process == null) return null;

			StreamGobbler out = new StreamGobbler("out:" + cmd,process.getInputStream(),charset);
			StreamGobbler err = new StreamGobbler("err:" + cmd,process.getErrorStream(),charset,1);
			out.start();
			err.start();

			return process;
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}
	}

	private static Process execProcess(String cmd) throws IOException
	{
		if (cmd == null) return null;

		if (env == null)
		{
			Map<String,String> mapenv = System.getenv();
			ArrayList<String> list = new ArrayList<>();

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

		return Runtime.getRuntime().exec(cmds,env,currentdir == null ? null : new File(currentdir));
	}

	private static String formatdate(String format,Date date,TimeZone tz)
	{
		SimpleDateFormat df = new SimpleDateFormat(format);
		if (tz != null) df.setTimeZone(tz);
		return df.format(date);
	}

	public static String strftime(String str,Date date)
	{
		return strftime(str,date,null);
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
					catch(NumberFormatException ex) { }
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
		return new LinkedHashSet<>(Arrays.asList(obj));
	}

	public static List<String> getKeyValueList(Set<String> keys,Map<String,String> map)
	{
		ArrayList<String> result = new ArrayList<>();
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
		List<String> result = getKeyValueList(keys,map);
		return result.isEmpty() ? null : implode(result);
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
			BufferedReader br = new BufferedReader(new StringReader(xml.toStringNoDeclaration()));
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
		String getValue(String value) throws AdapterException;
	}

	public static String substitute(String str,Substituer sub) throws AdapterException
	{
		return substitute(substitutepattern,str,sub);
	}

	public static String substituteGet(String param,String def,VariableContext ctx) throws AdapterException
	{
		if (param.startsWith("$PASSWORD"))
			return javaadapter.crypter.decrypt(XML.getDefaultVariable(param,ctx));
		else if (param.startsWith("$"))
			return XML.getDefaultVariable(param,ctx);
		else if (param.startsWith("<"))
		{
			String value = readFile(param.substring(1));
			return value == null ? value : value.trim();
		}
		else if (param.startsWith("!") && param.endsWith("!"))
			return javaadapter.crypter.decrypt(param);
		else if (param.startsWith("!"))
		{
			try {
				String value = exec(param.substring(1),"ISO-8859-1",null);
				return value == null ? value : value.trim();
			} catch(IOException ex) {
				throw new AdapterException(ex);
			}
		}
		else if (param.startsWith("@@"))
			return strftime(param.substring(2),new Date(),ctx == null ? gmttimezone : ctx.getTimeZone());
		else if (param.startsWith("@"))
			return strftime(param.substring(1),new Date());
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

	public static String substitute(Pattern pattern,String str,Substituer sub) throws AdapterException
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

	public static String substitute(String str,final VariableContext ctx) throws AdapterException
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws AdapterException
			{
				return substituteGet(param,null,ctx);
			}
		});
	}

	public static String substitute(String str) throws AdapterException
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws AdapterException
			{
				return substituteGet(param,null,null);
			}
		});
	}

	public static String substitute(String str,final Map<String,String> map) throws AdapterException
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws AdapterException
			{
				return substituteGet(param,map.get(param),null);
			}
		});
	}

	public static String substitute(String str,final XML xml) throws AdapterException
	{
		return substitute(str,new Misc.Substituer() {
			public String getValue(String param) throws AdapterException
			{
				try {
					String value = xml.getStringByPath(param);
					if (value != null) return value;
				} catch (AdapterXmlPathException ex) {}
				return substituteGet(param,null,null);
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

	public static Set<Path> glob(String glob) throws AdapterException
	{
		return glob(glob,null);
	}

	public static Set<Path> glob(String glob,String dir) throws AdapterException
	{
		if (dir == null) dir = "";

		final TreeSet<Path> paths = new TreeSet<>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);

		try {
			Files.walkFileTree(Paths.get(dir),new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path,BasicFileAttributes attrs) throws IOException
				{
					if (matcher.matches(path)) paths.add(path);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path path,IOException exc) throws IOException
				{
					return FileVisitResult.CONTINUE;
				}
			});
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}

		if (paths.isEmpty())
		{
			File file = new File(glob,dir);
			if (file.exists())
				paths.add(file.toPath());
			else if (File.separatorChar == '\\') // Windows
			{
				file = new File(glob.replace(File.separator,"/"),dir.replace(File.separator,"/"));
				if (file.exists()) paths.add(file.toPath());
			}
		}

		if (Misc.isLog(30)) Misc.log("Glob for " + glob + ": " + paths);
		return paths;
	}

	public static boolean matches(String text,String glob)
	{
		String rest = null;
		int pos = glob.indexOf('*');
		if (pos != -1)
		{
			rest = glob.substring(pos + 1);
			glob = glob.substring(0, pos);
		}

		if (glob.length() > text.length())
			return false;

		// handle the part up to the first *
		for (int i = 0;i < glob.length();i++)
			if (glob.charAt(i) != '?' && !glob.substring(i, i + 1).equalsIgnoreCase(text.substring(i,i + 1)))
				return false;

		// recurse for the part after the first *, if any
		if (rest == null)
			return glob.length() == text.length();
		else
		{
			for (int i = glob.length();i <= text.length();i++)
			{
				if (matches(text.substring(i),rest))
					return true;
			}
			return false;
		}
	}

	public static void sleep(int delay) throws AdapterException
	{
		try {
			Thread.sleep(delay);
		} catch(InterruptedException ex) {
			throw new AdapterException(ex);
		}
	}

}
