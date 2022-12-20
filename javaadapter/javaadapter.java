import java.util.*;
import java.io.File;

class Shutdown extends Thread
{
	private ArrayList<Object> closelist = new ArrayList<>();
	private boolean shuttingdown = false;

	public boolean isShuttingDown()
	{
		return shuttingdown;
	}

	public void setShuttingDown(boolean shuttingdown)
	{
		this.shuttingdown = shuttingdown;
	}

	@Override
	public synchronized void run()
	{
		shuttingdown = true;
		javaadapter.isstarted = false;

		System.out.print("Shutting down... ");
		for(int i = closelist.size() - 1;i >= 0;i--)
		{
			Object object = closelist.get(i);
			System.out.print("[" + object.getClass().getName() + "] ");
			try
			{
				Misc.invoke(object,"close");
			}
			catch(Exception ex)
			{
			}

			try
			{
				Misc.invoke(object,"closesuper");
			}
			catch(Exception ex)
			{
			}
		}

		Misc.log(0,"Shutdown completed");
	}

	public void setCloseList(List<?> list)
	{
		closelist.addAll(list);
	}

	public void setCloseList(Object object)
	{
		closelist.add(object);
	}
}

class AdapterExtend
{
	private Map<String,AdapterExtendBase> extendlist = new LinkedHashMap<>();
	private static final String DEFAULTNAME = "default";
	protected String defaultclass;

	protected AdapterExtend() {};

	public Map<String,AdapterExtendBase> getInstances()
	{
		return extendlist;
	}

	public AdapterExtendBase getInstance(String name) throws AdapterException
	{
		AdapterExtendBase instance = extendlist.get(name);
		if (instance == null) throw new AdapterException("Adapter " + getClass().getName() + " " + name + " not found");
		return instance;
	}

	public AdapterExtendBase getInstance(XML xml) throws AdapterException
	{
		String instance = xml.getAttribute("instance");
		String type = xml.getAttribute("type");
		for(Map.Entry<String,AdapterExtendBase> entry:extendlist.entrySet())
		{
			String key = entry.getKey();
			AdapterExtendBase ctx = entry.getValue();
			if (key.equals(instance)) return ctx;
			if (key.equals(getClass().getName())) return ctx;
			if (type != null && type.equals(ctx.getSupportedType())) return ctx;
		}
		throw new AdapterException(xml,"Cannot find instance for class " + getClass().getName());
	}

	public void setInstance(XML xml) throws AdapterException
	{
		String classname = xml.getValue("class",null);
		if (classname == null) classname = xml.getAttribute("class");
		if (classname == null) classname = defaultclass;

		AdapterExtendBase ctx = (AdapterExtendBase)Misc.newObject(classname,xml);
		ctx.setXML(xml);

		String name = xml.getAttribute("name");
		if (name == null) name = classname;
		extendlist.put(name,ctx);

	}

	public void setInstance(AdapterExtendBase ctx)
	{
		extendlist.put(ctx.getClass().getName(),ctx);
	}
}

class AdapterExtendBase
{
	private XML xml;
	public String getSupportedType() { return null; };
	public XML getXML() { return xml; };
	public void setXML(XML xml) { this.xml = xml; };
}

public class javaadapter
{
	private static XML xmlconfig;
	static Shutdown shutdown = new Shutdown();
	private static HashMap<String,List<Subscriber>> subscriberlist;
	static ArrayList<SoapServer> soapservers = new ArrayList<>();
	static boolean isstarted = false;
	static Date startdate = new Date();
	static boolean dohooks = true;
	private static String currentdir;
	static String adaptername;
	static final String DEFAULTCFGFILENAME = "javaadapter.xml";

	public static boolean isShuttingDown()
	{
		return shutdown.isShuttingDown();
	}

	public static void startCleanupHandler()
	{
		shutdown.setShuttingDown(false);
	}

	public static void endCleanupHandler()
	{
		shutdown.setShuttingDown(true);
	}

	public static void setForShutdown(List<?> list)
	{
		shutdown.setCloseList(list);
	}

	public static void setForShutdown(Object obj)
	{
		shutdown.setCloseList(obj);
	}

	public static XML getConfiguration()
	{
		return xmlconfig;
	}

	public static String getCurrentDir()
	{
		return currentdir;
	}

	public static void setCurrentDir(String dir)
	{
		currentdir = dir;
	}

	public static synchronized List<Subscriber> subscriberGet(String name)
	{
		return subscriberlist.get(name);
	}

	public static synchronized void initShutdownHook()
	{
		Runtime.getRuntime().addShutdownHook(shutdown);
	}

	public static void main(String[] args)
	{
		String filename = DEFAULTCFGFILENAME;
		if (args.length > 0)
		{
			if (args[0].equals("crypt"))
			{
				String str = (new DefaultCrypto()).encrypt(args[1]);
				System.out.println(str);
				return;
			}
			else if (args[0].equals("cryptStrong"))
			{
				String str = (new StrongCrypto()).encrypt(args[1]);
				System.out.println(str);
				return;
			}

			filename = args[0];
		}

		try
		{
			initShutdownHook();
			init(filename);
			while(true)
			{
				Thread.sleep(60);
				if (isShuttingDown()) break;
			}
		}
		catch(Throwable ex)
		{
			Misc.log(ex);
			Misc.exit(1,10000);
		}

		System.exit(0);
	}

	static void init(String filename) throws AdapterException
	{
		ArrayList<Hook> hooklist = new ArrayList<>();

		File file = new File(filename);
		String[] parts = file.getName().split("\\.");
		adaptername = parts[0];

		xmlconfig = Misc.readConfig(filename);

		subscriberlist = new HashMap<>();

		XML[] jms = xmlconfig.getElements("jms");
		for(XML jmsxml:jms)
			JMS.getInstance().setInstance(jmsxml);

		XML[] soapserverlist = xmlconfig.getElements("soapserver");
		for(XML serverxml:soapserverlist)
		{
			SoapServer server = null;

			String port = serverxml.getAttribute("port");
			if (port == null)
				server = new SoapServer(serverxml);
/* IFDEF JAVA6 */
			else
				server = new SoapServerStandAlone(serverxml);
/* */
			if (server != null) soapservers.add(server);
		}

		XML[] xmllist = xmlconfig.getElements("subscriber");
		for(XML el:xmllist)
		{
			String name = el.getAttribute("name");
			if (name == null) continue;

			Misc.log(5,"Subscriber: " + name);
			List<Subscriber> sublist = Misc.initSubscribers(el);
			Misc.activateSubscribers(sublist);
			subscriberlist.put(name,sublist);
		}

		xmllist = dohooks ? xmlconfig.getElements("hook") : new XML[0];
		for(XML el:xmllist)
			Misc.initHooks(el,hooklist);

		Misc.activateHooks(hooklist);

		Misc.log(0,"Adapter startup completed");

		isstarted = true;
	}

	public static String getName()
	{
		return adaptername;
	}
}
