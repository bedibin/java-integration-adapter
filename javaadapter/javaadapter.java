import java.util.*;
import java.io.File;

class Shutdown extends Thread
{
	private ArrayList<Object> closelist = new ArrayList<Object>();
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

	public void setCloseList(ArrayList<?> list)
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
	private HashMap<String,AdapterExtendBase> extendlist = new HashMap<String,AdapterExtendBase>();
	private final String defaultname = "default";
	protected String defaultclass;

	protected AdapterExtend() {};

	public void setInstance(XML xml) throws Exception
	{
		String classname = xml.getValue("class",defaultclass);
		String name = xml.getValue("name",defaultname + "/" + getClass().getName());
		AdapterExtendBase ctx = (AdapterExtendBase)Misc.newObject(classname,xml);
		extendlist.put(name,ctx);
	}

	public void publish(String string,XML xmlpublish) throws Exception
	{
		String instance = xmlpublish.getAttribute("instance");
		if (instance == null) instance = defaultname + "/" + getClass().getName();
		AdapterExtendBase ctx = extendlist.get(instance);
		if (ctx == null)
		{
			String type = xmlpublish.getAttribute("type");
			instance = type + "ExtendBase";
			ctx = extendlist.get(instance);
			if (ctx == null)
			{
				ctx = (AdapterExtendBase)Misc.invoke(instance,"getInstance");
				extendlist.put(instance,ctx);
			}
		}
		String name = xmlpublish.getAttribute("name");
		ctx.publish(name,string);
	}
}

abstract class AdapterExtendBase
{
	abstract void publish(String name,String message) throws Exception;
}

public class javaadapter
{
	private static XML xmlconfig;
	static Shutdown shutdown = new Shutdown();
	private static HashMap<String,ArrayList<Subscriber>> subscriberlist;
	static ArrayList<SoapServer> soapservers = new ArrayList<SoapServer>();
	static crypt crypter = new crypt();
	static boolean isstarted = false;
	static Date startdate = new Date();
	static boolean dohooks = true;
	static String currentdir;
	static String adaptername;
	final static String DEFAULTCFGFILENAME = "javaadapter.xml";

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

	public static void setForShutdown(ArrayList<?> list)
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

	public static synchronized ArrayList<Subscriber> subscriberGet(String name)
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
				String str = crypter.encrypt(args[1]);
				System.out.println(str);
				return;
			}
			else if (args[0].equals("cryptStrong"))
			{
				String str = crypter.encryptStrong(args[1]);
				System.out.println(str);
				return;
			}

			filename = args[0];
		}

		try
		{
			initShutdownHook();
			init(filename);
			while(true) Thread.sleep(Integer.MAX_VALUE);
		}
		catch(Throwable ex)
		{
			Misc.log(ex);
			Misc.exit(1,10000);
		}
	}

	static void init(String filename) throws Exception
	{
		ArrayList<Hook> hooklist = new ArrayList<Hook>();

		File file = new File(filename);
		String[] parts = file.getName().split("\\.");
		adaptername = parts[0];

		xmlconfig = new XML(filename);
		Misc.initXML(xmlconfig);

		subscriberlist = new HashMap<String,ArrayList<Subscriber>>();

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
			ArrayList<Subscriber> sublist = Misc.initSubscribers(el);
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
