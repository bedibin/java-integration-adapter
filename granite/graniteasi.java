import java.util.*;
import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.lang.reflect.*;
import javax.naming.*;
import com.granite.asi.dto.clientview.*;
import com.granite.asi.dto.generated.clientview.PathChannelAssignments;
import com.granite.asi.dto.DataObject;
import com.granite.asi.dto.UdaASIList;
import com.granite.asi.factory.*;
import com.granite.asi.service.*;
import com.granite.asi.util.*;
import com.granite.asi.key.*;

class ASI
{
	protected DataObjectFactory dataFactory;
	protected ServiceFactory serviceFactory;
	protected LockService lockService;

	static public final String CORESERVICE = "CoreService";
	static public final String KEYCLASS = "KeyClass";
	static public final String UDAGROUP = "UdaGroup";
	static public final String COREDATAOBJECT = "CoreDataObject";
	static public final String INSTANCEID = "InstanceId";
	static public final String ROOTSERVICE = "RootService";
	static public final String PARENTSERVICE = "ParentService";

	public Hashtable<String,Hashtable<String,Object>> infoServices = new Hashtable<String,Hashtable<String,Object>>();

	public XML xml;
	public String domain;

	protected static ASI instance;

	private ASI() throws Exception
	{
		System.out.print("Connection to Granite... ");

		xml = new XML("graniteasi.xml");
		domain = xml.getValue("domain",null);

		init();

		Hashtable<String,Object> info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.CABLE);
		info.put(KEYCLASS,"generated.CableKey");
		info.put(UDAGROUP,"OpenView - C\u00E2ble");
		info.put(COREDATAOBJECT,CoreDataObjects.CABLE);
		info.put(INSTANCEID,"CableInstId");
		infoServices.put("Cable",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.PAIR);
		info.put(KEYCLASS,"PairKey");
		info.put(UDAGROUP,"OpenView - Paire/Fibre");
		info.put(COREDATAOBJECT,CoreDataObjects.PAIR);
		info.put(INSTANCEID,"CablePairInstId");
		info.put(ROOTSERVICE,"Cable");
		info.put(PARENTSERVICE,"Cable");
		infoServices.put("CablePair",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.CARD);
		info.put(KEYCLASS,"CardKey");
		info.put(UDAGROUP,"OpenView - Carte");
		info.put(COREDATAOBJECT,CoreDataObjects.CARD);
		info.put(INSTANCEID,"CardInstId");
		info.put(ROOTSERVICE,"Shelf");
		info.put(PARENTSERVICE,"Slot");
		infoServices.put("Card",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SUBCARD);
		info.put(KEYCLASS,"SubCardKey");
		info.put(UDAGROUP,"OpenView - Carte");
		info.put(COREDATAOBJECT,CoreDataObjects.SUBCARD);
		info.put(INSTANCEID,"CardInstId");
		info.put(ROOTSERVICE,"Shelf");
		info.put(PARENTSERVICE,"Slot");
		infoServices.put("SubCard",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.CONTAINER);
		info.put(KEYCLASS,"ContainerKey");
		info.put(UDAGROUP,"OpenView - Equipement");
		info.put(COREDATAOBJECT,CoreDataObjects.CONTAINER);
		info.put(INSTANCEID,"EquipInstId");
		infoServices.put("Container",info);

		info = new Hashtable<String,Object>();
		info.put(KEYCLASS,"PathElementKey");
		info.put(COREDATAOBJECT,CoreDataObjects.PATHELEMENT);
		info.put(INSTANCEID,"ElementInstId");
		info.put(ROOTSERVICE,"Path");
		info.put(PARENTSERVICE,"Leg");
		infoServices.put("Element",info);

		info = new Hashtable<String,Object>();
		info.put(KEYCLASS,"PathLegKey");
		info.put(COREDATAOBJECT,CoreDataObjects.PATHLEG);
		info.put(INSTANCEID,"LegInstId");
		info.put(ROOTSERVICE,"Path");
		info.put(PARENTSERVICE,"Path");
		infoServices.put("Leg",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.PATH);
		info.put(KEYCLASS,"generated.PathKey");
		info.put(UDAGROUP,"OpenView - Circuit");
		info.put(COREDATAOBJECT,CoreDataObjects.PATH);
		info.put(INSTANCEID,"CircPathInstId");
		infoServices.put("Path",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.PORT);
		info.put(KEYCLASS,"PortKey");
		info.put(UDAGROUP,"OpenView - Port");
		info.put(COREDATAOBJECT,CoreDataObjects.PORT);
		info.put(INSTANCEID,"PortInstId");
		info.put(ROOTSERVICE,"Shelf");
		info.put(PARENTSERVICE,"Card");
		infoServices.put("Port",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SEGMENT);
		info.put(KEYCLASS,"generated.SegmentKey");
		info.put(UDAGROUP,"OpenView - Segment");
		info.put(COREDATAOBJECT,CoreDataObjects.SEGMENT);
		info.put(INSTANCEID,"CircInstId");
		infoServices.put("Segment",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SHELF);
		info.put(KEYCLASS,"ShelfKey");
		info.put(UDAGROUP,"OpenView - Equipement");
		info.put(COREDATAOBJECT,CoreDataObjects.SHELF);
		info.put(INSTANCEID,"EquipInstId");
		infoServices.put("Shelf",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SLOT);
		info.put(KEYCLASS,"SlotKey");
		info.put(COREDATAOBJECT,CoreDataObjects.SLOT);
		info.put(INSTANCEID,"SlotInstId");
		info.put(ROOTSERVICE,"Shelf");
		info.put(PARENTSERVICE,"Shelf");
		infoServices.put("Slot",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SUBCARD);
		info.put(KEYCLASS,"SubCardKey");
		info.put(UDAGROUP,"OpenView - Carte");
		info.put(COREDATAOBJECT,CoreDataObjects.SUBCARD);
		info.put(INSTANCEID,"CardInstId");
		info.put(ROOTSERVICE,"Shelf");
		info.put(PARENTSERVICE,"Slot");
		infoServices.put("SubCard",info);

		info = new Hashtable<String,Object>();
		info.put(CORESERVICE,CoreServices.SITE);
		info.put(KEYCLASS,"generated.SiteKey");
		info.put(UDAGROUP,"OpenView - Site");
		info.put(COREDATAOBJECT,CoreDataObjects.SITE);
		info.put(INSTANCEID,"SiteInstId");
		infoServices.put("Site",info);

		System.out.println("Done");
	}

	private synchronized void init() throws Exception
	{
		dataFactory = ASIFactory.newDataObjectFactory(DataForms.CLIENTVIEW);
		serviceFactory = ASIFactory.newServiceFactory(Protocols.IIOP,dataFactory);

		Properties properties = new Properties();
		XML conn = xml.getElement("connection");
		if (conn == null)
			properties.load(new FileInputStream(new File(javaadapter.getCurrentDir(),"graniteasi.properties")));
		else
		{
			properties.setProperty(Context.INITIAL_CONTEXT_FACTORY,conn.getValue("context","weblogic.jndi.WLInitialContextFactory"));
			properties.setProperty(Context.PROVIDER_URL,conn.getValue("url"));
			properties.setProperty("com.granite.asi.host",conn.getValue("hostname"));
			properties.setProperty("com.granite.asi.username",conn.getValue("username"));
			properties.setProperty("com.granite.asi.password",conn.getValueCrypt("password"));
			domain = conn.getValue("database");
			properties.setProperty("com.granite.asi.database",domain);
		}

		try
		{
			serviceFactory.startup(properties);
			lockService = (LockService)serviceFactory.newService(CoreServices.LOCK);
		}
		catch(com.granite.asi.exception.StartupException ex)
		{
			resetLicense();
			serviceFactory.startup(properties);
			lockService = (LockService)serviceFactory.newService(CoreServices.LOCK);
		}
	}

	public synchronized static ASI getInstance() throws Exception
	{
		if (instance == null)
		{
			instance = new ASI();
			javaadapter.setForShutdown(instance);
		}
		return instance;
	}

	public synchronized void close()
	{
		if (instance == null) return;
		instance = null;

		try
		{
			serviceFactory.shutdown();
		}
		catch(Exception ex)
		{
		}
		System.out.print("Granite ");
	}

	static public Long getGraniteId(String objectid)
	{
		String[] ids = objectid.split("\\\\");
		if (ids.length < 3) return null;

		Long idnum = new Long(ids[2]);
		if (idnum == 0) return null;

		return idnum;
	}

	public synchronized void lock(LockableKey key) throws Exception
	{
		lockService.lock(key);
	}

	public synchronized void unlock(LockableKey key) throws Exception
	{
		lockService.unlock(key);
	}

	public synchronized Service getService(CoreServices service) throws Exception
	{
		try
		{
			return serviceFactory.newService(service);
		}
		catch(Exception ex)
		{
			Misc.log(1,"Granite seems to be down, trying to reconnect");
			close();
			resetLicense();
			init();
			return serviceFactory.newService(service);
		}
	}

	public synchronized DataObject getObject(CoreDataObjects object) throws Exception
	{
		return dataFactory.newDataObject(object);
	}

	public synchronized DataObject getObject(Service service,Key key) throws Exception
	{
		DataObject object = null;
		try
		{
			object = (DataObject)Misc.invoke(service,"get",key);
		}
		catch(InvocationTargetException ex)
		{
			Misc.log(1,"Error invoking Granite object, Granite might be down, trying to reconnect");
			close();
			resetLicense();
			init();
			Misc.rethrow(ex);
		}

		return object;
	}

	public synchronized Key getKey(Hashtable<String,Object> infoService,long id) throws Exception
	{
		Object objects[] = new Object[1];
		objects[0] = id;
		Class<?> types[] = new Class[1];
		types[0] = Long.TYPE;

		Key key = (Key)Class.forName("com.granite.asi.key." + (String)infoService.get(KEYCLASS)).getConstructor(types).newInstance(objects);

		return key;
	}

	private synchronized void resetLicense() throws Exception
	{
		XML licensexml = xml.getElement("licensereset");
		if (licensexml == null) return;

		String delaystr = licensexml.getAttribute("delay");
		int delay = (delaystr == null) ? 0 : new Integer(delaystr);

		Thread.sleep(delay);

		URL url = new URL(licensexml.getAttribute("url"));
		Publisher publisher = Publisher.getInstance();
		Publisher.PublisherObject pub = publisher.new PublisherObject(url);
		String request = pub.sendHttpRequest("",licensexml.getElement(null));
		if (Misc.isLog(5)) Misc.log("License reset request: " + request);

		Pattern p = Pattern.compile("xng-?\\d+:ASI-01-000");
		Matcher m = p.matcher(request);
		boolean found = m.find();
		if (found)
		{
			String id = m.group();
			Misc.log("WARNING: Granite ASI license not released. Forcing license purge for ID " + id);
			String body = "purgewhat=Selected&daysOldToPurge=30&" + id + "=yes";
			publisher.publish(body,licensexml);

			Thread.sleep(delay);
		}
	}
}

class ASIobject
{
	private ASI asi;
	private String className;
	private DataObject object;
	private Service service;
	private Long parentId;
	private Key key;
	private Hashtable<String,Object> infoService;
	private Hashtable<String,Object> parentInfoService;
	private Hashtable<String,Object> rootInfoService;

	private void init(String classname) throws Exception
	{
		asi = ASI.getInstance();
		className = classname;

		synchronized(asi)
		{
			infoService = asi.infoServices.get(classname);
			if (infoService == null) throw new AdapterException("Not supported service class " + classname);

			if (infoService.containsKey(ASI.PARENTSERVICE))
				parentInfoService = asi.infoServices.get(infoService.get(ASI.PARENTSERVICE));

			if (infoService.containsKey(ASI.ROOTSERVICE))
				rootInfoService = asi.infoServices.get(infoService.get(ASI.ROOTSERVICE));

			if (infoService.containsKey(ASI.CORESERVICE))
				service = asi.getService((CoreServices)infoService.get(ASI.CORESERVICE));
			else
				service = asi.getService((CoreServices)parentInfoService.get(ASI.CORESERVICE));
		}
	}

	public ASIobject(String classname,long id) throws Exception
	{
		init(classname);

		key = asi.getKey(infoService,id);
		object = asi.getObject(service,key);

		if (object == null) throw new RuntimeException(classname + " id " + id + " not found");
	}

	public ASIobject(String classname) throws Exception
	{
		init(classname);

		object = asi.getObject((CoreDataObjects)infoService.get(ASI.COREDATAOBJECT));

		key = null;
	}

	public ASIobject(DataObject object) throws Exception
	{
		String classname = object.getClass().getName();
		int pos = classname.lastIndexOf(".");
		if (pos != -1)
			classname = classname.substring(pos + 1);

		init(classname);

		synchronized(asi)
		{
			this.object = object;
			key = object.getKey();
		}
	}

	public String getAttribute(String name) throws Exception
	{
		synchronized(asi)
		{
			return Misc.invoke(object,"get" + name).toString();
		}
	}

	public Hashtable<String,Object> getAllAttributes() throws Exception
	{
		synchronized(asi)
		{
			return getAllAttributesImpl();
		}
	}

	private Hashtable<String,Object> getAllAttributesImpl() throws Exception
	{
		Hashtable<String,Object> result = new Hashtable<String,Object>();

		Method[] methods = object.getClass().getMethods();
		for (int i = 0;i < methods.length;i++)
		{
			Method method = methods[i];
			String name = method.getName();
			if (!name.startsWith("get")) continue;

			name = name.substring(3); // strip get prefix

			Class<?>[] params = method.getParameterTypes();
			if (params.length != 0) continue;

			Object objresult = method.invoke(object,(Object[])null);
			if (objresult == null) continue;

			String value = objresult.toString();

			result.put(name,value);
		}

		try
		{
			UdaASIList udas = (UdaASIList)Misc.invoke(object,"getUdas");

			for(int i = 0;i < udas.size();i++)
			{
				Uda uda = (Uda)udas.get(i);
				String name = uda.getUdaName();
				String value = uda.getUdaValue();
				String group = uda.getGroupName();

				String elementname = group + "_" + name;
				elementname = XML.fixName(elementname);

				result.put(elementname,value);
			}
		}
		catch(Exception ex)
		{
		}
		return result;
	}

	protected void setWild(String name,boolean wild) throws Exception
	{
		String function = "setWild" + name;
		Misc.invoke(object,function,wild);
	}

	private void setAttribute(Method method,Object value) throws Exception
	{
		Object[] params = new Object[1];

		params[0] = value;

		method.invoke(object,params);
	}

	private void setAttributeImpl(String name,Object value) throws Exception
	{
		name = "set" + name;
		if (value == null) value = "";

		Method[] methods = object.getClass().getMethods();
		for (int i = 0;i < methods.length;i++)
		{
			Method method = methods[i];

			if (!name.equals(method.getName())) continue;

			Class<?>[] params = method.getParameterTypes();
			if (params.length != 1) continue;

			String type = params[0].getName();

			if (Misc.isLog(9)) Misc.log("Name: " + name + ", object: " + object.getClass().getName() + ", value: " + value.getClass().getName() + " , param: " + type);
			if (type.equals("long") && value instanceof String)
				setAttribute(method,new Long((String)value));
			else if (type.equals("int") && value instanceof String)
				setAttribute(method,new Integer((String)value));
			else if (type.equals("double") && value instanceof String)
				setAttribute(method,new Double((String)value));
			else
				setAttribute(method,value);
			return;
		}
		throw new AdapterException("No method found for " + name);
	}

	public void setAttribute(String name,String value) throws Exception
	{
		synchronized(asi)
		{
			setAttributeImpl(name,value);
		}
	}

	private void setAttributeImpl(String name,String value) throws Exception
	{
		if (className.equals("Path"))
		{
			if (name.equals("Topology"))
				name += "Abbr";
			else if (name.equals("PathName"))
				name = "Name";
			else if (name.equals("VirtualChans"))
			{
				name = "ChannelAssignments";
				if (value.equals("Y")) value = "VIRTUAL";
				else if (value.equals("N")) value = "FIXED";
				else if (value.equals("D")) value = "DYNAMIC";
				setAttributeImpl(name,PathChannelAssignments.valueOf(value));
				return;
			}
		}

		if (parentInfoService != null && name.equals(parentInfoService.get(ASI.INSTANCEID)))
			parentId = new Long(value);

		setAttributeImpl(name,(Object)value);
	}

	public String getClassName()
	{
		return className;
	}

	public String getName() throws Exception
	{
		return getAttribute("Name");
	}

	public void setUda(String group,String name,String value) throws Exception
	{
		synchronized(asi)
		{
			UdaASIList udas = (UdaASIList)Misc.invoke(object,"getUdas");
			Uda uda = (Uda)udas.getUda(group,name);
			if (uda == null)
			{
				uda = Uda.create(group,name,value);
				@SuppressWarnings("unchecked")
				ArrayList<Uda> udalist = (ArrayList<Uda>)udas;
				udalist.add(uda);
			}
			else
				uda.setUdaValue(value);
		}
	}

	public void setDefaultUda(String value) throws Exception
	{
		synchronized(asi)
		{
			setUda((String)asi.infoServices.get(className).get(ASI.UDAGROUP),"Service Request ID",value);
		}
	}

	public void operation(String name,boolean lock) throws Exception
	{
		synchronized(asi)
		{
			operationImpl(name,lock);
		}
	}

	private void operationImpl(String name,boolean lock) throws Exception
	{
		LockableKey lockkey = null;
		if (parentInfoService != null && rootInfoService != null)
		{
			Long rootId = null;

			if (key == null)
			{
				if (parentId != null)
				{
					if (Misc.isLog(5)) Misc.log("Parent field found");
					ASIobject parentObject = new ASIobject((String)infoService.get(ASI.PARENTSERVICE),parentId);
					rootId = new Long(parentObject.getAttribute((String)rootInfoService.get(ASI.INSTANCEID)));
				}
			}
			else
				rootId = new Long(getAttribute((String)rootInfoService.get(ASI.INSTANCEID)));

			if (rootId != null)
			{
				ASIobject rootObject = new ASIobject((String)infoService.get(ASI.ROOTSERVICE),rootId);
				lockkey = (LockableKey)rootObject.key;
			}
		}
		else if (key != null)
			lockkey = (LockableKey)key;

		if (lock && lockkey != null) asi.lock(lockkey);
		try
		{
			Object[] objects = new Object[1];
			objects[0] = name.equals("delete") ? key : object;

			Method method = Misc.getMethod(service,name,objects);
			if (method == null)
			{
				if (Misc.isLog(5)) Misc.log("Trying " + name + " with an array");
				Key[] keylist = { key };
				DataObject[] objectlist = { object };
				objects[0] = name.equals("delete") ? keylist : objectlist;
				method = Misc.getMethod(service,name,objects);
			}

			if (method == null)
				throw new NoSuchMethodException("Method " + name + " is not available");

			if (name.equals("insert"))
			{

				key = (Key)method.invoke(service,objects);
				ASIobject newobject = new ASIobject(className,key.getInstId());
				object = newobject.object;
			}
			else
				method.invoke(service,objects);
		}
		finally
		{
			if (lock && lockkey != null) asi.unlock(lockkey);
		}
	}

	public void update() throws Exception
	{
		operation("update",true);
	}

	public void insert() throws Exception
	{
		operation("insert",false);
	}

	public void delete() throws Exception
	{
		operation("delete",true);
	}

	public ASIobject[] query() throws Exception
	{
		synchronized(asi)
		{
			Query query = new Query();
			query.addQueryEntry(object);

			Object[] params = new Object[1];
			params[0] = query;

			DataObject[] dataobjects = (DataObject[])Misc.invoke(service,"query",params);
			if (Misc.isLog(3)) Misc.log("ASI query " + dataobjects.length);
			if (dataobjects.length == 0) return null;

			ASIobject[] objects = new ASIobject[dataobjects.length];
			for(int i = 0;i < dataobjects.length;i++)
				objects[i] = new ASIobject(dataobjects[i]);

			if (Misc.isLog(5)) Misc.log("ASI query done");
			return objects;
		}
	}
}

class ChangeRequestSubscriber extends Subscriber
{
	public ChangeRequestSubscriber() throws Exception
	{
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		String objectname = xml.getValue("objectName");
		String objectid = xml.getValue("objectId");
		String requestid = xml.getValue("changeRequestId");

		Long idnum = ASI.getGraniteId(objectid);
		if (idnum == null) return null;

		if (Misc.isLog(3)) Misc.log("Change request: " + objectname + ", " + idnum + ", " + requestid);

		ASIobject object = new ASIobject(objectname,idnum);
		String name = object.getName();
		if (Misc.isLog(5)) Misc.log("Processing " + name + "...");
		object.setDefaultUda(requestid);
		object.update();
		return null;
	}
	public void close()
	{
	}
}

class NewServiceRequestSubscriber extends Subscriber
{
	public NewServiceRequestSubscriber() throws Exception
	{
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		String objectname = xml.getValue("objectName");
		if (!objectname.equals("Path")) return null;

		String objectid = xml.getValue("objectId");
		Long idnum = ASI.getGraniteId(objectid);

		if (idnum != null)
		{
			XML result = xml.getElementByPath("serviceRequestFields[name='Status']/value");
			if (result == null) return null;

			String value = result.getValue();
			if (value == null) return null;

			if (Misc.isLog(3)) Misc.log("Update service request: " + objectname + " id: " + idnum);

			value = value.trim();

			ASIobject object = new ASIobject(objectname,idnum);
			object.setAttribute("Status",value);
			object.update();

			return null;
		}

		if (Misc.isLog(3)) Misc.log("New service request: " + objectname);

		ASIobject object = new ASIobject(objectname);

		XML fields[] = xml.getElements("serviceRequestFields");
		for(XML field:fields)
		{
			String uda = field.getValue("udaGroup");
			String name = field.getValue("name");
			// Use: Name, Status, Type, Bandwidth, TopologyAbbr, 
			if (name == null) continue;

			String value = field.getValue("value");
			if (value == null) continue;

			if (uda != null)
				object.setUda(uda,name,value);
			else
				object.setAttribute(name,value);
		}

		object.insert();
		return null;
	}
	public void close()
	{
	}
}

