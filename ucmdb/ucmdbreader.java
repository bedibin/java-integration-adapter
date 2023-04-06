import java.util.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.zip.GZIPOutputStream;
import com.hp.ucmdb.api.*;
import com.hp.ucmdb.api.view.*;
import com.hp.ucmdb.api.view.result.*;
import com.hp.ucmdb.api.types.*;
import com.hp.ucmdb.api.topology.*;
import com.hp.ucmdb.api.topology.queryparameter.*;
import com.hp.ucmdb.api.classmodel.*;
import com.hp.ucmdb.api.users.*;

/* Rules for relations:
First prefix including the semicolumn is optionnal
END1:END2:REL = END2:END1:RREL
END1::REL = END1:RREL
END2:REL = END2::RREL
If multiple relations between 2 CIs: END1:END2:REL_x
*/

enum BulkType { BULK, DATA };
enum ServiceType { TOPOLOGY, MANAGEMENT };

class Ucmdb
{
	private static Ucmdb instance;
	private static Ucmdb instanceManagement;
	private UcmdbService service;
	private UcmdbManagementService serviceManagement;
	private String adapterinfo;
	private int bulksize = 1;
	private BulkType bulktype = BulkType.BULK;

	private Ucmdb(ServiceType management) throws AdapterException
	{
		adapterinfo = "javaadapter/" + javaadapter.getName();

		System.out.print("Connection to UCMDB... ");
		String type = management == ServiceType.MANAGEMENT ? "ucmdb_management" : "ucmdb";
		XML xml = javaadapter.getConfiguration().getElementByPath("/configuration/connection[@type='" + type + "']");
		if (xml == null) throw new AdapterException("No connection element with type '" + type + "' specified");

		String host = xml.getValue("server","localhost");
		String protocol = xml.getValue("protocol","http");
		String portstr = xml.getValue("port","8080");
		int port = Integer.parseInt(portstr);
		String username = xml.getValue("username","admin");
		String password = xml.getValueCrypt("password");
		String repository = xml.getValue("repository",null);

		XML bulkxml = xml.getElement("bulksize");
		if (bulkxml != null)
		{
			String bulkprop = System.getProperty("javaadapter.ucmdb.bulksize");
			String bulkstr = bulkprop == null ? bulkxml.getValue() : bulkprop;
			bulksize = Integer.parseInt(bulkstr);
			bulktype = bulkxml.getAttributeEnum("type",BulkType.BULK,BulkType.class);
		}

		UcmdbServiceProvider serviceProvider;
		try {
			serviceProvider  = UcmdbServiceFactory.getServiceProvider(protocol,host,port);
		} catch(java.net.MalformedURLException ex) {
			throw new AdapterException(ex);
		}
		ClientContext clientContext = serviceProvider.createClientContext(adapterinfo);
		Credentials credentials = serviceProvider.createCredentials(username,password,repository);
		int retry = 0;
		while(true)
		{
			try {
				if (management == ServiceType.MANAGEMENT)
					serviceManagement = serviceProvider.connectManagement(credentials,clientContext);
				else
					service = serviceProvider.connect(credentials,clientContext);
				break;
			} catch (CustomerNotAvailableException ex) {
				retry++;
				if (retry > 3)
					Misc.rethrow(ex);
				Misc.sleep(10000);
			}
		}
		System.out.println("Done");
	}

	TopologyQueryService getQuery()
	{
		return service.getTopologyQueryService();
	}

	TopologyUpdateService getUpdate()
	{
		return service.getTopologyUpdateService();
	}

	ClassModelService getClassModel()
	{
		return service.getClassModelService();
	}

	ViewService getView()
	{
		return service.getViewService();
	}

	SystemUserService getUsers()
	{
		return serviceManagement.getSystemUserService();
	}

	public synchronized static Ucmdb getInstance() throws AdapterException
	{
		if (instance == null)
			instance = new Ucmdb(ServiceType.TOPOLOGY);
		return instance;
	}

	public synchronized static Ucmdb getManagementInstance() throws AdapterException
	{
		if (instanceManagement == null)
			instanceManagement = new Ucmdb(ServiceType.MANAGEMENT);
		return instanceManagement;
	}

	public int getBulkSize() { return bulksize; }
	public BulkType getBulkType() { return bulktype; }

	static final Pattern causedbypattern = Pattern.compile("Caused by:\\s.*?:\\s+(\\S.+)$",Pattern.MULTILINE);
	static final Pattern exceptionpattern = Pattern.compile("^([\\w\\.]+: +\\S.*)$",Pattern.MULTILINE);

	public static String cleanupException(Exception ex)
	{
		String error = ex.getMessage();
		if (error == null) return ex.getClass().getName();

		Matcher matcher = causedbypattern.matcher(error);
		HashSet<String> causes = new HashSet<>();
		while(matcher.find())
		{
			String cause = matcher.group(1);
			causes.add(cause);
		}

		if (causes.size() == 0)
		{
			matcher = exceptionpattern.matcher(error);
			while(matcher.find())
			{
				String cause = matcher.group(1);
				causes.add(cause);
			}
		}

		if (causes.size() == 0)
		{
			String[] lines = error.split("\n");
			causes.add(lines[0]); // First line only
		}
		return Misc.implode(causes);
	}

	public String getInfo()
	{
		return adapterinfo;
	}

	public static String implode(Element element)
	{
		StringBuilder out = new StringBuilder();
		String sep = "";
		for(Property prop:element.properties())
		{
			out.append(sep + prop.getName() + "=" + prop.getValue());
			sep = ",";
		}
		return out.toString();
	}

	public static String implode(Iterable<? extends Element> elements)
	{
		StringBuilder out = new StringBuilder();
		String sep = "";
		for(Element element:elements)
		{
			String id = element.getId().getAsString();
			out.append(sep + "[" + id + ": " + implode(element));
			sep = " ";
		}
		return out.toString();
	}
}

class ReaderUCMDBUsers extends ReaderUtil
{
	Ucmdb ucmdb = Ucmdb.getManagementInstance();
	Iterator<SystemUserInfo> users;

	public ReaderUCMDBUsers(XML xml) throws AdapterException
	{
		setXML(xml);

		SystemUserService userService = ucmdb.getUsers();
		users = userService.getAllSystemUsers().iterator();
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		if (!users.hasNext()) return null;
		SystemUserInfo user = users.next();
		Map<String,String> row = new LinkedHashMap<>();
		row.put("Name",user.getName());
		row.put("FirstName",user.getFistName());
		row.put("LastName",user.getLastName());
		row.put("Email",user.getEmail());
		row.put("Phone",user.getPhone());
		row.put("Address",user.getAddress());
		row.put("Location",user.getLocation());
		row.put("Company",user.getCompany());
		row.put("Origin",user.getDatastoreOrigin());
		return row;
	}
}

class ReaderUCMDB extends ReaderUtil
{
	Iterator<TopologyCI> cis;
	Iterator<? extends ViewResultTreeNode> viewIter;
	Topology topo;
	String rootname;
	Map<UcmdbId,Set<String>> mapids;
	Ucmdb ucmdb = Ucmdb.getInstance();

	public ReaderUCMDB(XML xml) throws AdapterException
	{
		setXML(xml);

		String viewname = xml.getAttribute("view");
		String queryname = xml.getAttribute("query");
		if (viewname != null)
		{
			if (queryname != null)
				throw new AdapterException("Cannot specify both view and query attributes");
			if (instance == null) instance = viewname;
			ViewService viewService = ucmdb.getView();
			ViewFactory viewFactory = viewService.getFactory();
			ViewExecutionOptions viewOptions = viewFactory.createOptions();
			viewOptions.withViewProperties();

			ViewResult viewResult = viewService.executeView(viewname,viewOptions);
			List<? extends ViewResultTreeNode> viewList = viewResult.roots();
			if (headers == null)
			{
				Iterator<? extends ViewResultTreeNode> iter = viewList.iterator();
				if (iter.hasNext())
					headers = propertiesToMap(iter.next().viewProperties()).keySet();
			}

			viewIter = viewList.iterator();
			return;
		}

		if (instance == null) instance = queryname;
		rootname = xml.getAttribute("root");
		if (rootname == null) rootname = "Root";

		TopologyQueryService queryService = ucmdb.getQuery();
		TopologyQueryFactory queryFactory = queryService.getFactory();

		Query query = queryFactory.createNamedQuery(queryname);
		ExecutableQuery execquery = query.toExecutable();

		QueryParameters queryparams = execquery.queryParameters();
		XML[] xmlparams = xml.getElements("param");
		for(XML xmlparam:xmlparams)
		{
			String value = xmlparam.getAttribute("value");
			queryparams.addValue(xmlparam.getAttribute("name"),Misc.substitute(value));
		}

		topo = queryService.executeQuery(execquery);
		Collection<TopologyCI> topocis = topo.getCIsByName(rootname);
		if (Misc.isLog(30)) Misc.log("TopologyCIS: " + topocis);
		cis = topocis.iterator();
		mapids = topo.getContainingNodesMap();
	}

	@SuppressWarnings("unchecked")
	private String getValue(Property property)
	{
		Object value = property.getValue();
		if (value == null) return "";

		switch(property.getType())
		{
		case DATE:
			Date date = (Date)value;
			return Misc.getGmtDateFormat().format(date);
		case STRING_LIST:
			return Misc.implode((Iterable<String>)value,"\n");
		}
		return value.toString().trim();
	}

	private void setProperties(String prefix,Map<String,String> row,Element element)
	{
		String post = "";
		if (prefix != null) post = prefix + ":";

		row.put(post + "INFO",element.getType());
		row.put(post + "ID",element.getId().getAsString());

		for(Property property:element.properties())
			row.put(post + property.getName(),getValue(property));
	}

	String getNameCount(String name,HashMap<String,Integer> counts)
	{
		Integer count = counts.get(name);
		if (count == null)
			counts.put(name,0);
		else
		{
			count++;
			counts.put(name,count);
			name += "_" + count;
		}

		return name;
	}

	String getNameCI(Element ci,HashMap<String,Integer> counts)
	{
		Set<String> set = mapids.get(ci.getId());
		Iterator<String> iter = set.iterator();
		String name = iter.hasNext() ? iter.next() : ci.getType();
		name = getNameCount(name,counts);
		return rootname.equals(name) ? null : name;
	}

	void getRelated(TopologyCI current,String prefix,HashMap<UcmdbId,String> ids,HashMap<String,Integer> counts,Map<String,String> row) throws AdapterException
	{
		if (current == null) return;

		for(TopologyRelation relation:current.getIncomingRelations())
		{
			TopologyCI ci = relation.getEnd1CI();
			String name = ids.get(ci.getId());
			if (!ids.containsKey(ci.getId()))
			{
				name = getNameCI(ci,counts);
				ids.put(ci.getId(),name);
				setProperties(name,row,ci);
			}
			if (!ids.containsKey(relation.getId()))
			{
				String relname = getNameCount((prefix == null ? "" : prefix + ":") + (name == null ? "" : name) + ":RREL",counts);
				ids.put(relation.getId(),relname);
				setProperties(relname,row,relation);
				getRelated(ci,name,ids,counts,row);
			}
		}

		for(TopologyRelation relation:current.getOutgoingRelations())
		{
			TopologyCI ci = relation.getEnd2CI();
			String name = ids.get(ci.getId());
			if (!ids.containsKey(ci.getId()))
			{
				name = getNameCI(ci,counts);
				ids.put(ci.getId(),name);
				setProperties(name,row,ci);
			}
			if (!ids.containsKey(relation.getId()))
			{
				String relname = getNameCount((prefix == null ? "" : prefix + ":") + (name == null ? "" : name) + ":REL",counts);
				ids.put(relation.getId(),relname);
				setProperties(relname,row,relation);
				getRelated(ci,name,ids,counts,row);
			}
		}
	}

	private Map<String,String> propertiesToMap(List<Property> propertyList)
	{
		Map<String,String> row = new LinkedHashMap<>();
		for(Property property:propertyList)
			row.put(property.getName(),getValue(property));
		return row;
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		if (viewIter != null)
		{
			if (!viewIter.hasNext()) return null;

			ViewResultTreeNode viewItem = viewIter.next();
			List<Property> propertyList = viewItem.viewProperties();
			Map<String,String> row = propertiesToMap(propertyList);
			if (Misc.isLog(30)) Misc.log("uCMDB view row: " + row);
			return row;
		}

		if (!cis.hasNext())
		{
			if (!topo.hasNextChunk()) return null;
			topo = topo.getNextChunk();
			Collection<TopologyCI> topocis = topo.getCIsByName(rootname);
			if (Misc.isLog(30)) Misc.log("TopologyCIS next chunk: " + topocis);
			cis = topocis.iterator();
			if (!cis.hasNext()) return null;
		}

		HashMap<UcmdbId,String> ids = new HashMap<>();
		HashMap<String,Integer> counts = new HashMap<>();
		Map<String,String> row = new LinkedHashMap<>();
		TopologyCI root = cis.next();

		ids.put(root.getId(),null);
		setProperties(null,row,root);

		getRelated(root,null,ids,counts,row);
		row = normalizeFields(row);

		if (Misc.isLog(30)) Misc.log("uCMDB query row: " + row);

		return row;
	}
}

class UCMDBUpdateSubscriber extends UpdateSubscriber
{
	static class AttributeType
	{
		Type type;
		String enumname;
		Collection<String> enumvalues;
	}

	class Ends
	{
		private String end1;
		private String end2;

		Ends(String end1,String end2)
		{
			this.end1 = end1;
			this.end2 = end2;
		}

		String getEnd1() { return end1; };
		String getEnd2() { return end2; };

		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof Ends)) return false;
			Ends ends = (Ends)obj;
			return end1.equals(ends.end1) && end2.equals(ends.end2);
		}
	}


	private TopologyUpdateService update;
	private TopologyUpdateFactory factory;
	private TopologyModificationData bulkdata;
	private TopologyModificationBulk bulk;
	private int bulkcount = 0;
	private SyncOper bulkoper = SyncOper.START;
	private EnumMap<SyncOper,TopologyModificationAction> actionmap;
	private ClassModelService classmodel;
	private HashMap<String,AttributeType> attrtypes = new HashMap<>();
	private Ucmdb ucmdb;
	private String adapterinfo;
	private final Pattern relationTagPattern = Pattern.compile("^((([^:]+):)?([^:]*):((REL|RREL)(_(\\d+))*)):INFO$");

	public UCMDBUpdateSubscriber() throws AdapterException
	{
		ucmdb = Ucmdb.getInstance();
		update = ucmdb.getUpdate();
		factory = update.getFactory();
		classmodel = ucmdb.getClassModel();
		adapterinfo = ucmdb.getInfo();

		actionmap = new EnumMap<>(SyncOper.class);
		actionmap.put(SyncOper.UPDATE,TopologyModificationAction.UPDATE_IF_EXISTS);
		actionmap.put(SyncOper.ADD,TopologyModificationAction.CREATE_OR_UPDATE);
		actionmap.put(SyncOper.REMOVE,TopologyModificationAction.DELETE_IF_EXISTS);
	}

	private void FillUpdateData(TopologyModificationData data,XML xml,SyncOper oper) throws AdapterException
	{
		final int debug = 15;
		HashMap<String,Element> elements = new HashMap<>();
		XML[] fields = xml.getElements();

		// First predefine all CIs
		for(XML field:fields)
		{
			String tagname = field.getTagName();
			String prefix = null;
			String id = null;
			XML idxml = null;
			String end1id = null;
			String end2id = null;

			if (tagname.equals("INFO"))
			{
				prefix = "root";
				idxml = xml.getElement("ID");
				end1id = xml.getValue("END1",null);
				end2id = xml.getValue("END2",null);
			}

			Matcher matcher = relationTagPattern.matcher(tagname);
			if (tagname.endsWith(":INFO") && !matcher.find())
			{
				prefix = tagname.substring(0,tagname.length() - ":INFO".length());
				idxml = xml.getElement(prefix + ":ID");
			}

			if (prefix == null) continue;

			String value = field.getValue();

			if (oper == SyncOper.UPDATE)
			{
				XML old = field.getElement("oldvalue");
				if (old != null)
				{
					OnOper type = Field.getOnOper(field,"type");
					String oldvalue = old.getValue();
					if ((value == null || type == OnOper.INITIAL) && oldvalue != null)
						value = oldvalue;
				}

				if (idxml != null)
				{
					id = idxml.getValue();
					if (id == null) id = idxml.getValue("oldvalue",null);
				}
			} else if (oper == SyncOper.REMOVE && idxml != null)
				id = idxml.getValue();

			if (value == null) throw new AdapterException(xml,"Value for element " + tagname + " cannot be empty");

			if (end1id != null && end2id != null)
			{
				if (Misc.isLog(debug)) Misc.log("UCMDBAPI: Defining relation type " + value + " with ID " + prefix + " between " + end1id + " and " + end2id);
				Relation relation = factory.createRelation(value,factory.restoreCIIdFromString(end1id),factory.restoreCIIdFromString(end2id));
				elements.put(prefix,relation);
				data.addRelation(relation);
				continue;
			}

			if (prefix.equals("END1") && id == null) id = xml.getValue("END1",null);
			if (prefix.equals("END2") && id == null) id = xml.getValue("END2",null);
			if (Misc.isLog(debug)) Misc.log("UCMDBAPI: Defining CI type " + value + " with ID " + prefix + (id == null ? "" : ": " + id));
			CI ci = id == null ? factory.createCI(value) : factory.createCI(factory.restoreCIIdFromString(id),value);
			elements.put(prefix,ci);
			data.addCI(ci);
		}

		// Second predefine all relations
		for(XML field:fields)
		{
			String tagname = field.getTagName();
			String value = field.getValue();
			Matcher matcher = relationTagPattern.matcher(tagname);
			if (matcher.find())
			{
				String prefix = matcher.group(1);
				String end1prefix = matcher.group(3);
				if (end1prefix == null || end1prefix.isEmpty()) end1prefix = "root";
				String end2prefix = matcher.group(4);
				if (end2prefix == null || end2prefix.isEmpty()) end2prefix = "root";
				String relprefix = matcher.group(5); // REL_X or RREL_X
				String reltype = matcher.group(6); // REL or RREL

				if (reltype.equals("RREL"))
				{
					// Reverse END1 and END2
					String tmpprefix = end1prefix;
					end1prefix = end2prefix;
					end2prefix = tmpprefix;
				}

				CI end1 = (CI)elements.get(end1prefix);
				if (end1 == null) throw new AdapterException(xml,"Missing " + end1prefix + ":INFO element");
				CI end2 = (CI)elements.get(end2prefix);
				if (end2 == null) throw new AdapterException(xml,"Missing " + end2prefix + ":INFO element");

				if (Misc.isLog(debug)) Misc.log("UCMDBAPI: Defining relation type " + value + " with ID " + prefix + " from CI ID " + end1prefix + " to CI ID " + end2prefix);
				Relation relation = factory.createRelation(value,end1,end2);
				elements.put(prefix,relation);
				data.addRelation(relation);
			}
		}

		// Third populate all attributes
		for(XML field:fields)
		{
			OnOper type = Field.getOnOper(field,"type");
			if (type == OnOper.INFO) continue;
			if (type == OnOper.INFOAPI) continue;

			if (type != OnOper.KEY)
			{
				if (oper == SyncOper.REMOVE) continue;
				if (oper == SyncOper.UPDATE)
				{
					XML old = field.getElement("oldvalue");
					if (old == null) continue;
					String oldvalue = old.getValue();
					if (type == OnOper.INITIAL && oldvalue != null) continue;
				}
			}

			String tagname = field.getTagName();
			int lastColumn = tagname.lastIndexOf(":");
			String prefix = lastColumn == -1 ? "root" : tagname.substring(0,lastColumn);
			Element element = elements.get(prefix);
			if (element == null) throw new AdapterException(xml,"Element " + tagname + " referencing a non existing CI or relation. INFO field missing?");
			String suffix = lastColumn == -1 ? tagname : tagname.substring(lastColumn + 1);

			if (Misc.isLog(debug)) Misc.log("UCMDBAPI: CI attribute '" + suffix + "' prefix " + prefix);
			if (suffix.equals("INFO")) continue;
			if (suffix.equals("ID")) continue;
			if (suffix.equals("END1")) continue;
			if (suffix.equals("END2")) continue;

			String citype = element.getType();
			AttributeType attrtype = attrtypes.get(citype + ":" + suffix);
			if (attrtype == null)
			{
				ClassDefinition classdef = classmodel.getClassDefinition(citype);
				attrtype = new AttributeType();
				attrtype.type = classdef.getAttribute(suffix).getType();

				if (attrtype.type == Type.LIST)
				{
					EnumInfo enumInfo = (EnumInfo)classdef.getAttribute(suffix).getTypeInfo();
					attrtype.enumname = enumInfo.getEnumName();
					EnumDefinitions enumdefs = classmodel.getEnumDefinitions();
					StringEnum enumdef = enumdefs.getStringEnum(attrtype.enumname);
					attrtype.enumvalues = enumdef.getEntries();
				}

				attrtypes.put(citype + ":" + suffix,attrtype);
			}

			String value = field.getValue();

			if (Misc.isLog(debug)) Misc.log("UCMDBAPI: Setting CI attribute '" + suffix + "' to ID " + prefix + " type:" + attrtype.type + " with: " + value);
			if (value == null)
				element.setStringProperty(suffix,null);
			else switch(attrtype.type)
			{
			case DOUBLE:
				try {
				element.setDoubleProperty(suffix,Double.parseDouble(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a double: " + xml);
				}
				break;
			case FLOAT:
				try {
				element.setFloatProperty(suffix,Float.parseFloat(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a float: " + xml);
				}
				break;
			case LONG:
				try {
				element.setLongProperty(suffix,Long.parseLong(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a long: " + xml);
				}
				break;
			case ENUM:
			case INTEGER:
				try {
				element.setIntProperty(suffix,Integer.parseInt(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into an integer: " + xml);
				}
				break;
			case LIST:
				Collection<String> list = attrtype.enumvalues;
				if (list != null && !list.contains(value))
				{
					Misc.log("WARNING: [" + getKeyValue() + "] Invalid value '" + value + "' given for enumeration '" + attrtype.enumname + "': " + xml);
					break;
				}
			case STRING:
				element.setStringProperty(suffix,value);
				break;
			case BOOLEAN:
				element.setBooleanProperty(suffix,(value.equals("1") || value.toLowerCase().equals("true")));
				break;
			case STRING_LIST:
				element.setStringListProperty(suffix,value.split("\n"));
				break;
			case DATE:
				try {
					element.setDateProperty(suffix,Misc.getGmtDateFormat().parse(value));
				} catch(java.text.ParseException ex) {
					throw new AdapterException(ex);
				}
				break;
			case BYTES:
				try {
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					GZIPOutputStream gzip = new GZIPOutputStream(bos);
					gzip.write(value.getBytes());
					gzip.close();
					element.setBytesProperty(suffix,bos.toByteArray());
					bos.close();
				} catch(IOException ex) {
					throw new AdapterException(ex);
				}
				break;
			default:
				throw new AdapterException("Unsupported uCMDB type " + attrtype.type + " for field " + suffix);
			}
		}
	}

	private void push(XML xml,SyncOper oper) throws AdapterException
	{
		if (Misc.isLog(30)) Misc.log("UCMDBAPI: Push " + oper + ": " + xml);

		BulkType bulktype = ucmdb.getBulkType();
		if (bulktype == BulkType.DATA && bulkcount > 0 && oper != bulkoper) flush();
		if (bulkdata == null) bulkdata = factory.createTopologyModificationData();
		bulkoper = oper;

		FillUpdateData(bulkdata,xml,oper);

		if (bulktype == BulkType.BULK)
		{
			if (bulk == null) bulk = factory.createTopologyModificationBulk(adapterinfo);
			TopologyModificationBulkElement element = factory.createTopologyModificationBulkElement(bulkdata,actionmap.get(oper));
			bulk.addTopologyModificationElement(element);
			bulkdata = null;
		}

		int bulksize = ucmdb.getBulkSize();
		bulkcount++;
		if (bulkcount >= bulksize)
			flush();
	}

	private void flush() throws AdapterException
	{
		if (bulkdata != null)
		{
			TopologyModificationData currentbulkdata = bulkdata;
			SyncOper currentoper = bulkoper;
			bulkdata = null;
			bulkcount = 0;
			bulkoper = SyncOper.START;

			SingleTopologyModification modif = factory.createSingleTopologyModification(adapterinfo + "/" + currentoper,currentbulkdata,actionmap.get(currentoper));
			try {
				GracefulTopologyModificationOutput output = update.executeGracefully(modif);
				if (output.isSuccessFul()) return;

				List<TopologyModificationFailure> failures = output.getFailures();
				for(TopologyModificationFailure failure:failures)
				{
					TopologyData data = failure.getTopology();
					Exception cause = failure.getCause();
					Misc.log("ERROR: uCMDB bulk " + currentoper.toString().toLowerCase() + " " + ucmdb.implode(data.getCIs()) + " " + ucmdb.implode(data.getRelations()) + ": " + getExceptionMessage(cause));
					if (Misc.isLog(30)) Misc.log(cause);
				}
			} catch (TopologyUpdateException | ExecutionException ex) {
				throw new AdapterException(ex);
			}
		}

		if (bulk != null)
		{
			TopologyModificationBulk currentbulk = bulk;
			bulk = null;
			bulkcount = 0;
			try {
				update.execute(currentbulk);
			} catch (TopologyUpdateException | ExecutionException ex) {
				throw new AdapterException(ex);
			}
		}
	}

	protected void add(UpdateDestInfo destinfo,XML xml) throws AdapterException
	{
		ArrayList<XML> customs = destinfo.getCustomList(SyncOper.ADD);
		for(XML custom:customs)
		{
			String name = custom.getAttribute("name");
			if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
			String value = custom.getAttribute("value");
			if (value == null) value = "";
			value = Misc.substitute(value,xml);
			xml.setValue(name,value);
			if (Misc.isLog(10)) Misc.log("Updating " + name + " with value " + value + " instead of adding: " + xml);
		}

		if (customs.size() > 0)
		{
			updatemulti(SyncOper.ADD,xml);
			return;
		}

		String end1 = getAddValue(xml,"END1");
		String end2 = getAddValue(xml,"END2");
		if (end1 != null && end2 != null)
		{
			String[] end1values = end1.split("\n");
			String[] end2values = end2.split("\n");

			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Adding multiple relationship ends: " + xml);

			for(String end1value:end1values) for(String end2value:end2values)
			{
				xml.setValue("END1",end1value);
				xml.setValue("END2",end2value);
				push(xml,SyncOper.ADD);
			}
			return;
		}

		push(xml,SyncOper.ADD);
	}

	private String getAddValue(XML xml) throws AdapterException
	{
		if (xml == null) return null;
		return xml.getValue();
	}

	private String getAddValue(XML xml,String name) throws AdapterException
	{
		XML idxml = xml.getElement(name);
		return getAddValue(idxml);
	}

	private String getUpdateValue(XML xml) throws AdapterException
	{
		if (xml == null) return null;
		XML oldxml = xml.getElement("oldvalue");
		if (oldxml != null)
		{
			String result = oldxml.getValue();
			if (result != null) return result;
		}
		return xml.getValue();
	}

	private String getUpdateValue(XML xml,String name) throws AdapterException
	{
		XML idxml = xml.getElement(name);
		return getUpdateValue(idxml);
	}

	private void updatemulti(SyncOper oper,XML xml) throws AdapterException
	{
		XML idxml = xml.getElement("ID");
		String idlist = getUpdateValue(idxml);
		if (idlist != null)
		{
			String[] ids = idlist.split("\n");
			for(String id:ids)
			{
				idxml.setValue(id);
				push(xml,SyncOper.UPDATE);
			}
			return;
		}

		push(xml,SyncOper.UPDATE);
	}

	protected void remove(UpdateDestInfo destinfo,XML xml) throws AdapterException
	{
		ArrayList<XML> customs = destinfo.getCustomList(SyncOper.REMOVE);
		for(XML custom:customs)
		{
			String name = custom.getAttribute("name");
			if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
			String value = custom.getAttribute("value");
			if (value == null) value = "";
			value = Misc.substitute(value,xml);
			XML xmlval = xml.setValue(name,value);
			xmlval.add("oldvalue");
			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Updating " + name + " with value " + value + " instead of deleting: " + xml);
		}

		if (customs.size() > 0)
		{
			updatemulti(SyncOper.REMOVE,xml);
			return;
		}
		
		String end1 = getUpdateValue(xml,"END1");
		String end2 = getUpdateValue(xml,"END2");
		if (end1 != null && end2 != null)
		{
			String[] end1values = end1.split("\n");
			String[] end2values = end2.split("\n");

			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Removing multiple relationship ends: " + xml);

			for(String end1value:end1values) for(String end2value:end2values)
			{
				XML delete = new XML();
				delete = delete.add("remove");
				delete.add("END1",end1value);
				delete.add("END2",end2value);
				delete.add("INFO",xml.getValue("INFO",null));

				push(delete,SyncOper.REMOVE);
			}
			return;
		}

		String idlist = getUpdateValue(xml,"ID");
		if (idlist != null)
		{
			String[] ids = idlist.split("\n");
			if (ids.length > 1)
			{
				for(String id:ids)
				{
					XML delete = xml.copy();
					delete.add("ID",id);

					push(delete,SyncOper.REMOVE);
				}
				return;
			}
		}

		push(xml,SyncOper.REMOVE);
	}

	protected void update(UpdateDestInfo destinfo,XML xml) throws AdapterException
	{
		String old1 = getUpdateValue(xml,"END1");
		String old2 = getUpdateValue(xml,"END2");
		String new1 = getAddValue(xml,"END1");
		String new2 = getAddValue(xml,"END2");
		if (old1 != null && old2 != null && new1 != null && new2 != null)
		{
			String[] old1values = old1.split("\n");
			String[] old2values = old2.split("\n");
			String[] new1values = new1.split("\n");
			String[] new2values = new2.split("\n");

			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Update causing relationship recreation: " + xml);

			List<Ends> oldlist = new ArrayList<>();
			for(String old1value:old1values) for(String old2value:old2values)
				oldlist.add(new Ends(old1value,old2value));
			List<Ends> newlist = new ArrayList<>();
			for(String new1value:new1values) for(String new2value:new2values)
				newlist.add(new Ends(new1value,new2value));

			List<Ends> updatelist = new ArrayList<>(newlist);
			updatelist.retainAll(oldlist);
			List<Ends> addlist = new ArrayList<>(newlist);
			addlist.removeAll(oldlist);
			List<Ends> removelist = new ArrayList<>(oldlist);
			removelist.removeAll(newlist);

			for(Ends remove:removelist)
			{
				XML tmpxml = new XML();
				tmpxml = tmpxml.add("remove");
				tmpxml.add("END1",remove.getEnd1());
				tmpxml.add("END2",remove.getEnd2());
				tmpxml.add("INFO",xml.getValue("INFO",null));
				push(tmpxml,SyncOper.REMOVE);
			}

			for(Ends add:addlist)
			{
				XML tmpxml = new XML();
				tmpxml = tmpxml.add("add");
				tmpxml.add(xml.getElements());
				tmpxml.setValue("END1",add.getEnd1());
				tmpxml.setValue("END2",add.getEnd2());
				push(tmpxml,SyncOper.ADD);
			}

			for(Ends update:updatelist)
			{
				XML tmpxml = new XML();
				tmpxml = tmpxml.add("update");
				tmpxml.add(xml.getElements());
				tmpxml.setValue("END1",update.getEnd1());
				tmpxml.setValue("END2",update.getEnd2());
				push(tmpxml,SyncOper.UPDATE);
			}

			return;
		}

		updatemulti(SyncOper.UPDATE,xml);
	}

	protected void start(UpdateDestInfo destinfo,XML xml) throws AdapterException {}

	protected void end(UpdateDestInfo destinfo,XML xml) throws AdapterException
	{
		flush();
	}

	@Override
	protected void setKeyValue(XML xml) throws AdapterException
	{
		String[] keys = {"END1","END2","ID","INFO"};
		for(String key:keys)
		{
			XML xmlkey = xml.getElement(key);
			if (xmlkey != null) xmlkey.setAttribute("type","key");
		}
		super.setKeyValue(xml);
	}

	@Override
	protected String getExceptionMessage(Exception ex)
	{
		return Ucmdb.cleanupException(ex);
	}

	public static void main(String[] args) throws Exception
	{
		String filename = javaadapter.DEFAULTCFGFILENAME;
		if (args.length > 0) filename = args[0];
		javaadapter.init(filename);

		Ucmdb ucmdb = Ucmdb.getInstance();
		TopologyQueryService queryService = ucmdb.getQuery();
		TopologyQueryFactory queryFactory = queryService.getFactory();

		ExecutableQuery executableQuery = queryService.createExecutableQuery("Test Query List");
		ArrayList<String> parameterValue = new ArrayList<>();
		parameterValue.add("B521200");
		//executableQuery.setStringListParameter("ValueList",parameterValue);
		executableQuery.queryParameters().addValue("ValueList",parameterValue);
		//executableQuery.queryParameters().addValue("ValueName","B521200");
		//executableQuery.setStringParameter("ValueList","B521200");
		Topology topology = queryService.executeQuery(executableQuery);
		Collection<TopologyCI> allcis = topology.getAllCIs();
		System.out.println(allcis.size());
	}
}

class UcmdbLongName
{
	public static String fixLongName(String longName)
	{
		String newName = longName;
		if (longName.length() > getMaxNameLengthAllow())
			newName = cutLongName(longName);
		String res = newName.toUpperCase();
		if (res.indexOf("-") >= 0)
			res = res.replaceAll("-", "_");
		return res;
	}
  
	public static String cutLongName(String longName)
	{
		return longName.substring(0, getMaxNameLengthAllow() - 12) + "_" + Math.abs(longName.hashCode());
	}

	public static int getMaxNameLengthAllow()
	{
		return 30 - 1;
	}

	public static void main(String[] args) throws Exception
	{
		// java -cp ucmdbreader.jar UcmdbLongName A_desj_asset_maintenance_contract
		// Return: A_DESJ_ASSET_MAIN_1137851210
		System.out.println(fixLongName(args[0]));
	}
}
