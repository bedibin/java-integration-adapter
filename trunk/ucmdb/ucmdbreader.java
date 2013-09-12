import java.util.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import com.hp.ucmdb.api.*;
import com.hp.ucmdb.api.view.*;
import com.hp.ucmdb.api.view.result.*;
import com.hp.ucmdb.api.types.*;
import com.hp.ucmdb.api.topology.*;
import com.hp.ucmdb.api.classmodel.*;

class Ucmdb
{
	private static Ucmdb instance;
	private UcmdbService service;
	public static SimpleDateFormat dateformat;

	private Ucmdb() throws Exception
	{
		dateformat = new SimpleDateFormat(Misc.DATEFORMAT);
		dateformat.setTimeZone(TimeZone.getTimeZone("GMT"));

		System.out.print("Connection to UCMDB... ");
		XML xml = javaadapter.getConfiguration().getElementByPath("/configuration/connection[@type='ucmdb']");
		if (xml == null) throw new AdapterException("No connection element with type 'ucmdb' specified");

		String host = xml.getValue("server","localhost");
		String protocol = xml.getValue("protocol","http");
		String portstr = xml.getValue("port","8080");
		int port = new Integer(portstr);
		String username = xml.getValue("username","admin");
		String password = xml.getValueCrypt("password");

		UcmdbServiceProvider serviceProvider = UcmdbServiceFactory.getServiceProvider(protocol,host,port);
		ClientContext clientContext = serviceProvider.createClientContext("uCMDBAdapter:"+javaadapter.getName());
		Credentials credentials = serviceProvider.createCredentials(username,password);
		service = serviceProvider.connect(credentials,clientContext);
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

	public synchronized static Ucmdb getInstance() throws Exception
	{
		if (instance == null)
			instance = new Ucmdb();
		return instance;
	}

	static final Pattern causedbypattern = Pattern.compile("Caused by:\\s.*?:\\s+(\\S.+)$",Pattern.MULTILINE);

	public static String cleanupException(Exception ex)
	{
		String error = ex.getMessage();
		Matcher matcher = causedbypattern.matcher(error);
		HashSet<String> causes = new HashSet<String>();
		while(matcher.find())
		{
			String cause = matcher.group(1);
			causes.add(cause);
		}

		if (causes.size() == 0) return error;
		return Misc.implode(causes);
	}
}

class ReaderUCMDB implements Reader
{
	private ArrayList<String> headers;
	Iterator<TopologyCI> cis;
	Topology topo;
	String root;
	Map<UcmdbId,Set<String>> mapids;
	Ucmdb ucmdb = Ucmdb.getInstance();

	public ReaderUCMDB(XML xml) throws Exception
	{
		String queryname = xml.getAttribute("query");
		root = xml.getAttribute("root");
		if (root == null) root = "Root";

		TopologyQueryService queryService = ucmdb.getQuery();
		TopologyQueryFactory queryFactory = queryService.getFactory();

		Query query = queryFactory.createNamedQuery(queryname);
		ExecutableQuery execquery = query.toExecutable();
/*
		execquery.setMaxChunkSize(2);

		String sortfields = xml.getAttribute("sort_fields");
		if (sortfields == null) throw new AdapterException(xml,"sort_fields attribute is mandatory");
		String[] sortattrs = sortfields.split("\\s*,\\s*");
		ArrayList<AttributeSortingRule> sortlist = new ArrayList<AttributeSortingRule>();
		for(String attr:sortattrs)
			sortlist.add(queryFactory.createAttributeSortingOrderElement(attr,AttributeSortingOrderElementDirection.ASCENDING));

		CIsChunk chunk = queryService.retrieveCIsSortedChunk(execquery,"Root",sortlist);
		Query subquery = queryFactory.createNamedQuery(queryname);
		QueryDefinition querydef = subquery.getDefinition();
		QueryNode node = querydef.getNode(root);
		ArrayList<UcmdbId> rootids = new ArrayList<UcmdbId>();
		for(CI ci:chunk.cis())
			rootids.add(ci.getId());
		node.withIds(rootids);
		Topology subtopo = queryService.executeQuery(subquery.toExecutable());
		mapids = subtopo.getContainingNodesMap();
		for(CI root:chunk.cis())
		{
			ArrayList<UcmdbId> ids = new ArrayList<UcmdbId>();
			HashMap<String,String> row = new HashMap<String,String>();
			setProperties(null,row,root);
			getRelated(subtopo.getCI(root.getId()),ids,row);
		}

		System.exit(0);
*/
		topo = queryService.executeQuery(execquery);
		Collection<TopologyCI> topocis = topo.getCIsByName(root);
		if (Misc.isLog(30)) Misc.log("TopologyCIS: " + topocis);
		cis = topocis.iterator();
		mapids = topo.getContainingNodesMap();
	}

	private String setProperties(String prefix,LinkedHashMap<String,String> row,Element element)
	{
		String post = "";
		if (prefix != null) post = prefix + ":";

		String info = post + "INFO";

		if (prefix != null)
		{
			int count = 0;
			if (row.containsKey(info))
				count = new Integer(row.get(post + "NB"));
			count++;

			row.put(post + "NB","" + count);

			if (count > 1) post = prefix + "_" + (count - 1) + ":";
		}

		row.put(info,element.getType());
		row.put(post + "ID",element.getId().getAsString());

		for(Property property:element.properties())
		{
			Object value = property.getValue();
			if (property.getType() == Type.DATE)
			{
				Date date = (Date)value;
				row.put(post + property.getName(),ucmdb.dateformat.format(date));
			}
			else
				row.put(post + property.getName(),value == null ? "" : value.toString().trim());
		}

		return post;
	}

	String getName(Element ci)
	{
		Set<String> set = mapids.get(ci.getId());
		Iterator<String> iter = set.iterator();
		if (iter.hasNext()) return iter.next();
		return ci.getType();
	}

	void getLink(TopologyRelation relation,ArrayList<UcmdbId> ids,LinkedHashMap<String,String> row) throws Exception
	{
		if (ids.contains(relation.getId())) return;
		ids.add(relation.getId());
		String id = setProperties(getName(relation.getEnd1CI()) + "-" + getName(relation.getEnd2CI()) + "-" + getName(relation),row,relation);
		row.put(id + "END1",relation.getEnd1CI().getId().getAsString());
		row.put(id + "END2",relation.getEnd2CI().getId().getAsString());
	}

	void getRelated(TopologyCI root,ArrayList<UcmdbId> ids,LinkedHashMap<String,String> row) throws Exception
	{
		if (root == null) return;

		for(TopologyRelation relation:root.getIncomingRelations())
		{
			getLink(relation,ids,row);
			TopologyCI ci = relation.getEnd1CI();
			if (ids.contains(ci.getId())) continue;
			ids.add(ci.getId());
			setProperties(getName(ci),row,ci);
			getRelated(root,ids,row);
		}

		for(TopologyRelation relation:root.getOutgoingRelations())
		{
			getLink(relation,ids,row);
			TopologyCI ci = relation.getEnd2CI();
			if (ids.contains(ci.getId())) continue;
			ids.add(ci.getId());
			setProperties(getName(ci),row,ci);
			getRelated(root,ids,row);
		}
	}

	public LinkedHashMap<String,String> next() throws Exception
	{
		if (!cis.hasNext())
		{
			if (!topo.hasNextChunk()) return null;
			topo = topo.getNextChunk();
			Collection<TopologyCI> topocis = topo.getCIsByName(root);
			if (Misc.isLog(30)) Misc.log("TopologyCIS next chunk: " + topocis);
			cis = topocis.iterator();
			if (!cis.hasNext()) return null;
		}

		ArrayList<UcmdbId> ids = new ArrayList<UcmdbId>();
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		TopologyCI root = cis.next();

		ids.add(root.getId());
		setProperties(null,row,root);

		getRelated(root,ids,row);

		if (Misc.isLog(30)) Misc.log("uCMDB row: " + row);

		return row;
	}

	public ArrayList<String> getHeader()
	{
		return headers;
	}

	public String getName()
	{
		return this.getClass().getName();
	}
}

class UCMDBUpdateSubscriber extends UpdateSubscriber
{
	private TopologyUpdateService update;
	private TopologyUpdateFactory factory;
	private ClassModelService classmodel;
	private HashMap<String,Type> attrtypes = new HashMap<String,Type>();
	private Ucmdb ucmdb;

	public UCMDBUpdateSubscriber() throws Exception
	{
		ucmdb = Ucmdb.getInstance();
		update = ucmdb.getUpdate();
		factory = update.getFactory();
		classmodel = ucmdb.getClassModel();
	}

	private void FillUpdateData(TopologyModificationData data,XML xml,String prefix,boolean isdelete) throws Exception
	{
		String citype = xml.getValue(prefix == null ? "INFO" : prefix + ":INFO");
		if (citype == null) throw new AdapterException("INFO field not found. Set it to CI type");
		String id = xml.getValue(prefix == null ? "ID" : prefix + ":ID",null);
		String end1 = xml.getValue(prefix == null ? "END1" : prefix + ":END1",null);
		String end2 = xml.getValue(prefix == null ? "END2" : prefix + ":END2",null);

		Element ci;
		if (end1 == null && end2 == null)
			ci = (id == null ? data.addCI(citype) : data.addCI(factory.restoreCIIdFromString(id),citype));
		else
			ci = (id == null ? data.addRelation(citype,factory.restoreCIIdFromString(end1),factory.restoreCIIdFromString(end2)) : data.addRelation(factory.restoreCIIdFromString(id),citype,factory.restoreCIIdFromString(end1),factory.restoreCIIdFromString(end2)));
		if (ci == null) throw new AdapterException("CI data is null");

		XML[] fields = xml.getElements();
		for(XML field:fields)
		{
			String type = field.getAttribute("type");
			if (type != null && type.equals("info")) continue;
			if (type != null && isdelete && !type.equals("key")) continue;

			XML old = field.getElement("oldvalue");
			if (type != null && old != null)
			{
				String oldvalue = old.getValue();
				if (type.equals("initial") && oldvalue != null) continue;
			}

			String tagname = field.getTagName();
			String suffix = tagname;
			if (prefix != null)
			{
				if (!tagname.startsWith(prefix + ":")) continue;
				suffix = tagname.substring(prefix.length()+1);
			}

			if (Misc.isLog(30)) Misc.log("Tag suffix is " + suffix);
			if (suffix.equals("INFO")) continue;
			if (suffix.equals("ID")) continue;
			if (suffix.equals("END1")) continue;
			if (suffix.equals("END2")) continue;

			if (suffix.equals(":INFO") && !isdelete)
				FillUpdateData(data,xml,tagname.substring(0,tagname.length()-5),isdelete);
			else if (suffix.indexOf(':') == -1)
			{
				Type attrtype = attrtypes.get(citype + ":" + suffix);
				if (attrtype == null)
				{
					ClassDefinition classdef = classmodel.getClassDefinition(citype);
					attrtype = classdef.getAttribute(suffix).getType();
					attrtypes.put(citype + ":" + suffix,attrtype);
				}

				String value = field.getValue();

				if (Misc.isLog(30)) Misc.log("Setting CI attribute '" + suffix + "' type:" + attrtype + " with: " + value);
				if (value == null)
					ci.setStringProperty(suffix,null);
				else switch(attrtype)
				{
				case LONG:
					try {
					ci.setLongProperty(suffix,new Long(value));
					} catch (NumberFormatException ex) {
					Misc.log("ERROR: Cannot convert '" + value + "' for field '" + suffix + "' into a long: " + xml);
					}
					break;
				case ENUM:
				case INTEGER:
					try {
					ci.setIntProperty(suffix,new Integer(value));
					} catch (NumberFormatException ex) {
					Misc.log("ERROR: Cannot convert '" + value + "' for field '" + suffix + "' into an integer: " + xml);
					}
					break;
				case LIST:
				case STRING:
					ci.setStringProperty(suffix,value);
					break;
				case BOOLEAN:
					ci.setBooleanProperty(suffix,(value.equals("1") || value.equals("true")));
					break;
				case STRING_LIST:
					ci.setStringListProperty(suffix,value.split("\n"));
					break;
				case DATE:
					ci.setDateProperty(suffix,ucmdb.dateformat.parse(value));
					break;
				default:
					throw new AdapterException("Unsupported uCMDB type " + attrtype + " for field " + suffix);
				}
			}
		}
	}

	private void add(XML xml) throws Exception
	{
		TopologyModificationData data = factory.createTopologyModificationData();
		FillUpdateData(data,xml,null,false);
		update.create(data,CreateMode.UPDATE_EXISTING);
	}

	private void remove(XML xmldest,XML xml) throws Exception
	{
		TopologyModificationData data = factory.createTopologyModificationData();
		XML[] customs = xmldest.getElements("customremove");
		for(XML custom:customs)
		{
			String name = custom.getAttribute("name");
			if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
			String value = custom.getAttribute("value");
			if (value == null) value = "";
			xml.add(name,value);
			if (Misc.isLog(10)) Misc.log("Updating " + name + " with value " + value + " instead of deleting: " + xml);
			FillUpdateData(data,xml,null,true);
			update.update(data);
			return;
		}
		
		FillUpdateData(data,xml,null,true);
		update.delete(data,DeleteMode.IGNORE_NON_EXISTING);
	}

	private void update(XML xml) throws Exception
	{
		TopologyModificationData data = factory.createTopologyModificationData();
		FillUpdateData(data,xml,null,false);
		update.update(data);
	}

	@Override
	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
		String oper = xmloper.getTagName();
		try
		{
			if (oper.equals("add")) add(xmloper);
			else if (oper.equals("remove")) remove(xmldest,xmloper);
			else if (oper.equals("update")) update(xmloper);
		} catch(ExecutionException ex) {
			if (stoponerror) Misc.rethrow(ex);
			Misc.log("ERROR: " + Ucmdb.cleanupException(ex) + ": " + xmloper);
		} catch(Exception ex) {
			Misc.rethrow(ex,"UCMDB API error while processing: " + xmloper);
		}
	}
}
