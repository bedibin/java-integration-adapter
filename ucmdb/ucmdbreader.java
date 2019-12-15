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

/* Rules for relations:
First prefix including the semicolumn is optionnal
END1:END2:REL = END2:END1:RREL
END1::REL = END1:RREL
END2:REL = END2::RREL
If multiple relations between 2 CIs: END1:END2:REL_x
*/

enum BulkType { BULK, DATA };

class Ucmdb
{
	private static Ucmdb instance;
	private UcmdbService service;
	private String adapterinfo;
	private int bulksize = 1;
	private BulkType bulktype = BulkType.BULK;

	private Ucmdb() throws Exception
	{
		adapterinfo = "javaadapter/" + javaadapter.getName();

		System.out.print("Connection to UCMDB... ");
		XML xml = javaadapter.getConfiguration().getElementByPath("/configuration/connection[@type='ucmdb']");
		if (xml == null) throw new AdapterException("No connection element with type 'ucmdb' specified");

		String host = xml.getValue("server","localhost");
		String protocol = xml.getValue("protocol","http");
		String portstr = xml.getValue("port","8080");
		int port = new Integer(portstr);
		String username = xml.getValue("username","admin");
		String password = xml.getValueCrypt("password");

		XML bulkxml = xml.getElement("bulksize");
		if (bulkxml != null)
		{
			String bulkprop = System.getProperty("javaadapter.ucmdb.bulksize");
			String bulkstr = bulkprop == null ? bulkxml.getValue() : bulkprop;
			bulksize = new Integer(bulkstr);
			bulktype = bulkxml.getAttributeEnum("type",BulkType.BULK,BulkType.class);
		}

		UcmdbServiceProvider serviceProvider = UcmdbServiceFactory.getServiceProvider(protocol,host,port);
		ClientContext clientContext = serviceProvider.createClientContext(adapterinfo);
		Credentials credentials = serviceProvider.createCredentials(username,password);
		int retry = 0;
		while(true)
		{
			try {
				service = serviceProvider.connect(credentials,clientContext);
				break;
			} catch (Exception ex) {
				retry++;
				if (retry > 3)
					Misc.rethrow(ex);
				Thread.sleep(10000);
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

	public synchronized static Ucmdb getInstance() throws Exception
	{
		if (instance == null)
			instance = new Ucmdb();
		return instance;
	}

	public int getBulkSize() { return bulksize; }
	public BulkType getBulkType() { return bulktype; }

	static final Pattern causedbypattern = Pattern.compile("Caused by:\\s.*?:\\s+(\\S.+)$",Pattern.MULTILINE);

	public static String cleanupException(Exception ex)
	{
		String error = ex.getMessage();
		if (error == null) return ex.getClass().getName();

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

class ReaderUCMDB extends ReaderUtil
{
	Iterator<TopologyCI> cis;
	Topology topo;
	String rootname;
	Map<UcmdbId,Set<String>> mapids;
	Ucmdb ucmdb = Ucmdb.getInstance();

	public ReaderUCMDB(XML xml) throws Exception
	{
		super(xml);

		String queryname = xml.getAttribute("query");
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

	private void setProperties(String prefix,LinkedHashMap<String,String> row,Element element)
	{
		String post = "";
		if (prefix != null) post = prefix + ":";

		row.put(post + "INFO",element.getType());
		row.put(post + "ID",element.getId().getAsString());

		for(Property property:element.properties())
		{
			Object value = property.getValue();
			if (property.getType() == Type.DATE)
			{
				Date date = (Date)value;
				row.put(post + property.getName(),Misc.gmtdateformat.format(date));
			}
			else
				row.put(post + property.getName(),value == null ? "" : value.toString().trim());
		}
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

	void getRelated(TopologyCI current,String prefix,HashMap<UcmdbId,String> ids,HashMap<String,Integer> counts,LinkedHashMap<String,String> row) throws Exception
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

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		if (!cis.hasNext())
		{
			if (!topo.hasNextChunk()) return null;
			topo = topo.getNextChunk();
			Collection<TopologyCI> topocis = topo.getCIsByName(rootname);
			if (Misc.isLog(30)) Misc.log("TopologyCIS next chunk: " + topocis);
			cis = topocis.iterator();
			if (!cis.hasNext()) return null;
		}

		HashMap<UcmdbId,String> ids = new HashMap<UcmdbId,String>();
		HashMap<String,Integer> counts = new HashMap<String,Integer>();
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
		TopologyCI root = cis.next();

		ids.put(root.getId(),null);
		setProperties(null,row,root);

		getRelated(root,null,ids,counts,row);
		row = normalizeFields(row);

		if (Misc.isLog(30)) Misc.log("uCMDB row: " + row);

		return row;
	}
}

class UCMDBUpdateSubscriber extends UpdateSubscriber
{
	class AttributeType
	{
		Type type;
		String enumname;
		Collection<String> enumvalues;
	}

	private TopologyUpdateService update;
	private TopologyUpdateFactory factory;
	private TopologyModificationData bulkdata;
	private TopologyModificationBulk bulk;
	private int bulkcount = 0;
	private SyncOper bulkoper = SyncOper.START;
	private EnumMap<SyncOper,TopologyModificationAction> actionmap;
	private ClassModelService classmodel;
	private HashMap<String,AttributeType> attrtypes = new HashMap<String,AttributeType>();
	private Ucmdb ucmdb;
	private String adapterinfo;
	private final Pattern relationTagPattern = Pattern.compile("^((([^:]+):)?([^:]*):((REL|RREL)(_(\\d+))*)):INFO$");

	public UCMDBUpdateSubscriber() throws Exception
	{
		ucmdb = Ucmdb.getInstance();
		update = ucmdb.getUpdate();
		factory = update.getFactory();
		classmodel = ucmdb.getClassModel();
		adapterinfo = ucmdb.getInfo();

		actionmap = new EnumMap<SyncOper,TopologyModificationAction>(SyncOper.class);
		actionmap.put(SyncOper.UPDATE,TopologyModificationAction.UPDATE_IF_EXISTS);
		actionmap.put(SyncOper.ADD,TopologyModificationAction.CREATE_OR_UPDATE);
		actionmap.put(SyncOper.REMOVE,TopologyModificationAction.DELETE_IF_EXISTS);
	}

	private void FillUpdateData(TopologyModificationData data,XML xml,SyncOper oper) throws Exception
	{
		final int debug = 15;
		HashMap<String,Element> elements = new HashMap<String,Element>();
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
			if (value == null) throw new AdapterException(xml,"Value for element " + tagname + " cannot be empty");

			if (oper == SyncOper.UPDATE)
			{
				XML old = field.getElement("oldvalue");
				if (old != null)
				{
					OnOper type = Field.getOnOper(field,"type");
					String oldvalue = old.getValue();
					if (type == OnOper.INITIAL && oldvalue != null)
						value = oldvalue;
				}

				if (idxml != null)
				{
					id = idxml.getValue();
					if (id == null) id = idxml.getValue("oldvalue",null);
				}
			}

			if (end1id != null && end2id != null)
			{
				if (Misc.isLog(debug)) Misc.log("UCMDBAPI: Defining relation type " + value + " with ID " + prefix + " between " + end1id + " and " + end2id);
				Relation relation = factory.createRelation(value,factory.restoreCIIdFromString(end1id),factory.restoreCIIdFromString(end2id));
				elements.put(prefix,relation);
				data.addRelation(relation);
				continue;
			}

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
				element.setDoubleProperty(suffix,new Double(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a double: " + xml);
				}
				break;
			case FLOAT:
				try {
				element.setFloatProperty(suffix,new Float(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a float: " + xml);
				}
				break;
			case LONG:
				try {
				element.setLongProperty(suffix,new Long(value));
				} catch (NumberFormatException ex) {
				Misc.log("WARNING: [" + getKeyValue() + "] Cannot convert '" + value + "' for field '" + suffix + "' into a long: " + xml);
				}
				break;
			case ENUM:
			case INTEGER:
				try {
				element.setIntProperty(suffix,new Integer(value));
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
				element.setDateProperty(suffix,Misc.gmtdateformat.parse(value));
				break;
			case BYTES:
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				GZIPOutputStream gzip = new GZIPOutputStream(bos);
				gzip.write(value.getBytes());
				gzip.close();
				element.setBytesProperty(suffix,bos.toByteArray());
				bos.close();
				break;
			default:
				throw new AdapterException("Unsupported uCMDB type " + attrtype.type + " for field " + suffix);
			}
		}
	}

	private void push(XML xml,SyncOper oper) throws Exception
	{
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

	private void flush()
	{
		if (bulkdata != null)
		{
			TopologyModificationData currentbulkdata = bulkdata;
			SyncOper currentoper = bulkoper;
			bulkdata = null;
			bulkcount = 0;
			bulkoper = SyncOper.START;

			SingleTopologyModification modif = factory.createSingleTopologyModification(adapterinfo + "/" + currentoper,currentbulkdata,actionmap.get(currentoper));
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
		}

		if (bulk != null)
		{
			TopologyModificationBulk currentbulk = bulk;
			bulk = null;
			bulkcount = 0;
			update.execute(currentbulk);
		}
	}

	protected void add(XML xmldest,XML xml) throws Exception
	{
		XML[] customs = xmldest.getElements("customadd");
		for(XML custom:customs)
		{
			String name = custom.getAttribute("name");
			if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
			String value = custom.getAttribute("value");
			if (value == null) value = "";
			value = Misc.substitute(value,xml);
			xml.add(name,value);
			if (Misc.isLog(10)) Misc.log("Updating " + name + " with value " + value + " instead of adding: " + xml);
		}

		if (customs.length > 0)
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

	private String getAddValue(XML xml) throws Exception
	{
		if (xml == null) return null;
		return xml.getValue();
	}

	private String getAddValue(XML xml,String name) throws Exception
	{
		XML idxml = xml.getElement(name);
		return getAddValue(idxml);
	}

	private String getUpdateValue(XML xml) throws Exception
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

	private String getUpdateValue(XML xml,String name) throws Exception
	{
		XML idxml = xml.getElement(name);
		return getUpdateValue(idxml);
	}

	private void updatemulti(SyncOper oper,XML xml) throws Exception
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

	protected void remove(XML xmldest,XML xml) throws Exception
	{
		XML[] customs = xmldest.getElements("customremove");
		for(XML custom:customs)
		{
			String name = custom.getAttribute("name");
			if (name == null) throw new AdapterException(custom,"Attribute 'name' required");
			String value = custom.getAttribute("value");
			if (value == null) value = "";
			value = Misc.substitute(value,xml);
			XML xmlval = xml.add(name,value);
			xmlval.add("oldvalue");
			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Updating " + name + " with value " + value + " instead of deleting: " + xml);
		}

		if (customs.length > 0)
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

				push(xml,SyncOper.REMOVE);
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

	protected void update(XML xmldest,XML xml) throws Exception
	{
		String old1 = getUpdateValue(xml,"END1");
		String old2 = getUpdateValue(xml,"END2");
		if (old1 != null && old2 != null)
		{
			String[] old1values = old1.split("\n");
			String[] old2values = old2.split("\n");

			if (Misc.isLog(10)) Misc.log("UCMDBAPI: Update causing relationship recreation: " + xml);

			for(String old1value:old1values) for(String old2value:old2values)
			{
				XML delete = new XML();
				delete = delete.add("remove");
				delete.add("END1",old1value);
				delete.add("END2",old2value);
				delete.add("INFO",xml.getValue("INFO",null));

				push(delete,SyncOper.REMOVE);
			}
			add(xmldest,xml);
			return;
		}

		updatemulti(SyncOper.UPDATE,xml);
	}

	protected void start(XML xmldest,XML xml) throws Exception {}

	protected void end(XML xmldest,XML xml) throws Exception
	{
		flush();
	}

	@Override
	protected void setKeyValue(XML xml) throws Exception
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
		ArrayList<String> parameterValue = new ArrayList<String>();
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
		return longName.substring(0, getMaxNameLengthAllow() - 12) + "_" + String.valueOf(Math.abs(longName.hashCode()));
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
