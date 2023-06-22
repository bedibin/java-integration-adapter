import java.util.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.io.StringReader;

class ReaderServiceManager extends ReaderXML
{
	private XML result;
	private int count;
	private SoapRequest soap;
	private XML xmlconfig;
	private XML publisher;

	public ReaderServiceManager(XML xml) throws AdapterException
	{
		super(xml);

		publisher = new XML();
		XML pub = publisher.add("publisher");
		String name = xml.getAttribute("name");
		if (name == null) name = xml.getParent().getAttribute("name");
		pub.setAttribute("url",xml.getAttribute("url"));
		pub.setAttribute("username",xml.getAttribute("username"));
		pub.setAttribute("password",xml.getAttribute("password"));
		pub.setAttribute("type","soap");
		pub.setAttribute("action","RetrieveList");

		xmlconfig = xml;
		String request = xmlconfig.getAttribute("query");
		String object = xmlconfig.getAttribute("object");
		soap = new SoapRequest("Retrieve" + object + "ListRequest");
		soap.setAttribute("count","10");
		soap.setAttribute("start","0");
		XML keys = soap.add("keys");
		keys.setAttribute("query",request.trim());
		result = soap.publish(publisher);
		Misc.log("Result: " + result);
		count = position = 0;
		xmltable = result.getElements("instance");

		if (headers != null) return;
		LinkedHashMap<String,String> first = getXML(0);
		if (first != null)
			headers = new LinkedHashSet<>(first.keySet());
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		Map<String,String> row = getXML(position);
		if (row == null)
		{
			String more = result.getAttribute("more");
			if (!"1".equals(more)) return null;
			count += position;
			soap.setAttribute("start","" + count);
			soap.setAttribute("handle",result.getAttribute("handle"));
			result = soap.publish(publisher);
			xmltable = result.getElements("instance");
			position = 0;
			row = getXML(position);
			if (row == null) return null;
		}

		position++;

		row = normalizeFields(row);
		if (Misc.isLog(15)) Misc.log("row [xml]: " + row);
		return row;
	}
}

class ReaderServiceManagerRelations extends ReaderUtil
{
	// all_next[parent][child] = ChildExtract
	TreeMap<String,TreeMap<String,ChildExtract>> all_next = new TreeMap<>(db.getCollator());
	Iterator<String> next_iterator;
	Iterator<Map.Entry<String,ChildResult>> child_iterator;
	String current_parent;

	static final String ParentLabel = "parent_ci";
	static final String ChildLabel = "child_ci";
	static final String RelTypeLabel = "relation_name";
	static final String RelParentTypeLabel = "relation_parent";
	static final String RelCiLabel = "relation_ci";
	static final String LevelLabel = "level";
	static final String OutageLabel = "outage";

	static class ChildExtract {
		ChildExtract(String type,boolean outage) {
			this.type = type;
			this.outage = outage;
		}
		String type;
		boolean outage;
	};

	static class ChildResult {
		ChildResult(int level,String type,String ci,boolean outage,String parent_type) {
			this.level = level;
			this.type = type;
			this.parent_type = parent_type;
			this.ci = ci;
			this.outage = outage;
		};

		int level;
		String type;
		String parent_type;
		String ci;
		boolean outage;

		public String toString() {
			return "CI: " + ci + " type: " + type + " level: " + level + " outage: " + outage;
		}
	};

	public ReaderServiceManagerRelations(XML xml) throws AdapterException
	{
		setSorted(true);

		String conn = xml.getAttribute("instance");
		String sql = 
"select m.logical_name " + ParentLabel
	+ ",c.logical_name " + ChildLabel
	+ ",m.relationship_name " + RelTypeLabel
	+ ",m.outage_dependency " + OutageLabel
	+ " " +
"from cirelationsm1 m " +
"inner join device2m1 p on m.logical_name = p.logical_name and p.istatus != 'Retired' " +
"inner join cirelationsa1 a on a.logical_name = m.logical_name and a.relationship_name = m.relationship_name " +
"inner join device2m1 c on a.related_cis = c.logical_name and c.istatus != 'Retired' " +
"order by 1,2,3";
		DBOper oper = db.makesqloper(conn,sql);

		String[] default_headers = {ParentLabel,ChildLabel,RelTypeLabel,RelCiLabel,LevelLabel,OutageLabel,RelParentTypeLabel};
		if (headers == null) headers = new LinkedHashSet<>(Arrays.asList(default_headers));
		if (instance == null) instance = conn;

		/* First step is to create "all_next", a list of all CIs with their associated children */
		Map<String,String> row;
		while((row = oper.next()) != null) {
			String parent = row.get(ParentLabel);
			String child = row.get(ChildLabel);
			String reltype = row.get(RelTypeLabel);
			boolean outage = "t".equals(row.get(OutageLabel));

			if (all_next.containsKey(parent)) {
				all_next.get(parent).put(child,new ChildExtract(reltype,outage));
			} else {
				TreeMap<String,ChildExtract> map = new TreeMap<>(db.getCollator());
				map.put(child,new ChildExtract(reltype,outage));
				all_next.put(parent,map);
			}
		}

		/* Next step is to iterate through all CIs */
		next_iterator = all_next.keySet().iterator();
		/* Create an empty list of all possible descendent */
		child_iterator = (new TreeMap<String,ChildResult>(db.getCollator())).entrySet().iterator();
	}

	private void getAllChildren(TreeMap<String,ChildResult> map,int level,String ci,boolean outage,String parent_relation) throws AdapterException
	{
		TreeMap<String,ChildExtract> children = all_next.get(ci);
		if (children == null) return;
		/* For a specific CI, search recursively for all its descendents and populate the "map" list with the result */
		for(Map.Entry<String,ChildExtract> child:children.entrySet()) {
			String childname = child.getKey();
			ChildExtract childextract = child.getValue();
			boolean childoutage = outage ? childextract.outage : outage;
			String parent_type = parent_relation == null ? childextract.type : parent_relation;
			ChildResult duplicatechild = map.get(childname);
			if (duplicatechild == null)
			{
				ChildResult newchild = new ChildResult(level,childextract.type,ci,childoutage,parent_type);
				map.put(childname,newchild);
			} else {
				/* The descendent was already found in the results, we have to make the decision which one to keep
				   between the one we just got and the one we found previously.
				   The one to keep is:
				   1- If new one has outage but not previous one, for any level, use new one
				   2- If same outage scope and new one is lower level than previous one, use new one
				   3- Otherwise, keep old one
				*/
				if ((childoutage && !duplicatechild.outage) 
					|| (childoutage == duplicatechild.outage && level < duplicatechild.level))
				{
					duplicatechild.level = level;
					duplicatechild.ci = ci;
					duplicatechild.outage = childoutage;
					duplicatechild.type = childextract.type;
					duplicatechild.parent_type = parent_type;
					// No continue statement here since we need to process better children as well
				}
				else
					// New route is worst than old one
					continue;
			}
			getAllChildren(map,level+1,childname,childoutage,parent_type);
		}
		return;
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws AdapterException
	{
		while(!child_iterator.hasNext()) {
			/* We consumed all descendents, select next CI, if any */
			if (!next_iterator.hasNext()) return null;
			current_parent = next_iterator.next();
			TreeMap<String,ChildResult> children = new TreeMap<>();
			/* Get all descendents and iterate through them */
			getAllChildren(children,1,current_parent,true,null);
			child_iterator = children.entrySet().iterator();
		}

		Map.Entry<String,ChildResult> child = child_iterator.next();
		LinkedHashMap<String,String> row = new LinkedHashMap<>();
		ChildResult childresult = child.getValue();
		row.put(ParentLabel,current_parent);
		row.put(ChildLabel,child.getKey());
		row.put(RelTypeLabel,childresult.type);
		row.put(RelParentTypeLabel,childresult.parent_type);
		row.put(RelCiLabel,childresult.ci);
		row.put(LevelLabel,"" + childresult.level);
		row.put(OutageLabel,childresult.outage ? "t" : "f");

		return row;
	}
}

class ServiceManagerUpdateSubscriber extends UpdateSubscriber
{
	public ServiceManagerUpdateSubscriber() throws AdapterException
	{
	}

	private void setValue(XML xml,String name,String value) throws AdapterException
	{
		if (value == null) value = "";
		if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
			value = value.replace(' ','T') + "Z";
		if ("".equals(value)) value = " ";
		char first = value.charAt(0);
		if (first == '~' || first == '>')
			value = "" + first + value;

		xml.add(name).setValue(value);
	}

	private String getMessage(XML xml) throws AdapterException
	{
		if (xml == null)
			throw new AdapterException("Service Manager returned no data");

		String status = xml.getAttribute("status");
		if ("SUCCESS".equals(status)) return null;

		StringBuilder sb = new StringBuilder();
		String messageattr = xml.getAttribute("message");
		if (messageattr != null) sb.append(messageattr);

		XML messagexml = xml.getElement("messages");
		if (messagexml != null)
		{
			XML[] messages = messagexml.getElements("message");
			for(XML message:messages)
			{
				if (sb.length() > 0)
					sb.append(": ");
				sb.append(message.getValue());
			}
		}

		if (sb.length() == 0) return status;
		return sb.toString();
	}

	protected void add(XML xmloper) throws AdapterException
	{
		oper(xmloper.getParent().getAttribute("name"),xmloper);
	}

	protected void remove(XML xmloper) throws AdapterException
	{
		oper(xmloper.getParent().getAttribute("name"),xmloper);
	}

	protected void update(XML xmloper) throws AdapterException
	{
		oper(xmloper.getParent().getAttribute("name"),xmloper);
	}

	protected void start(XML xmloper) throws AdapterException {}
	protected void end(XML xmloper) throws AdapterException {}

	protected void oper(String object,XML xmloper) throws AdapterException
	{
		UpdateDestInfo destinfo = getDestInfo();
		XML publisher = new XML();
		XML pub = publisher.add("publisher");
		pub.setAttribute("name",object);
		pub.setAttribute("url",destinfo.getUrl());
		pub.setAttribute("username",destinfo.getUserName());
		pub.setAttribute("password",destinfo.getPassword());
		pub.setAttribute("type","soap");

		ArrayList<XML> customs = null;
		SyncOper oper = Enum.valueOf(SyncOper.class,xmloper.getTagName().toUpperCase());
		String soapoper;
		switch(oper)
		{
		case ADD:
			customs = destinfo.getCustomList(SyncOper.ADD);
			if (customs.size() > 0)
			{
				String action = customs.get(0).getAttribute("action");
				soapoper = action == null ? "Update" : action;
			}
			else
				soapoper = "Create";
			break;
		case REMOVE:
			customs = destinfo.getCustomList(SyncOper.REMOVE);
			if (customs.size() > 0)
			{
				String action = customs.get(0).getAttribute("action");
				soapoper = action == null ? "Update" : action;
			}
			else
				soapoper = "Delete";
			break;
		case UPDATE:
			soapoper = "Update";
			if ("Relationship".equals(object))
			{
				// Relationship API is special since removed child relationships must be specifically deleted
				boolean doremove = false;
				XML xmlremove = new XML();
				XML remove = xmlremove.add("remove");
				XML[] updates = xmloper.getElements();
				for(XML update:updates)
				{
					if ("LIST_ChildCIs".equals(update.getTagName()))
					{
						String type = update.getAttribute("info");
						if (type != null && type.equals("key")) break;
						String oldvalue = update.getValue("oldvalue",null);
						String newvalue = update.getValue();
						if (oldvalue != null && newvalue != null)
						{
							ArrayList<String> deleteset = new ArrayList<>(Arrays.asList(oldvalue.split("\n")));
							ArrayList<String> newset = new ArrayList<>(Arrays.asList(newvalue.split("\n")));
							deleteset.removeAll(newset);
							if (deleteset.size() > 0)
							{
								remove.add("LIST_ChildCIs",Misc.implode(deleteset,"\n"));
								doremove = true;
							}
						}
					}
					else
						remove.add(update);
				}
				if (doremove) oper(object,xmlremove);
			}
			break;
		default:
			return;
		}

		pub.setAttribute("action",soapoper);
		SoapRequest soap = new SoapRequest(soapoper + object + "Request");
		XML model = soap.add("model");
		XML keys = model.add("keys");
		XML instance = model.add("instance");
		XML[] fields = xmloper.getElements();
		for(XML field:fields)
		{
			String value = field.getValue();
			String name = field.getTagName();
			if (value == null) value = "";
			OnOper type = Field.getOnOper(field,"type");
			if (type != null)
			{
				if (type == OnOper.INFO) continue;
				if (type == OnOper.INFOAPI) continue;
				if (type == OnOper.KEY)
				{
					keys.add(name,value);
					instance.add(name,value);
					continue;
				}
				if (oper == SyncOper.REMOVE) continue;
				if (oper == SyncOper.UPDATE)
				{
					XML old = field.getElement("oldvalue");
					if (old == null) continue;
					String oldvalue = old.getValue();
					if (type == OnOper.INITIAL && oldvalue != null) continue;
				}
			}
			else if (oper == SyncOper.REMOVE) continue;

			if (name.startsWith("TABLE_"))
			{
				name = name.substring("TABLE_".length());
				Reader reader = new ReaderCSV(new StringReader(value));
				Map<String,String> row;
				XML array = instance.add(name);
				Set<String> header = reader.getHeader();
				while((row = reader.next()) != null)
				{
					XML structure = array.add(name);
					for(String key:header)
						setValue(structure,key,row.get(key));
				}
				XML structure = array.add(name);
				for(String key:header)
					setValue(structure,key,null);
				continue;
			}

			if (name.startsWith("LIST_"))
			{
				String namelist = name = name.substring("LIST_".length());
				String[] names = name.split("-");
				if (names.length > 1)
				{
					namelist = names[0];
					name = names[1];
				}

				XML list = instance.add(namelist);
				String[] values = value.split("\n");
				for(String line:values)
					setValue(list,name,line);
				setValue(list,name,null);
				continue;
			}

			XML struct = instance;
			String[] names = name.split("-");
			if (names.length > 1)
			{
				struct = instance.add(names[0]);
				name = names[1];
			}

			setValue(struct,name,value);
		}

		if (customs != null && customs.size() > 0)
		{
			for(XML custom:customs)
			{
				String namecust = custom.getAttribute("name");
				if (namecust == null) throw new AdapterException(custom,"Attribute 'name' required");
				String valuecust = custom.getAttribute("value");
				if (valuecust == null) valuecust = "";
				setValue(instance,namecust,Misc.substitute(valuecust,xmloper));
			}
		}

		XML result = soap.publish(publisher);

		Pattern ondupspattern = destinfo.getOnDupsPattern();
		String message = getMessage(result);
		if (message != null && oper == SyncOper.ADD && ((ondupspattern != null && ondupspattern.matcher(message).find()) || (ondupspattern == null && (message.contains("duplicate key") || message.contains("already associated") || message.contains("d\u00e9j\u00e0 associ\u00e9") || message.contains("already exists") || message.contains("existe d\u00e9j\u00e0")))))
		{
			soap.rename("Update" + object + "Request");
			publisher.setAttribute("action","Update");

			String[] newkeys = destinfo.getMergeKeys();
			if (newkeys != null)
			{
				for(XML key:keys.getElements())
					key.remove();
				for(String key:newkeys)
				{
					XML field = xmloper.getElement(key);
					if (field == null) throw new AdapterException("Invalid key '" + key + "' in merge_keys attribute: " + xmloper);
					String value = field.getValue("oldvalue",null);
					if (value == null) value = field.getValue();

					keys.add(key,value);
				}
			}

			result = soap.publish(publisher);
			message = getMessage(result);
		}

		String fault = result.getValue("faultstring",null);
		if (fault != null) Misc.log("SOAP FAULT ERROR: [" + getKeyValue() + "] " + fault + ": " + xmloper);
		if (message != null) Misc.log("ERROR: [" + getKeyValue() + "] " + message + ": " + xmloper);
	}
}


class ServiceManagerDBProcessor implements DBProcessor
{
	final static byte[] SMBINARRAY = { 0x5f, 0x52, 0x43, 0x46, 0x4d, 0x2a, 0x3d };

	static
	{
		DBProcessorManager.register(new ServiceManagerDBProcessor());
	}

	public String getFieldValue(byte[] bytes) throws AdapterDbException
	{
		try {
			if (!Misc.startsWith(bytes,SMBINARRAY)) return new String(bytes,"iso-8859-1");

			//Misc.log(Misc.toHexString(bytes));
			//Misc.log(new String(bytes));
			StringBuilder sb = new StringBuilder();
			int pos = SMBINARRAY.length;
			int structpos = -1;
			while(pos < bytes.length)
			{
				int len = 0;

				switch(bytes[pos])
				{
				case 0x2d:
					pos++;
					len = (int)bytes[pos];
					break;
				case 0x2c:
					len = 4;
					break;
				case 0x2b:
					len = 3;
					break;
				case 0x2a:
					len = 2;
					break;
				case 0x2f:
					// Empty value
					break;
				case (byte)0x80:
					// Array start
					break;
				case (byte)0x81:
					// Array end
					break;
				case (byte)0x90:
					structpos = 0;
					break;
				case (byte)0x91:
					structpos = -1;
					sb.append("\n");
					break;
				default:
					String str = new String(bytes);
					throw new AdapterDbException("Unsupported 0x" + String.format("%x",bytes[pos]) + "[" + pos + "] SM array " + Misc.toHexString(bytes) + ": " + str);
				}

				pos++;
				if (structpos >= 0)
				{
					if (structpos > 1) sb.append(",");
					structpos++;
					if (structpos > 0)
					{
						sb.append(CsvWriter.escape(new String(Arrays.copyOfRange(bytes,pos,pos+len),"utf-8"),',','"',(char)0,false));
						pos += len;
					}
					continue;
				}

				if (len == 0) continue;
				sb.append(new String(Arrays.copyOfRange(bytes,pos,pos+len),"utf-8"));
				pos += len;
			}
			//Misc.log(sb.toString());

			return sb.toString();
		} catch(java.io.UnsupportedEncodingException | AdapterException ex) {
			throw new AdapterDbException(ex);
		}
	}
}
