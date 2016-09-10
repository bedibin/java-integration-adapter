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

	public ReaderServiceManager(XML xml) throws Exception
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
			headers = new LinkedHashSet<String>(first.keySet());
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		LinkedHashMap<String,String> row = getXML(position);
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

class ServiceManagerUpdateSubscriber extends UpdateSubscriber
{
	public ServiceManagerUpdateSubscriber() throws Exception
	{
	}

	private void setValue(XML xml,String name,String value) throws Exception
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

	private String getMessage(XML xml) throws Exception
	{
		if (xml == null)
			throw new AdapterException("Service Manager returned no data");

		String status = xml.getAttribute("status");
		if ("SUCCESS".equals(status)) return null;

		StringBuffer sb = new StringBuffer();
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

	@Override
	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
		oper(xmloper.getParent().getAttribute("name"),xmldest,xmloper);
	}

	protected void oper(String object,XML xmldest,XML xmloper) throws Exception
	{
		XML publisher = new XML();
		XML pub = publisher.add("publisher");
		pub.setAttribute("name",object);
		pub.setAttribute("url",xmldest.getAttribute("url"));
		pub.setAttribute("username",xmldest.getAttribute("username"));
		pub.setAttribute("password",xmldest.getAttribute("password"));
		pub.setAttribute("type","soap");

		XML[] customs = null;
		String oper = xmloper.getTagName();
		String soapoper;
		if (oper.equals("add")) soapoper = "Create";
		else if (oper.equals("remove"))
		{
			customs = xmldest.getElements("customremove");
			soapoper = customs.length > 0 ? "Update" : "Delete";
		}
		else if (oper.equals("update"))
		{
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
							ArrayList<String> deleteset = new ArrayList<String>(Arrays.asList(oldvalue.split("\n")));
							ArrayList<String> newset = new ArrayList<String>(Arrays.asList(newvalue.split("\n")));
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
				if (doremove) oper(object,xmldest,xmlremove);
			}
		}
		else return;

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
			String type = field.getAttribute("type");
			if (type != null)
			{
				if (type.equals("info")) continue;
				if (type.equals("infoapi")) continue;
				if (type.equals("key"))
				{
					keys.add(name,value);
					instance.add(name,value);
					continue;
				}
				if (oper.equals("remove")) continue;
				if (oper.equals("update"))
				{
					XML old = field.getElement("oldvalue");
					if (old == null) continue;
					String oldvalue = old.getValue();
					if (type.equals("initial") && oldvalue != null) continue;
				}
			}

			if (customs != null && customs.length > 1)
			{
				for(XML custom:customs)
				{
					String namecust = custom.getAttribute("name");
					if (namecust == null) throw new AdapterException(custom,"Attribute 'name' required");
					String valuecust = custom.getAttribute("value");
					if (valuecust == null) valuecust = "";
					setValue(instance,namecust,Misc.substitute(valuecust,xmloper));
				}
				continue;
			}

			if (name.startsWith("TABLE_"))
			{
				name = name.substring("TABLE_".length());
				Reader reader = new ReaderCSV(new StringReader(value));
				LinkedHashMap<String,String> row;
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

		XML result = soap.publish(publisher);

		Pattern ondupspattern = null;
		String ondupsmatch = xmldest.getAttribute("on_duplicates_match");
		if (ondupsmatch != null) ondupspattern = Pattern.compile(ondupsmatch);

		String message = getMessage(result);
		if (message != null && oper.equals("add") && ((ondupspattern != null && ondupspattern.matcher(message).find()) || (ondupspattern == null && (message.contains("duplicate key") || message.contains("already associated") || message.contains("d\u00e9j\u00e0 associ\u00e9")))))
		{
			soap.renameTag("Update" + object + "Request");
			publisher.setAttribute("action","Update");

			String keyfields = xmldest.getAttribute("merge_keys");
			if (keyfields != null)
			{
				String[] newkeys = keyfields.split("\\s*,\\s*");
				for(XML key:keys.getElements())
					key.remove();
				for(String key:newkeys)
				{
					XML field = xmloper.getElement(key);
					if (field == null) throw new AdapterException(xmldest,"Invalid key '" + key + "' in merge_keys attribute: " + xmloper);
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
