import java.util.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;

class ReaderServiceManager extends ReaderXML
{
	private XML result;
	private int count;
	private SoapRequest soap;
	private XML xmlconfig;

	public ReaderServiceManager(XML xml) throws Exception
	{
		xmlconfig = xml;
		String request = xmlconfig.getAttribute("query");
		String object = xmlconfig.getAttribute("object");
		soap = new SoapRequest("Retrieve" + object + "ListRequest");
		soap.setAttribute("count","10");
		soap.setAttribute("start","0");
		SoapRequest keys = soap.add("keys");
		keys.setAttribute("query",request.trim());
		result = soap.publish(xmlconfig);
		Misc.log("Result: " + result);
		count = position = 0;
		xmltable = result.getElements("instance");
		LinkedHashMap<String,String> first = getXML(0);
		headers = new ArrayList<String>(first.keySet());
	}

	@Override
	public LinkedHashMap<String,String> next() throws Exception
	{
		LinkedHashMap<String,String> row = getXML(position);
		if (row == null)
		{
			String more = result.getAttribute("more");
			if (!"1".equals(more)) return null;
			count += position;
			soap.setAttribute("start","" + count);
			soap.setAttribute("handle",result.getAttribute("handle"));
			result = soap.publish(xmlconfig);
			xmltable = result.getElements("instance");
			position = 0;
			row = getXML(position);
			if (row == null) return null;
		}

		position++;
		if (Misc.isLog(15)) Misc.log("row [xml]: " + row);
		return row;
	}
}

class ServiceManagerUpdateSubscriber extends UpdateSubscriber
{
	public ServiceManagerUpdateSubscriber() throws Exception
	{
	}

	@Override
	protected void oper(XML xmldest,XML xmloper) throws Exception
	{
	}
}
