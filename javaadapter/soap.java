import java.util.*;

class SoapRequest extends XML
{
	private String prefix;
	private final String nsenv = System.getProperty("javaadapter.soap.nsenv");
	private final String defnsenv = "http://schemas.xmlsoap.org/soap/envelope/";

	private void makeRequest(String request,String ns,XML header)
	{
		XML env = super.add("SOAP-ENV:Envelope");
		env.setAttribute("xmlns:SOAP-ENV",nsenv == null ? defnsenv : nsenv);

		if (ns == null)
			prefix = "";
		else
		{
			env.setAttribute("xmlns:ns1",ns);
			prefix = "ns1:";
		}

		if (header != null)
		{
			try {
				XML headerxml = env.add("SOAP-ENV:Header");
				for(XML el:header.getElements(null))
					headerxml.add(el);
			} catch(AdapterException ex) {}
		}

		XML body = env.add("SOAP-ENV:Body");

		XML xml = body;

		if (request != null)
			xml = body.add(prefix + request);

		super.node = xml.node;
	}

	public SoapRequest(AdapterException ex) throws AdapterXmlException
	{
		XML env = super.add("SOAP-ENV:Envelope");
		env.setAttribute("xmlns:SOAP-ENV",nsenv == null ? defnsenv : nsenv);

		XML body = env.add("SOAP-ENV:Body");
		XML fault = body.add("SOAP-ENV:Fault");

		fault.add("faultcode","Server.InternalError");
		String message = ex.toString();

		fault.add("faultstring",message);

		super.node = fault.node;
	}

	public SoapRequest() throws AdapterXmlException
	{
		makeRequest(null,null,null);
	}

	public SoapRequest(String request) throws AdapterXmlException
	{
		makeRequest(request,null,null);
	}

	public SoapRequest(String request,String ns) throws AdapterXmlException
	{
		makeRequest(request,ns,null);
	}

	public SoapRequest(String request,String ns,XML header) throws AdapterXmlException
	{
		makeRequest(request,ns,header);
	}

	public SoapRequest(XML xml)
	{
		super(xml.dom,xml.node);
	}

	public SoapRequest add(XML node) throws AdapterXmlException
	{
		XML xml = super.add(node);
		SoapRequest request = new SoapRequest(xml);
		request.prefix = prefix;
		return request;
	}

	public SoapRequest add(String name)
	{
		XML xml = super.add(prefix + name);
		SoapRequest request = new SoapRequest(xml);
		request.prefix = prefix;
		return request;
	}

	public SoapRequest add(String name,String value)
	{
		XML xml = super.add(prefix + name,value);
		SoapRequest request = new SoapRequest(xml);
		request.prefix = prefix;
		return request;
	}

	public XML publish(XML xmlconfig) throws AdapterException
	{
		Publisher publisher = Publisher.getInstance();

		XML envsoap = publisher.publish(this,xmlconfig);
		if (Misc.isLog(15)) Misc.log("SOAP response: " + envsoap);
		if (envsoap == null) return null;

		XML bodysoap = envsoap.getElement("Body");
		if (bodysoap == null)
		{
			Misc.log("WARNING: SOAP message returned invalid body element: " + this.rootToString() + "\nResponse: " + envsoap);
			return null;
		}

		XML faultsoap = bodysoap.getElement("Fault");
		if (faultsoap != null)
		{
			publisher.setSessionID(xmlconfig.getAttribute("url"),null);

			String faultdelay = xmlconfig.getAttribute("faultdelay");
			if (faultdelay != null)
				Misc.sleep(new Integer(faultdelay));
			return faultsoap.copy();
		}

		XML response = bodysoap.getElement(null);
		if (response != null)
		{
			response = response.copy();
			envsoap.copyAttributes(response);
		}

		return response;
	}
}

class SoapServer
{
	private ArrayList<Subscriber> sublist = new ArrayList<Subscriber>();
	private XML xmlserver;

	public SoapServer(XML xml) throws AdapterException
	{
		System.out.print("Soap server initialisation... ");

		javaadapter.setForShutdown(this);
		xmlserver = xml;

		sublist = Misc.initSubscribers(xml);

		System.out.println("Done");

		Misc.activateSubscribers(sublist);
	}

	public void close()
	{
		System.out.print("SoapServer ");
	}

	public XML getXML()
	{
		return xmlserver;
	}

	public ArrayList<Subscriber> getSubscribers()
	{
		return sublist;
	}

	public String processXML(String request,Subscriber sub) throws AdapterException
	{
		if (Misc.isLog(9)) Misc.log("SOAP request: " + request);

		XML xml = new XML(new StringBuilder(request));
		HashMap<String,String> attrs = xml.getAttributes();
		xml = xml.getElement("Body");
		if (xml != null)
		{
			attrs.putAll(xml.getAttributes());
			xml = xml.getElement(null);
		}
		if (xml == null)
		{
			Misc.log("WARNING: SOAP message returned is invalid: " + request);
			return null;
		}
		xml = xml.copy();
		if (attrs != null)
		{
			Iterator<String> itr = attrs.keySet().iterator();
			while(itr.hasNext())
			{
				String key = itr.next();
				String ns = attrs.get(key);
				if (ns != null && ns.indexOf("xmlsoap.org") == -1)
					xml.setAttribute(key,ns);
			}
		}

		xml = sub.run(xml);
		if (xml == null) return null;

		SoapRequest response = new SoapRequest();
		response.add(xml);

		return response.rootToString();
	}
}

class SoapLookup
{
	private XML xmlconfig;

	public SoapLookup(XML xmlconfig)
	{
		this.xmlconfig = xmlconfig;
	}

	public void lookup(XML xml) throws AdapterException
	{
		XML requestxml = xmlconfig.getElement("request");
		if (requestxml == null) return;

		XML requestnode = requestxml.getElement(null).copy();

		Operation.setValueByPath(null,xml,requestnode,xmlconfig.getAttribute("getsourcevaluepath"),requestxml.getAttribute("setvaluepath"));

		XML[] valuelist = xmlconfig.getElements("valuepath");
		for(XML valuexml:valuelist)
			Operation.setValueByPath(null,xml,requestnode,valuexml.getAttribute("get"),valuexml.getAttribute("set"));

		String ns = requestxml.getAttribute("ns");
		String name = requestxml.getAttribute("name");

		SoapRequest request = new SoapRequest(name,ns);
		request.add(requestnode);

		if (Misc.isLog(7)) Misc.log("Soap lookup request: " + request.rootToString());

		XML result = request.publish(xmlconfig);
		if (result == null) return;
		if (Misc.isLog(7)) Misc.log("Soap lookup result: " + result.rootToString());

		XML[] extractfields = xmlconfig.getElements("extractfield");
		for(XML extractfield:extractfields)
		{
			XML xmlset = xml;

			String tagname = extractfield.getAttribute("tagname");
			if (tagname != null)
			{
				xmlset = xml.getElement(tagname);
				if (xmlset == null)
					xmlset = xml.add(tagname);
			}

			String resultpath = extractfield.getAttribute("getresultpath");
			if (resultpath != null)
			{
				XML[] xmlpaths = result.getElementsByPath(resultpath);
				xmlset.add(xmlpaths);
			}

			resultpath = extractfield.getAttribute("getresultvaluepath");
			if (resultpath == null) continue;

			String resultpathname = extractfield.getAttribute("getresultvaluepathname");
			String resultname = null;
			if (resultpathname != null)
				resultname = result.getValueByPath(resultpathname,null);

			XML[] resultlist = result.getElementsByPath(resultpath);
			for(XML resultxml:resultlist)
			{
				String value = resultxml.getValue();
				if (resultname == null)
					xmlset.add(resultxml.getTagName(),value);
				else
					xmlset.add(resultname,value);
			}
		}
	}
}

