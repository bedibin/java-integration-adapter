import java.util.*;
import org.tiling.scheduling.*;

class Operation extends SchedulerTask
{
	private String classname;
	protected XML function;
	private Rate rate;
	protected enum ResultTypes { LAST, MERGE, TRANSPARENT };
	private HashMap<String,String> variablelist = new HashMap<String,String>();

	public void run() {}

	public Operation(String classname,XML function) throws Exception
	{
		this.classname = classname;
		this.function = function;
	}

	public Operation(XML function) throws Exception
	{
		this.function = function;
	}

	protected Operation() {}

	public String getName() throws Exception
	{
		String name = this.getClass().getName();
		if (classname != null) name = classname;
		if (function != null)
		{
			String functionname = function.getAttribute("name");
			if (functionname != null) name = functionname;
		}
		return name;
	}

	public String getClassName()
	{
		return classname;
	}

	protected void setOperation(Operation sub) throws Exception
	{
		classname = sub.classname;
		function = sub.function;
	}

	public void close()
	{
	}

	public void closesuper()
	{
		if (classname != null) System.out.print(classname + " ");
	}

	protected XML getFunction()
	{
		return function;
	}

	public synchronized String getVariable(String name)
	{
		return variablelist.get(name);
	}

	private synchronized void setVariable(XML xml,String path,String name) throws Exception
	{
		if (name == null || !name.startsWith("$"))
		{
			Misc.log("WARNING: Trying to set variable " + name + ". Variable name must start with $");
			return;
		}

		String value = (path == null) ? xml.toString() : xml.getStringByPath(path);
		variablelist.put(name,value);
	}

	private void rateElement(XML element) throws Exception
	{
		int level = 1;
		String oper = element.getAttribute("oper");

		if (oper != null && oper.equals("start"))
		{
			rate = new Rate();
			return;
		}

		if (rate == null)
		{
			Misc.log("WARNING: Rate was not initialized. Forcing initialization.");
			rate = new Rate();
			return;
		}

		String levelstr = element.getAttribute("level");
		if (levelstr != null)
			level = new Integer(levelstr);

		String ratestr = rate.toString();

		if (Misc.isLog(level)) Misc.log("RATE" + (classname == null ? "" : " " + classname) + ": " + ratestr + " a second [MAX: " + rate.getMax() + ", COUNT: " + rate.getCount() + "]");
	}

	private void logElement(XML xml,XML element) throws Exception
	{
		int level = 1;
		boolean exception = false;

		String levelstr = element.getAttribute("level");
		if (levelstr != null)
		{
			if (levelstr.equals("exception"))
				exception = true;
			else
				level = new Integer(levelstr);
		}

		String value = element.getValue();
		if (value == null) value = "";
		final XML finalxml = xml;

		String str = Misc.substitute(value,new Misc.Substituer() {
			public String getValue(String param) throws Exception
			{
				if (param.startsWith("$"))
				{
					String var = getVariable(param);
					if (var != null) return var;
				}
				return Misc.substituteGet(param,finalxml.getStringByPath(param));
			}
		});

		String filename = element.getAttribute("dumpfilename");
		if (filename != null)
		{
			String append = element.getAttribute("append");
			Misc.toFile(filename,str,element.getAttribute("charset"),append != null && append.equals("true"));
		}

		if (exception)
			throw new AdapterException(xml,str);
		else
			Misc.log(level,str);
	}

	public static ResultTypes getResultType(XML xml) throws Exception
	{
		String resultstr = xml.getAttribute("result_type");
		if (resultstr != null)
		{
			if (resultstr.equals("merge")) return ResultTypes.MERGE;
			else if (resultstr.equals("transparent")) return ResultTypes.TRANSPARENT;
		}
		return ResultTypes.LAST;
	}

	protected XML runFunction(XML xml,XML function) throws Exception
	{
		if (function == null)
		{
			Misc.log("WARNING: Subscriber with no function!");
			return null;
		}

		if (!Misc.isFilterPass(function,xml)) return null;

		if (Misc.isLog(3)) Misc.log("Processing function " + function.getAttribute("name"));

		XML elements[] = function.getElements(null);
		for(XML element:elements)
		{
			if (javaadapter.isShuttingDown()) return null;

			xml = runMulti(xml,element);
			if (xml == null) break;

			if (Misc.isLog(5)) Misc.log("Result: " + xml);
		}

		return xml;
	}

	public XML runMulti(XML xml,XML element) throws Exception
	{
		ResultTypes resulttype = getResultType(element);

		String name = xml.getTagName();
		if (name != null && name.equals("javaadapter:multi"))
		{
			XML[] xmllist = xml.getElements(null);
			XML resultxml = null;

			for(int i = 0;i < xmllist.length;i++)
			{
				if (javaadapter.isShuttingDown()) return null;

				XML xmlmulti = xmllist[i].copy();
				xml.copyAttributes(xmlmulti);

				XML currentxml = runElementResult(xmlmulti,element,ResultTypes.LAST);
				if (currentxml == null && resulttype == ResultTypes.LAST) break;

				switch(resulttype)
				{
				case LAST:
					resultxml = currentxml;
					break;
				case MERGE:
					if (resultxml == null)
					{
						resultxml = new XML();
						resultxml = resultxml.add("results");
					}
					if (currentxml != null) resultxml.add(currentxml);
					break;
				}
			}

			if (resultxml != null) xml = resultxml;
		}
		else
			xml = runElementResult(xml,element,resulttype);

		return xml;
	}

	static public void addByPath(XML xml,String path,String name,String value) throws Exception
	{
		if (path == null)
		{
			xml.add(name,value);
			return;
		}

		XML[] nodes = xml.getElementsByPath(path);
		for(XML node:nodes)
			node.add(name,value);
	}

	static public void removeByPath(XML xml,String path) throws Exception
	{
		if (path == null) return;

		XML[] nodes = xml.getElementsByPath(path);
		for(XML node:nodes)
			node.remove();
	}

	private void splitPath(XML xml,String path,String split,String name) throws Exception
	{
		if (path == null) return;
		if (name == null) name = "value";

		XML[] nodes = xml.getElementsByPath(path);
		for(XML node:nodes)
		{
			String nodevalue = node.getValue();
			if (nodevalue == null) continue;

			String[] values = nodevalue.split(split);
			for(String value:values)
				node.add(name,value);
		}
	}

	static public void setValueByPath(Operation oper,XML xmlget,XML xmlset,String getpath,String setpath) throws Exception
	{
		if (getpath == null) return;
		if (setpath == null) return;

		String sourcevalue = null;
		if (oper != null) sourcevalue = oper.getVariable(getpath);
		if (sourcevalue == null) sourcevalue = xmlget.getStringByPath(getpath);
		if (sourcevalue == null) return;

		if (Misc.isLog(15)) Misc.log("setValueByPath value: " + sourcevalue);

		xmlset.setValueByPath(setpath,sourcevalue);
	}

	public void setAttributeByPath(XML xmlget,XML xmlset,String getpath,String setpath,String name) throws Exception
	{
		String sourcevalue = getVariable(getpath);
		if (sourcevalue == null) sourcevalue = xmlget.getStringByPath(getpath);
		if (sourcevalue == null) return;

		xmlset.setAttributeByPath(setpath,name,sourcevalue);
	}

	private XML runElementResult(XML xml,XML element,ResultTypes resulttype) throws Exception
	{
		if (!Misc.isFilterPass(element,xml))
			return xml;

		XML previousxml = resulttype == ResultTypes.LAST ? null : xml.copy();

		XML currentxml = runElement(xml,element);
		if (currentxml == null) return null;

		switch(resulttype)
		{
		case LAST:
			return currentxml;
		case MERGE:
			xml = previousxml;
			if (Misc.isLog(20)) Misc.log("Before merge: " + xml);
			xml.add(currentxml);
			if (Misc.isLog(18)) Misc.log("Merged result: " + xml);
			return xml;
		case TRANSPARENT:
			return previousxml;
		}

		return null;
	}

	private XML runElement(XML xml,XML element) throws Exception
	{
		if (Misc.isLog(18)) Misc.log("runElement element:" + element.toString());
		if (Misc.isLog(18)) Misc.log("runElement xml:" + xml.toString());

		String tagname = element.getTagName();
		if (Misc.isLog(3)) Misc.log("Doing " + tagname + "...");

		if (tagname.equals("function"))
			return runFunction(xml,element);
		else if (tagname.equals("element"))
		{
			String filename = element.getAttribute("filename");
			if (filename == null)
				xml = element.getElement(null).copy();
			else
				xml = new XML(filename);
		}
		else if (tagname.equals("lookup"))
			xml.lookup(element);
		else if (tagname.equals("transformation"))
			xml.transform(element);
		else if (tagname.equals("sleep"))
			Thread.sleep(new Integer(element.getValue()));
		else if (tagname.equals("valuepath"))
			setValueByPath(this,xml,xml,element.getAttribute("get"),element.getAttribute("set"));
		else if (tagname.equals("splitpath"))
			splitPath(xml,element.getAttribute("path"),element.getAttribute("value"),element.getAttribute("name"));
		else if (tagname.equals("attributepath"))
			setAttributeByPath(xml,xml,element.getAttribute("get"),element.getAttribute("set"),element.getAttribute("name"));
		else if (tagname.equals("addpath"))
			addByPath(xml,element.getAttribute("path"),element.getAttribute("name"),element.getAttribute("value"));
		else if (tagname.equals("removepath"))
			removeByPath(xml,element.getAttribute("path"));
		else if (tagname.equals("setpath"))
			xml = xml.getElementByPath(element.getAttribute("path"));
		else if (tagname.equals("variablepath"))
			setVariable(xml,element.getAttribute("path"),element.getAttribute("name"));
		else if (tagname.equals("log"))
			logElement(xml,element);
		else if (tagname.equals("rate"))
			rateElement(element);
		else if (tagname.equals("publisher"))
		{
			String type = element.getAttribute("type");
			if (type == null) type = "";

			if (type.equals("soap"))
			{
				String ns = element.getAttribute("ns");
				String request = element.getAttribute("request");
				SoapRequest soap = new SoapRequest(request,ns);

				// Workaround for soap.add crash... Use it if needed.
				// soap.add(new XML(new StringBuffer(xml.rootToString())));

				soap.add(xml);

				xml = soap.publish(element);
			}
			else
			{
				Publisher publisher = Publisher.getInstance();
				xml = publisher.publish(xml,element);
			}
		}

		return xml;
	}
}

class Subscriber extends Operation
{
	protected Subscriber() {}

	public Subscriber(String classname,XML function) throws Exception
	{
		super(classname,function);
	}

	public Subscriber(XML function) throws Exception
	{
		super(function);
	}

	protected XML run(XML xml) throws Exception
	{
		if (Misc.isLog(8)) Misc.log("Message received");
		return runFunction(xml,function);
	}
}
