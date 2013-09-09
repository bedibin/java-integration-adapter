import java.util.*;

class ObjectRequestSubscriber
{
	public enum operations { get_object, update_object, insert_object, delete_object };
	private operations operation;
	private XML function;
	private Publisher publisher;
	private String domain;

	public ObjectRequestSubscriber(operations operation) throws Exception
	{
		ASI asi = ASI.getInstance();
		function = asi.xml.getElementByPath("/configuration/function[@name='" + operation.toString() + "']");
 		domain = asi.domain;
		if (function == null) throw new AdapterException("Function " + operation.toString() + " not available in XML file");
		this.operation = operation;
		publisher = Publisher.getInstance();
	}

	public void close()
	{
	}

	private ASIobject[] filter(XML xml) throws Exception
	{
		String objectname = xml.getValue("className");
		if (objectname == null) return null;

		String instid = xml.getValue("InstId",null);

		if (instid == null)
			instid = xml.getValueByPath("//filter[name='InstId']/value",null);

		if (instid != null)
		{
			try
			{
				ASIobject object = new ASIobject(objectname,new Long(instid));
				return new ASIobject[] { object };
			}
			catch(RuntimeException ex)
			{
				Misc.log(ex);
				return null;
			}
		}

		ASIobject object = new ASIobject(objectname);

		XML[] xmllist = xml.getElements("filter");
		for(XML el:xmllist)
		{
			String name = el.getValue("name");
			String value = el.getValue("value");
			String wild = el.getValue("wild","no");

			try
			{
				object.setWild(name,wild.equalsIgnoreCase("yes"));
			}
			catch(Exception ex)
			{
			}

			object.setAttribute(name,value);
		}

		xmllist = xml.getElements("udaFilter");
		for(XML el:xmllist)
		{
			String name = el.getValue("name");
			String value = el.getValue("value");
			String group = el.getValue("udaGroup");

			object.setUda(group,name,value);
		}

		ASIobject[] objects = object.query();
		if (objects == null) return null;

		return objects;
	}

	private void execute(XML xml,ASIobject object) throws Exception
	{
		if (operation == operations.get_object) return;

		if (Misc.isLog(3)) Misc.log("Execute operation " + operation.toString());

		XML[] fields = xml.getElements("updateFields");
		if (fields.length == 0)
			fields = xml.getElements("insertFields");

		for(XML field:fields)
		{
			String name = field.getValue("name");
			String value = field.getValue("value");
			String group = field.getValue("udaGroup",null);

			if (group == null)
				object.setAttribute(name,value);
			else
				object.setUda(group,name,value);
		}

		if (operation == operations.insert_object)
			object.insert();
		else if (operation == operations.delete_object)
			object.delete();
		else if (operation == operations.update_object)
			object.update();
	}

	private void get(XML xml,ASIobject object) throws Exception
	{
		XML node = xml.add(object.getClassName().toLowerCase());
		if (domain != null) node.add("Database",domain);

		Hashtable<String,Object> result = object.getAllAttributes();
		Iterator<String> itr = result.keySet().iterator();
		while(itr.hasNext())
		{
			String key = itr.next();
			String value = (String)result.get(key);
			if (Misc.isLog(15)) Misc.log("Key: " + key + ", value:" + value);
			node.add(key,value);
		}
	}

	public void process(XML xml) throws Exception
	{
		XML classxml = xml.getElement("class");
		if (classxml != null) xml = classxml;

		String objectname = xml.getValue("className");
		if (objectname == null) return;

		if (Misc.isLog(3)) Misc.log(operation.toString());

		XML doc = new XML();
		XML root = doc.add(operation.toString());

		String transactionid = xml.getValue("transactionId",null);
		if (transactionid != null) root.add("transactionId",transactionid);

		try
		{
			if (operation == operations.insert_object)
			{
				ASIobject object = new ASIobject(objectname);
				execute(xml,object);
				get(root,object);
			}
			else
			{
				ASIobject[] objects = filter(xml);
				if (objects != null) for(ASIobject object:objects)
				{
					if (Misc.isLog(5)) Misc.log("object " + object);
					execute(xml,object);
					if (operation != operations.delete_object)
						get(root,object);
				}
			}
			root.add("status","Success");
		}
		catch(Exception ex)
		{
			root.add("status","Failure");
			String error = "";
			for(Throwable cause = ex;cause != null;cause = cause.getCause())
				error += cause.toString();
			root.add("error_desc",error);
			Misc.log(ex);
		}

		if (Misc.isLog(8)) Misc.log("Pre result: " + doc.toString());

		XML[] xmllist = function.getElements("transformation");
		for(XML el:xmllist)
			doc = doc.transform(el.getValue());

		if (Misc.isLog(5)) Misc.log("Result: " + doc.toString());
		publisher.publish(doc,function);
	}
}

class GetObjectRequestSubscriber extends Subscriber
{
	private ObjectRequestSubscriber request;

	public GetObjectRequestSubscriber() throws Exception
	{
 		request = new ObjectRequestSubscriber(ObjectRequestSubscriber.operations.get_object);
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		request.process(xml);
		return null;
	}
	public void close()
	{
		request.close();
	}
}

class UpdateObjectRequestSubscriber extends Subscriber
{
	private ObjectRequestSubscriber request;

	public UpdateObjectRequestSubscriber() throws Exception
	{
 		request = new ObjectRequestSubscriber(ObjectRequestSubscriber.operations.update_object);
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		request.process(xml);
		return null;
	}
	public void close()
	{
		request.close();
	}
}

class InsertObjectRequestSubscriber extends Subscriber
{
	private ObjectRequestSubscriber request;

	public InsertObjectRequestSubscriber() throws Exception
	{
 		request = new ObjectRequestSubscriber(ObjectRequestSubscriber.operations.insert_object);
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		request.process(xml);
		return null;
	}
	public void close()
	{
		request.close();
	}
}

class DeleteObjectRequestSubscriber extends Subscriber
{
	private ObjectRequestSubscriber request;

	public DeleteObjectRequestSubscriber() throws Exception
	{
 		request = new ObjectRequestSubscriber(ObjectRequestSubscriber.operations.delete_object);
	}

	@Override
	public XML run(XML xml) throws Exception
	{
		request.process(xml);
		return null;
	}
	public void close()
	{
		request.close();
	}
}

