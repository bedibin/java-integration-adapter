import java.io.*;
import java.util.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;
import javax.xml.transform.dom.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;
import javax.jms.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.apache.xml.serialize.*;
import java.util.regex.*;

class XML
{
	protected Document dom;
	protected Node node;
	private static ArrayList<XML> defaults = new ArrayList<XML>();
	private static HashMap<String,String> defaultvars = new HashMap<String,String>();
	final static String LINE_NUMBER_KEY_NAME = "lineNumber";
	Stack<Element> elementStack = new Stack<Element>();
	StringBuilder textBuffer = new StringBuilder();
	SAXParser parser;
	private static Pattern xmlvaluepattern = Pattern.compile("[^\\x09\\x0A\\x0D\\x20-\\xD7EF\\xE000-\\xFFFD\\x10000-x10FFFF]");

	private DefaultHandler handler = new DefaultHandler()
	{
		private Locator locator;

		public void setDocumentLocator(Locator locator)
		{
                	this.locator = locator; // Save the locator, so that it can be used later for line tracking when traversing nodes.
		}

		public void startElement(String uri,String localName,String qName,Attributes attributes) throws SAXException
		{
			addTextIfNeeded();
			Element el = dom.createElement(qName);
			for (int i = 0; i < attributes.getLength(); i++)
			{
				el.setAttribute(attributes.getQName(i),attributes.getValue(i));
			}
			el.setUserData(LINE_NUMBER_KEY_NAME,String.valueOf(this.locator.getLineNumber()),null);
			elementStack.push(el);
		}

		public void endElement(String uri,String localName,String qName)
		{
			addTextIfNeeded();
			Element closedEl = elementStack.pop();
			if (elementStack.isEmpty()) // Is this the root element?
				dom.appendChild(closedEl);
			else
			{
				Element parentEl = elementStack.peek();
				parentEl.appendChild(closedEl);
			}
		}

		public void characters(char ch[],int start,int length) throws SAXException
		{
			textBuffer.append(ch,start,length);
		}

		// Outputs text accumulated under the current node
		private void addTextIfNeeded()
		{
			if (textBuffer.length() == 0) return;
			Element el = elementStack.peek();
			Node textNode = dom.createTextNode(textBuffer.toString());
			el.appendChild(textNode);
			textBuffer.delete(0, textBuffer.length());
		}
	};

	private boolean isElement(Node node)
	{
		if (node == null) return false;
		return node instanceof Element;
	}

	private Node getRootNode() throws Exception
	{
		Node node = dom.getDocumentElement();
		if (node == null) return null;
		return node;
	}

	private void init() throws Exception
	{
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setXIncludeAware(true);
		parser = factory.newSAXParser();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		dom = db.newDocument();
	}

	public XML(String filename) throws Exception
	{
		init();
		parser.parse(new File(javaadapter.getCurrentDir(),filename),handler);
		node = getRootNode();
	}

	public XML(String filename,String root) throws Exception
	{
		init();
		parser.parse(new File(javaadapter.getCurrentDir(),filename),handler);
		node = getElementByPath(root).node;
	}

	public XML(TextMessage msg) throws Exception
	{
		init();
		String txt = msg.getText();
		parser.parse(new InputSource(new StringReader(txt)),handler);
		node = getRootNode();
	}

	public XML(TextMessage msg,String root) throws Exception
	{
		init();
		String txt = msg.getText();
		parser.parse(new InputSource(new StringReader(txt)),handler);
		node = getElementByPath(root).node;
	}

	public XML(StringBuffer sb) throws Exception
	{
		init();
		String txt = sb.toString();
		try
		{
			parser.parse(new InputSource(new StringReader(txt)),handler);
		}
		catch(Exception ex)
		{
			Misc.rethrow(ex,"Invalid XML: " + txt);
		}
		node = getRootNode();
	}

	public XML(StringBuffer sb,String root) throws Exception
	{
		init();
		String txt = sb.toString();
		parser.parse(new InputSource(new StringReader(txt)),handler);
		node = getElementByPath(root).node;
	}

	protected XML(Document root,Node node) throws Exception
	{
		dom = root;
		this.node = node;
	}

	public XML() throws Exception
	{
		init();
		node = getRootNode();
	}

	public XML copy() throws Exception
	{
		XML xml = new XML();
		xml.add(this);
		xml.node = xml.getRootNode();
		return xml;
	}

	public Document getDocument()
	{
		return dom;
	}

	public void setRoot() throws Exception
	{
		node = getRootNode();
	}

	public XML getParent() throws Exception
	{
		if (node == null) return null;
		return new XML(dom,node.getParentNode());
	}

	public void write(String filename) throws Exception
	{
		TransformerFactory factory = TransformerFactory.newInstance();
		Transformer xformer = factory.newTransformer();
		Source source = new DOMSource(node == null ? dom : node);
		FileWriter fw = new FileWriter(new File(javaadapter.getCurrentDir(),filename));
		Result result = new StreamResult(fw);
		xformer.transform(source,result);
	}

	@SuppressWarnings("deprecation")
	public String toString(Node node)
	{
		try
		{
			OutputFormat format = new OutputFormat(dom);
			StringWriter out = new StringWriter();
			XMLSerializer serial = new XMLSerializer(out,format);
			if (node == null || node == dom)
				serial.serialize(dom);
			else
			{
				if (!isElement(node)) return "";
				Element el = (Element)node;
				serial.serialize(el);
			}
			return out.toString();
		}
		catch(Exception ex)
		{
			Misc.log(ex);
		}
		return null;
	}

	public String toString()
	{
		return toString(node);
	}

	public String rootToString()
	{
		return toString(dom);
	}

	public String getLine()
	{
		return (String)(node.getUserData(LINE_NUMBER_KEY_NAME));
	}

	public XML transform(String filename) throws Exception
	{
		try
		{
			TransformerFactory factory = TransformerFactory.newInstance();
			Templates template = factory.newTemplates(new StreamSource(new FileInputStream(new File(javaadapter.getCurrentDir(),filename))));
			Transformer xformer = template.newTransformer();
			Source source = new DOMSource(dom);

			XML xml = new XML();
			Result result = new DOMResult(xml.dom);
			xformer.transform(source,result);
			xml.node = xml.getRootNode();
			if (!(xml.node instanceof Element))
				throw new AdapterException("Transformation didn't return a node");
			return xml;
		}
		catch(Exception ex)
		{
			Misc.log(1,"XML transformation [" + filename + "]: " + toString());
			Misc.rethrow(ex);
		}
		return this;
	}

	public void transform(XML xmlinfo) throws Exception
	{
		XML xml = this;

		XML[] transformations = xmlinfo.getElements("transformation");
		if (transformations.length == 0 && xmlinfo.getTagName().equals("transformation") && Misc.checkActivate(xmlinfo) != null)
			transformations = new XML[] { xmlinfo };

		for(XML transformation:transformations)
		{
			if (Misc.isFilterPass(transformation,xml))
				xml = xml.transform(transformation.getValue());
		}

		dom = xml.dom;
		node = xml.node;
	}

	public void lookup(XML xmlinfo) throws Exception
	{
		XML xml = this;

		XML[] lookups = xmlinfo.getElements("lookup");
		if (lookups.length == 0 && xmlinfo.getTagName().equals("lookup"))
			lookups = new XML[] { xmlinfo };

		for(XML lookup:lookups)
		{
			String type = lookup.getAttribute("type");
			if (type == null) continue;

			boolean handleexception = false;
			String exception = lookup.getAttribute("exception");
			if (exception != null && exception.equals("false")) handleexception = true;

			try
			{
				if (type.equals("soap"))
				{
					SoapLookup soaplookup = new SoapLookup(lookup);
					soaplookup.lookup(xml);
				}
				else if (type.equals("db"))
				{
					Object db = Misc.invokeStatic("DB","getInstance");
					Misc.invoke(db,"lookup",lookup,xml);
				}
			}
			catch(Exception ex)
			{
				if (!handleexception) Misc.rethrow(ex);
				Misc.log(ex);

				String msg = ex.getMessage();
				if (msg == null) msg = ex.getCause() == null ? ex.toString() : ex.getCause().getMessage();
				xml.add("exception",msg);
			}
		}

		dom = xml.dom;
		node = xml.node;
	}

	public String getTagName() throws Exception
	{
		if (!isElement(node)) return null;
		Element el = (Element)node;
		return el.getTagName();
	}

	public void renameTag(String name) throws Exception
	{
		dom.renameNode(node,null,name);
	}

	public String getAttributeCrypt(String name) throws Exception
	{
		String value = getAttribute(name);
		if (value == null) return null;

		return javaadapter.crypter.decrypt(value);
	}

	public LinkedHashMap<String,String> getAttributes() throws Exception
	{
		if (node == null) return null;
		LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();

		NamedNodeMap map = node.getAttributes();
		for(int i = 0;i < map.getLength();i++)
		{
			Node attr = map.item(i);
			row.put(attr.getNodeName(),attr.getNodeValue());
		}

		return row;
	}

	public synchronized boolean isAttribute(String name) throws Exception
	{
		if (!isElement(node)) return false;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);

		if (attr == null)
		{
			for(XML def:defaults)
			{
				String attribute = def.getAttribute("attribute");
				if (!name.equals(attribute)) continue;
				String tagname = def.getAttribute("tagname");
				if (tagname != null && !tagname.equals(el.getTagName())) continue;
				String defname = def.getAttribute("name");
				if (defname != null)
				{
					String nodename = this.getAttribute("name");
					if (nodename == null || !nodename.equals(defname)) continue;
				}
				if (!Misc.isFilterPass(def,this)) continue;
				return true;
			}

			return false;
		}
		return true;
	}

	public String getAttributeDeprecated(String name) throws Exception
	{
		if (!isElement(node)) return null;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);
		if (attr == null) return null;
		return attr.getValue();
	}

	public String getAttribute(String name) throws Exception
	{
		if (!isElement(node)) return null;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);
		String value = el.getAttribute(name);

		if (attr == null)
		{
			for(XML def:defaults)
			{
				String attribute = def.getAttribute("attribute");
				if (!name.equals(attribute)) continue;
				String tagname = def.getAttribute("tagname");
				if (tagname != null && !tagname.equals(el.getTagName())) continue;
				String defname = def.getAttribute("name");
				if (defname != null)
				{
					String nodename = this.getAttribute("name");
					if (nodename == null || !nodename.equals(defname)) continue;
				}
				if (!Misc.isFilterPass(def,this)) continue;
				value = def.getAttribute("value");
				if (value == null) def.getValue();
				if (value == null) return null;
				return value;
			}

			return null;
		}

		if (value.length() == 0) return null;

		return value;
	}

	public String getValueCrypt() throws Exception
	{
		String value = getValue();
		if (value == null) return null;

		return javaadapter.crypter.decrypt(value);
	}

	public String getValue() throws Exception
	{
		if (node == null) return null;

		String filename = getAttribute("filename");
		if (filename != null)
			return Misc.readFile(filename);

		Node firstchild = node.getFirstChild();
		if (firstchild == null) return null;

		String value = firstchild.getNodeValue();
		if (value == null || value.length() == 0) return null;
		return value;
	}

	public String getValueCrypt(String name) throws Exception
	{
		String value = getValue(name);
		if (value == null) return null;

		return javaadapter.crypter.decrypt(value);
	}

	public String getValue(String name) throws Exception
	{
		return getValue(name,false);
	}

	public String getValue(String name,String defvalue) throws Exception
	{
		return getValue(name,defvalue,false);
	}

	public String getValueByPath(String name,String defvalue) throws Exception
	{
		return getValue(name,defvalue,true);
	}

	public String getValueByPath(String name) throws Exception
	{
		return getValue(name,true);
	}

	public String getValue(String name,boolean checkpath) throws Exception
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) throw new AdapterException(this,"XML doesn't contain " + name + " element");
		return xmllist[0].getValue();
	}

	public String getValue(String name,String defvalue,boolean checkpath) throws Exception
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) return defvalue;
		return xmllist[0].getValue();
	}

	public XML getElement(String name) throws Exception
	{
		return getElement(name,false);
	}

	public XML getElementByPath(String name) throws Exception
	{
		return getElement(name,true);
	}

	public XML getElement(String name,boolean checkpath) throws Exception
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) return null;
		return xmllist[0];
	}

	public XML[] getElements() throws Exception
	{
		return getElements(null);
	}

	public XML[] getElements(String name) throws Exception
	{
		return getElements(name,false);
	}

	public XML[] getElementsByPath(String name) throws Exception
	{
		return getElements(name,true);
	}

	public XML[] getElements(String name,boolean checkpath) throws Exception
	{
		ArrayList<XML> xmllist = new ArrayList<XML>();
		if (node == null) return new XML[0];

		Node currentnode = node.getFirstChild();

		while(currentnode != null)
		{
			if (isElement(currentnode))
			{
				Element el = (Element)currentnode;
				String tagname = el.getTagName();
				int pos = tagname.indexOf(':');
				if (name != null && pos != -1 && name.indexOf(':') == -1)
					tagname = tagname.substring(pos+1);

				if (name == null || name.equals(tagname))
				{
					XML xml = new XML(dom,currentnode);
					xml = Misc.checkActivate(xml);
					if (xml != null)
						xmllist.add(xml);
				}
			}

			currentnode = currentnode.getNextSibling();
		}

		if (checkpath && xmllist.size() == 0 && name != null)
		{
			XPathFactory factory = XPathFactory.newInstance();
			XPath xpath = factory.newXPath();
			XPathExpression expr = xpath.compile(name);

			Object result = expr.evaluate(node,XPathConstants.NODESET);

			NodeList nl = (NodeList)result;
			for(int i = 0;i < nl.getLength();i++)
			{
				XML xml = new XML(dom,nl.item(i));
				xml = Misc.checkActivate(xml);
				if (xml != null)
					xmllist.add(xml);
			}
		}

		XML[] xmlarray = new XML[xmllist.size()];
		return xmllist.toArray(xmlarray);
	}

	public String getStringByPath(String name) throws Exception
	{
		if (node == null) return "";

		XPathFactory factory = XPathFactory.newInstance();
		XPath xpath = factory.newXPath();
		XPathExpression expr = xpath.compile(name);

		String result = null;
		try
		{
			Object obj = expr.evaluate(node,XPathConstants.NODESET);
			NodeList nl = (NodeList)obj;
			for(int i = 0;i < nl.getLength();i++)
			{
				Node node = nl.item(i);
				Node firstchild = node.getFirstChild();
				if (firstchild == null)
				{
					result = null;
					break;
				}
				String value = firstchild.getNodeValue();
				if (value != null && value.length() > 0)
				{
					if (result == null)
						result = value;
					else
						result += ", " + value;
				}
			}
		}
		catch(XPathExpressionException ex)
		{
		}

		if (result == null) result = (String)expr.evaluate(node,XPathConstants.STRING);
		if (result == null) return "";

		return (String)result;
	}

	public void setAttributeByPath(String path,String name,String value) throws Exception
	{
		if (value == null) return;

		XML[] nodes = getElementsByPath(path);
		for(XML xmlnode:nodes)
		{
			Element el = (Element)xmlnode.node;
			el.setAttribute(name,value);
		}
	}

	public void removeAttribute(String name) throws Exception
	{
		if (!isElement(node)) return;

		Element el = (Element)node;
		el.removeAttribute(name);
	}

	public void setAttribute(String name,String value) throws Exception
	{
		if (!isElement(node)) return;
		if (value == null) return;

		Element el = (Element)node;
		el.setAttribute(name,value);
	}

	public void setValueByPath(String name,String value) throws Exception
	{
		XML[] nodes = getElementsByPath(name);

		for(XML xmlnode:nodes)
			xmlnode.setValue(value);
	}

	public void setValue(String name,String value) throws Exception
	{
		XML el = getElement(name);
		if (el != null) el.setValue(value);
	}

	public void setValue(String value) throws Exception
	{
		if (node == null) return; // Node is empty

		Node firstchild = node.getFirstChild();
		if (firstchild == null)
		{
			if (value != null)
			{
				Text text = dom.createTextNode(fixValue(value));
				node.appendChild(text);
			}
			return;
		}
		firstchild.setNodeValue(value);
	}

	public void remove() throws Exception
	{
		if (node == null) return; // Node is empty

		Node parent = node.getParentNode();
		if (parent == null)
		{
			dom.removeChild(node);
			node = null;
		}
		else
		{
			parent.removeChild(node);
			node = parent;
		}
	}

	public XML add(XML xml) throws Exception
	{
		if (xml.node == null) return xml; // Nothing to add

		Node newnode = dom.importNode(xml.node,true);
		if (node == null)
		{
			newnode = dom.appendChild(newnode);
			node = newnode;
		}
		else
			newnode = node.appendChild(newnode);

		return new XML(dom,newnode);
	}

	public XML add(XML[] xmllist) throws Exception
	{
		for(XML xml:xmllist)
			add(xml);

		return this;
	}

	public XML add(String name) throws Exception
	{
		Element element = dom.createElement(fixName(name));

		Node newnode;
		if (node == null)
		{
			newnode = dom.appendChild(element);
			node = newnode;
		}
		else
 			newnode = node.appendChild(element);

		return new XML(dom,newnode);
	}

	public XML addNS(String name,String ns) throws Exception
	{
		Element element = dom.createElementNS(ns,fixName(name));

		Node newnode;
		if (node == null)
		{
			newnode = dom.appendChild(element);
			node = newnode;
		}
		else
 			newnode = node.appendChild(element);

		return new XML(dom,newnode);
	}

	public XML add(String name,String value) throws Exception
	{
		Element element = dom.createElement(fixName(name));

		Node newnode;
		if (node == null)
		{
			newnode = dom.appendChild(element);
			node = newnode;
		}
		else
			newnode = node.appendChild(element);

		if (value != null)
		{
			Text text = dom.createTextNode(fixValue(value));
			newnode.appendChild(text);
		}

		return new XML(dom,newnode);
	}

	public XML add(String name,Map<String,String> row) throws Exception
	{
		XML parent = add(name);
		Iterator<String> itr = row.keySet().iterator();
		while(itr.hasNext())
		{
			String keyrow = itr.next();
			parent.add(keyrow,row.get(keyrow));
		}

		return parent;
	}

	public XML add(String name,String rowname,List<Map<String,String>> table) throws Exception
	{
		XML parent = add(name);

		for(int i = 0;i < table.size();i++)
			parent.add(rowname,table.get(i));

		return parent;
	}

	public XML addCDATA(String name,String value) throws Exception
	{
		Element element = dom.createElement(fixName(name));
		Node newnode;
		if (node == null)
		{
			newnode = dom.appendChild(element);
			node = newnode;
		}
		else
			newnode = node.appendChild(element);

		CDATASection cdata = dom.createCDATASection(value);
		newnode.appendChild(cdata);

		return new XML(dom,newnode);
	}

	static public synchronized void setDefaults(XML xmlconfig) throws Exception
	{
		XML[] xmllist = xmlconfig.getElements("default");
		for(XML xml:xmllist)
		{
			String attribute = xml.getAttribute("attribute");
			if (attribute != null)
				defaults.add(xml);
			else
			{
				attribute = xml.getAttribute("name");
				if (attribute != null && attribute.startsWith("$"))
				{
					String value = xml.getAttribute("value");
					if (value == null) value = xml.getValue();
					if (!defaultvars.containsKey(attribute)) // Take first matching one only
						defaultvars.put(attribute,value);
				}
				else
					throw new AdapterException(xml,"Missing \"attribute\" attribute or \"name\" attribute is not a variable");
			}
		}
		
	}

	static public synchronized void setDefaultVariable(String name,String value)
	{
		if (value == null)
		{
			defaultvars.remove(name);
			return;
		}

		defaultvars.put(name,value);
	}

	static public synchronized String getDefaultVariable(String name)
	{
		return defaultvars.get(name);
	}

	static public synchronized HashMap<String,String> getDefaultVariables()
	{
		return defaultvars;
	}

	static public boolean isNameChar(char ch)
	{
		if (ch >= 'a' && ch <= 'z') return true;
		if (ch >= 'A' && ch <= 'Z') return true;
		if (ch >= '0' && ch <= '9') return true;
		if (ch == '.' || ch == '-' || ch == ':' || ch == '_') return true;
		return false;
	}

	static public String fixName(String name)
	{
		StringBuffer newname = new StringBuffer();
		for(int i = 0;i < name.length();i++)
		{
			if (isNameChar(name.charAt(i)))
				newname.append(name.charAt(i));
			else
				newname.append('_');
		}

		if (newname.length() == 0) return "_";
		return newname.toString();
	}

	static public String fixValue(String str)
	{
		return xmlvaluepattern.matcher(str).replaceAll("");
	} 
}
