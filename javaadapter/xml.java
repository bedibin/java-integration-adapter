import java.io.*;
import java.util.*;
import java.text.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;
import javax.xml.transform.dom.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;
import javax.xml.XMLConstants;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import java.util.regex.*;
import org.json.JSONObject;

interface VariableContext
{
	public TimeZone getTimeZone();
}

class AdapterXmlException extends AdapterException
{
	AdapterXmlException(String str)
	{
		super(str);
	}

	AdapterXmlException(Throwable ex)
	{
		super(ex);
	}

	AdapterXmlException(XML xml,String str,Object... args)
	{
		super(Misc.getMessage(xml,str,args));
	}
}

class AdapterXmlPathException extends AdapterXmlException
{
	AdapterXmlPathException(XPathExpressionException ex)
	{
		super(ex);
	}
}

class XML
{
	static class DefaultVar
	{
		boolean substitute = false;
		String value;

		DefaultVar(String value)
		{
			this.value = value;
		}

		DefaultVar(XML xml) throws AdapterXmlException
		{
			String name = xml.getAttribute("name");
			String substr = xml.getAttribute("substitute");
			if (substr != null && substr.equals("true"))
				substitute = true;
			if (name != null && name.startsWith("$PASSWORD"))
				value = xml.getAttributeCrypt("value");
			else
				value = xml.getAttribute("value");
			if (value == null) value = xml.getValue();
		}

		String getValue()
		{
			if (substitute)
			{
				try {
					return Misc.substitute(value,new Misc.Substituer() {
						public String getValue(String param) throws AdapterException
						{
							DefaultVar var = defaultvars.get().get(param);
							return var == null ? null : var.getValue();
						}
					});
				} catch(AdapterException ex) {
					return null;
				}
			}
			return value;
		}
	}

	protected Document dom;
	protected Node node;
	private static ArrayList<XML> defaultattributes = new ArrayList<>();
	private static HashMap<String,XML> defaultelements = new HashMap<>();
	private static ThreadLocal<Map<String,DefaultVar>> defaultvars = new ThreadLocal<Map<String,DefaultVar>>()
	{
		@Override
		protected Map<String,DefaultVar> initialValue() {
			return new HashMap<String,DefaultVar>();
		}
	};
	static final String LINE_NUMBER_KEY_NAME = "lineNumber";
	Stack<Element> elementStack = new Stack<>();
	StringBuilder textBuffer = new StringBuilder();
	SAXParser parser;
	// See: https://www.w3.org/TR/xml/#charsets
	private static Pattern xmlvaluepattern = Pattern.compile("[^\\x09\\x0a\\x0d\\x20-\\x7f\\xa0-\\uD7EF\\uE000-\\uFFFD]");

	private static TransformerFactory getTransformerFactory()
	{
		TransformerFactory factory = TransformerFactory.newInstance();
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD,"");
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET,"");
		return factory;
	}

	private DefaultHandler handler = new DefaultHandler()
	{
		private Locator locator;

		@Override
		public void setDocumentLocator(Locator locator)
		{
                	this.locator = locator; // Save the locator, so that it can be used later for line tracking when traversing nodes.
		}

		@Override
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

		@Override
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

		@Override
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

	public boolean isEmpty()
	{
		return !isElement(node);
	}

	private Node getRootNode()
	{
		Node nodeel = dom.getDocumentElement();
		if (nodeel == null) return null;
		return nodeel;
	}

	public void setNS(String url,String prefix)
	{
		Node nodeel = dom.getDocumentElement();
		if (isEmpty()) return;

		Element el = (Element)nodeel;
		el.setAttributeNS("http://www.w3.org/2000/xmlns/","xmlns:" + prefix,url);
	}


	public XML() throws AdapterXmlException
	{
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			//factory.setNamespaceAware(true);
			parser = factory.newSAXParser();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			dom = db.newDocument();
			node = getRootNode();
		} catch(ParserConfigurationException | SAXException ex) {
			throw new AdapterXmlException(ex);
		}
	}

	public XML(String filename) throws AdapterXmlException
	{
		this();
		try {
			parser.parse(new File(javaadapter.getCurrentDir(),filename),handler);
		} catch(IOException | SAXException ex) {
			throw new AdapterXmlException(ex);
		}
		node = getRootNode();
	}

	public XML(String filename,String root) throws AdapterXmlException
	{
		this();
		try {
			parser.parse(new File(javaadapter.getCurrentDir(),filename),handler);
		} catch(IOException | SAXException ex) {
			throw new AdapterXmlException(ex);
		}
		node = getElementByPath(root).node;
	}

	public JSONObject toJSON()
	{
		return org.json.XML.toJSONObject(toString());
	}

	public XML(JSONObject json) throws AdapterXmlException
	{
		// https://github.com/stleary/JSON-java
		// https://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.json%22%20AND%20a%3A%22json%22
		this();
		String txt = org.json.XML.toString(json,"root");
		try
		{
			parser.parse(new InputSource(new StringReader(txt)),handler);
		} catch(IOException | SAXException ex) {
			Misc.rethrow(new AdapterXmlException(ex),"Invalid XML: " + txt);
		}
		node = getRootNode();
	}

	public XML(StringBuffer sb) throws AdapterXmlException
	{
		this();
		String txt = sb.toString();
		try
		{
			parser.parse(new InputSource(new StringReader(txt)),handler);
		} catch(IOException | SAXException ex) {
			Misc.rethrow(new AdapterXmlException(ex),"Invalid XML: " + txt);
		}
		node = getRootNode();
	}

	public XML(StringBuilder sb) throws AdapterXmlException
	{
		this();
		String txt = sb.toString();
		try
		{
			parser.parse(new InputSource(new StringReader(txt)),handler);
		} catch(IOException | SAXException ex) {
			Misc.rethrow(new AdapterXmlException(ex),"Invalid XML: " + txt);
		}
		node = getRootNode();
	}

	public XML(StringBuilder sb,String root) throws AdapterXmlException
	{
		this();
		String txt = sb.toString();
		try {
			parser.parse(new InputSource(new StringReader(txt)),handler);
		} catch(IOException | SAXException ex) {
			throw new AdapterXmlException(ex);
		}
		node = getElementByPath(root).node;
	}

	protected XML(Document root,Node node)
	{
		dom = root;
		this.node = node;
	}

	public XML copy() throws AdapterXmlException
	{
		XML xml = new XML();
		xml.add(this);
		xml.node = xml.getRootNode();
		return xml;
	}

	public XML rename(String name)
	{
		dom.renameNode(node,node.getNamespaceURI(),name);
		return this;
	}

	public Document getDocument()
	{
		return dom;
	}

	public void setRoot()
	{
		node = getRootNode();
	}

	public XML getParent()
	{
		if (node == null) return null;
		return new XML(dom,node.getParentNode());
	}

	public void write(String filename) throws AdapterXmlException
	{
		try {
			Transformer xformer = getTransformerFactory().newTransformer();
			Source source = new DOMSource(node == null ? dom : node);
			FileWriter fw = new FileWriter(new File(javaadapter.getCurrentDir(),filename));
			Result result = new StreamResult(fw);
			xformer.transform(source,result);
		} catch (IOException | TransformerException ex) {
			throw new AdapterXmlException(ex);
		}
	}

	public String toString(Node node,boolean omitdeclaration)
	{
		try
		{
			Transformer transformer = getTransformerFactory().newTransformer();
			if (omitdeclaration)
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
			StringWriter writer = new StringWriter();
			if (node == null || node == dom)
				transformer.transform(new DOMSource(dom),new StreamResult(writer));
			else
			{
				if (isEmpty()) return "";
				Element el = (Element)node;
				transformer.transform(new DOMSource(el),new StreamResult(writer));
			}
			return writer.getBuffer().toString();
		}
		catch(TransformerException ex)
		{
			Misc.log(ex);
		}
		return null;
	}

	public String toStringNoDeclaration()
	{
		return toString(node,true);
	}

	public String toString()
	{
		return toString(node,false);
	}

	public String rootToString()
	{
		return toString(dom,false);
	}

	public String getLine()
	{
		return (String)(node.getUserData(LINE_NUMBER_KEY_NAME));
	}

	public XML transform(String filename) throws AdapterXmlException
	{
		try
		{
			TransformerFactory factory = getTransformerFactory();
			Templates template = factory.newTemplates(new StreamSource(new FileInputStream(new File(javaadapter.getCurrentDir(),filename))));
			Transformer xformer = template.newTransformer();
			Source source = new DOMSource(dom);

			XML xml = new XML();
			Result result = new DOMResult(xml.dom);
			xformer.transform(source,result);
			xml.node = xml.getRootNode();
			// Next code is commented since we allow empty XML being return
			//if (!(xml.node instanceof Element))
			//	throw new AdapterXmlException("Transformation didn't return a node");
			return xml;
		}
		catch(FileNotFoundException | TransformerException ex)
		{
			Misc.log(1,"XML transformation [" + filename + "]: " + toString());
			throw new AdapterXmlException(ex);
		}
	}

	public void transform(XML xmlinfo) throws AdapterXmlException
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

	public void lookup(XML xmlinfo) throws AdapterXmlException
	{
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
					soaplookup.lookup(this);
				}
				else if (type.equals("db"))
				{
					Object db = Misc.invokeStatic("DB","getInstance");
					Misc.invoke(db,"lookup",lookup,this);
				}
			}
			catch(Exception ex)
			{
				if (!handleexception) Misc.rethrow(new AdapterXmlException(ex));
				Misc.log(ex);

				String msg = ex.getMessage();
				if (msg == null) msg = ex.getCause() == null ? ex.toString() : ex.getCause().getMessage();
				add("exception",msg);
			}
		}
	}

	public String getTagName()
	{
		if (isEmpty()) return null;
		Element el = (Element)node;
		return el.getTagName();
	}

	public String getXPath()
	{
		Node parent = null;
		Stack<Node> hierarchy = new Stack<>();
		StringBuilder buffer = new StringBuilder();

		hierarchy.push(node);
		parent = node.getParentNode();

		while (null != parent && parent.getNodeType() != Node.DOCUMENT_NODE)
		{
			hierarchy.push(parent);
			parent = parent.getParentNode();
		}

		Node nodeobj = null;
		while (!hierarchy.isEmpty() && null != (nodeobj = hierarchy.pop()))
		{
			if (nodeobj.getNodeType() == Node.ELEMENT_NODE)
			{
				if (buffer.length() == 0)
					buffer.append(nodeobj.getNodeName());
				else
				{
					buffer.append("/");
					buffer.append(nodeobj.getNodeName());
               
					int prevsiblings = 1;
					Node prevsibling = nodeobj.getPreviousSibling();
					while (null != prevsibling)
					{
						if (prevsibling.getNodeType() == nodeobj.getNodeType())
							if (prevsibling.getNodeName().equalsIgnoreCase(nodeobj.getNodeName()))
								prevsiblings++;
						prevsibling = prevsibling.getPreviousSibling();
					}
					buffer.append("[" + prevsiblings + "]");
				}
			}
		}

		return buffer.toString();
	}

	public String getAttributeCrypt(String name) throws AdapterXmlException
	{
		String value = getAttribute(name);
		if (value == null) return null;

		return Misc.getCrypter().decrypt(value);
	}

	public Map<String,String> getAttributes()
	{
		if (node == null) return null;
		LinkedHashMap<String,String> row = new LinkedHashMap<>();

		NamedNodeMap map = node.getAttributes();
		for(int i = 0;i < map.getLength();i++)
		{
			Node attr = map.item(i);
			row.put(attr.getNodeName(),attr.getNodeValue());
		}

		return row;
	}

	public void copyAttribute(String name,XML xml) throws AdapterXmlException
	{
		String source = getAttribute(name);
		if (source == null) return; // Nothing to copy
		String dest = xml.getAttribute(name);
		if (dest != null) return; // Don't overwrite
		xml.setAttribute(name,source);
	}

	public void copyAttributes(XML xml)
	{
		Map<String,String> map = getAttributes();
		for (Map.Entry<String,String> entry:map.entrySet())
			xml.setAttribute(entry.getKey(),entry.getValue());
	}

	public synchronized boolean isElementNoDefault(String name)
	{
		if (isEmpty()) return false;
		Node currentnode = node.getFirstChild();
		while(currentnode != null)
		{
			if (isElement(currentnode))
			{
				Element el = (Element)currentnode;
				String tagname = el.getTagName();
				if (tagname.equals(name)) return true;
			}
			currentnode = currentnode.getNextSibling();
		}

		return false;
	}

	public synchronized boolean isAttributeNoDefault(String name)
	{
		if (isEmpty()) return false;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);
		return attr != null;
	}

	public synchronized boolean isAttribute(String name) throws AdapterXmlException
	{
		if (isEmpty()) return false;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);

		if (attr == null)
		{
			for(XML def:defaultattributes)
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

	public String getAttributeDeprecated(String name)
	{
		if (isEmpty()) return null;
		Element el = (Element)node;
		Attr attr = el.getAttributeNode(name);
		if (attr == null) return null;
		return attr.getValue();
	}

	public void setAttributeDeprecated(String oldattr,String newattr) throws AdapterXmlException
	{
		if (isAttribute(newattr)) return;
		String value = getAttributeDeprecated(oldattr);
		if (value == null) return;
		setAttribute(newattr,value);
	}

	public String getAttribute(String name) throws AdapterXmlException
	{
		if (isEmpty()) return null;
		Element	el = (Element)node;
		Attr attr = el.getAttributeNode(name);
		String value = el.getAttribute(name);

		if (attr == null)
		{
			for(XML def:defaultattributes)
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

	public <E extends Enum<E>> E getAttributeEnum(String name,Class<E> type) throws AdapterXmlException
	{
		return getAttributeEnum(name,null,null,type);
	}

	public <E extends Enum<E>> E getAttributeEnum(String name,E def,Class<E> type) throws AdapterXmlException
	{
		return getAttributeEnum(name,def,null,type);
	}

	public <E extends Enum<E>> E getAttributeEnum(String name,E def,Set<E> validset,Class<E> type) throws AdapterXmlException
	{
		String value = getAttribute(name);
		if (value == null) return def;

		try {
			E result = Enum.valueOf(type,value.toUpperCase());
			if (validset != null && !validset.contains(result))
				throw new AdapterXmlException(this,"Invalid attribute '" + name + "' value: " + value);
			return result;
		} catch(IllegalArgumentException ex) {
			throw new AdapterXmlException(this,"Invalid attribute '" + name + "' value: " + value);
		}
	}

	public String getValueCrypt() throws AdapterXmlException
	{
		String value = getValue();
		if (value == null) return null;

		return Misc.getCrypter().decrypt(value);
	}

	public String getElementValue()
	{
		if (node == null) return null;

		Node firstchild = node.getFirstChild();
		if (firstchild == null) return null;

		String value = firstchild.getNodeValue();
		if (value == null || value.length() == 0) return null;
		return value;
	}

	public String getValue() throws AdapterXmlException
	{
		if (node == null) return null;

		try {
			String filename = getAttribute("filename");
			if (filename != null)
				return Misc.readFile(filename,getAttribute("charset"));
		} catch(AdapterException ex) {
			throw new AdapterXmlException(ex);
		}

		return getElementValue();
	}

	public String getValueCrypt(String name) throws AdapterXmlException
	{
		String value = getValue(name);
		if (value == null) return null;

		return Misc.getCrypter().decrypt(value);
	}

	public String getValue(String name) throws AdapterXmlException
	{
		return getValue(name,false);
	}

	public String getValue(String name,String defvalue) throws AdapterXmlException
	{
		return getValue(name,defvalue,false);
	}

	public String getValueByPath(String name,String defvalue) throws AdapterXmlException
	{
		return getValue(name,defvalue,true);
	}

	public String getValueByPath(String name) throws AdapterXmlException
	{
		return getValue(name,true);
	}

	public String getValue(String name,boolean checkpath) throws AdapterXmlException
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) throw new AdapterXmlException(this,"XML doesn't contain " + name + " element");
		return xmllist[0].getValue();
	}

	public String getValue(String name,String defvalue,boolean checkpath) throws AdapterXmlException
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) return defvalue;
		return xmllist[0].getValue();
	}

	public XML getElement(String name) throws AdapterXmlException
	{
		XML result = getElement(name,false);
		return result == null ? defaultelements.get(name) : result;
	}

	public XML getElementByPath(String name) throws AdapterXmlException
	{
		return getElement(name,true);
	}

	public XML getElement(String name,boolean checkpath) throws AdapterXmlException
	{
		XML[] xmllist = getElements(name,checkpath);
		if (xmllist.length == 0) return null;
		return xmllist[0];
	}

	public XML[] getElements() throws AdapterXmlException
	{
		return getElements(null);
	}

	public XML[] getElements(String name) throws AdapterXmlException
	{
		return getElements(name,false);
	}

	public XML[] getElementsByPath(String name) throws AdapterXmlException
	{
		return getElements(name,true);
	}

	public XML[] getElements(String name,boolean checkpath) throws AdapterXmlException
	{
		ArrayList<XML> xmllist = new ArrayList<>();
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

		if (checkpath && xmllist.isEmpty() && name != null)
		{
			XPathFactory factory = XPathFactory.newInstance();
			try {
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
			} catch(XPathExpressionException ex) {
				throw new AdapterXmlPathException(ex);
			}
		}

		XML[] xmlarray = new XML[xmllist.size()];
		return xmllist.toArray(xmlarray);
	}

	public String getStringByPath(String name) throws AdapterXmlPathException
	{
		if (node == null) return null;

		XPathFactory factory = XPathFactory.newInstance();
		XPath xpath = factory.newXPath();

		try {
			XPathExpression expr = xpath.compile(name);

			StringBuilder result = null;

			try {
				Object obj = expr.evaluate(node,XPathConstants.NODESET);
				NodeList nl = (NodeList)obj;
				for(int i = 0;i < nl.getLength();i++)
				{
					Node nodeel = nl.item(i);
					Node firstchild = nodeel.getFirstChild();
					if (firstchild == null)
					{
						result = null;
						break;
					}
					String value = firstchild.getNodeValue();
					if (value != null && value.length() > 0)
					{
						if (result == null)
							result = new StringBuilder(value);
						else
							result.append(", " + value);
					}
				}
			} catch(XPathExpressionException ex) {
				// Nothing since try with XPathConstants.STRING just after
			}

			if (result == null) result = new StringBuilder((String)expr.evaluate(node,XPathConstants.STRING));

			return result.toString();
		} catch(XPathExpressionException ex) {
			throw new AdapterXmlPathException(ex);
		}
	}

	public void setAttributeByPath(String path,String name,String value) throws AdapterXmlException
	{
		if (value == null) return;

		XML[] nodes = getElementsByPath(path);
		for(XML xmlnode:nodes)
		{
			Element el = (Element)xmlnode.node;
			el.setAttribute(name,value);
		}
	}

	public void removeAttribute(String name)
	{
		if (isEmpty()) return;

		Element el = (Element)node;
		el.removeAttribute(name);
	}

	public XML setAttribute(String name,String value)
	{
		if (isEmpty()) return this;
		if (value == null) return this;

		Element el = (Element)node;
		el.setAttribute(name,value);

		return this;
	}

	public void setValueByPath(String name,String value) throws AdapterXmlException
	{
		XML[] nodes = getElementsByPath(name);

		for(XML xmlnode:nodes)
			xmlnode.setValue(value);
	}

	public XML setValue(String name,String value) throws AdapterXmlException
	{
		XML el = getElement(name);
		if (el == null)
			return add(name,value);
		el.setValue(value);
		return el;
	}

	public void setValue(String value)
	{
		if (node == null) return; // Node is empty

		Node firstchild = node.getFirstChild();
		if (firstchild == null)
		{
			if (value == null) return;
			Text text = dom.createTextNode(fixValue(value));
			node.appendChild(text);
			return;
		}
		if (firstchild.getNodeType() != Node.TEXT_NODE && firstchild.getNodeType() != Node.CDATA_SECTION_NODE)
		{
			if (value == null) return;
			Text text = dom.createTextNode(fixValue(value));
			node.insertBefore(text,firstchild);
			return;
		}

		firstchild.setNodeValue(fixValue(value));
	}

	public void remove()
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

	public XML addBefore(String name) throws AdapterXmlException
	{
		if (node == null || node == dom.getDocumentElement()) throw new AdapterXmlException("Cannot insert before on an empty node");

		Element element = createElement(name);
		Node newnode = node.getParentNode().insertBefore(element,node);
		return new XML(dom,newnode);
	}

	public XML addBefore(XML xml) throws AdapterXmlException
	{
		if (xml == null || xml.node == null) return xml; // Nothing to add
		if (node == null || node == dom.getDocumentElement()) throw new AdapterXmlException(xml,"Cannot insert before on an empty node");

		Node newnode = dom.importNode(xml.node,true);
		newnode = node.getParentNode().insertBefore(newnode,node);
		return new XML(dom,newnode);
	}

	public XML addAfter(String name) throws AdapterXmlException
	{
		if (node == null || node == dom.getDocumentElement()) throw new AdapterXmlException("Cannot insert after on an empty node");
		Element element = createElement(name);

		Node newnode;
		Node nextnode = node.getNextSibling();
		if (nextnode == null)
		{
			newnode = dom.appendChild(element);
			node = newnode;
		}
		else
			newnode = node.getParentNode().insertBefore(element,nextnode);

		return new XML(dom,newnode);
	}

	public XML addAfter(XML xml) throws AdapterXmlException
	{
		if (xml == null || xml.node == null) return xml; // Nothing to add
		if (node == null || node == dom.getDocumentElement()) throw new AdapterXmlException(xml,"Cannot insert after on an empty node");

		Node newnode = dom.importNode(xml.node,true);
		Node nextnode = node.getNextSibling();
		if (nextnode == null)
			newnode = node.appendChild(newnode);
		else
			newnode = node.getParentNode().insertBefore(newnode,nextnode);
		return new XML(dom,newnode);
	}

	public XML add(XML xml)
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

	public XML add(XML[] xmllist) throws AdapterXmlException
	{
		for(XML xml:xmllist)
			add(xml);

		return this;
	}

	public XML add(String name)
	{
		Element element = createElement(name);

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

	public XML add(String name,String value)
	{
		Element element = createElement(name);

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

	public XML add(String name,Map<String,String> row)
	{
		XML parent = add(name);
		for(Map.Entry<String,String> entry:row.entrySet())
			parent.add(entry.getKey(),entry.getValue());
		return parent;
	}

	public XML add(String name,String rowname,List<Map<String,String>> table)
	{
		XML parent = add(name);

		for(Map<String,String> entry:table)
			parent.add(rowname,entry);

		return parent;
	}

	public XML addCDATA(String name,String value)
	{
		Element element = createElement(name);
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

	public boolean matchXPath(String string) throws AdapterXmlException
	{
		try {
			XPathFactory factory = XPathFactory.newInstance();
			XPath xpath = factory.newXPath();
			XPathExpression expr = xpath.compile(string);

			Boolean result = (Boolean)expr.evaluate(node,XPathConstants.BOOLEAN);
			return result.booleanValue();
		} catch(XPathExpressionException ex) {
			throw new AdapterXmlException(ex);
		}
	}

	public static synchronized void setDefaults(XML xmlconfig) throws AdapterXmlException
	{
		Map<String,DefaultVar> defaults = new HashMap<>();

		XML[] xmllist = xmlconfig.getElements("default");
		for(XML xml:xmllist)
		{
			String attribute = xml.getAttribute("attribute");
			if (attribute != null)
				defaultattributes.add(xml);
			else
			{
				String element = xml.getAttribute("element");
				if (element != null)
				{
					if (!defaultelements.containsKey(element)) // Take first matching one only
						defaultelements.put(element,xml);
				}
				else
				{
					attribute = xml.getAttribute("name");
					if (attribute != null && attribute.startsWith("$"))
					{
						if (!defaults.containsKey(attribute)) // Take first matching one only
							defaults.put(attribute,new DefaultVar(xml));
					}
					else
						throw new AdapterXmlException(xml,"Missing 'attribute'/'element' attribute or 'name' attribute is not a variable");
				}
			}
		}

		Properties props = System.getProperties();
		Enumeration keys = props.keys();
		final String KEY = "javaadapter.$";
		while (keys.hasMoreElements())
		{
			String key = (String)keys.nextElement();
			if (key.startsWith(KEY))
			{
				String value = (String)props.get(key);
				String var = key.substring(KEY.length() - 1);
				defaults.put(var,new DefaultVar(value));
			}
		}
		
		defaultvars = new ThreadLocal<Map<String,DefaultVar>>()
		{
			@Override
			protected Map<String,DefaultVar> initialValue() {
				return defaults;
			}
		};

	}

	public static synchronized void setDefaultVariable(String name,String value)
	{
		if (value == null)
		{
			defaultvars.get().remove(name);
			return;
		}

		defaultvars.get().put(name,new DefaultVar(value));
	}

	public static synchronized String getDefaultVariable(String name)
	{
		return getDefaultVariable(name,null);
	}

	public static synchronized String getDefaultVariable(String name,VariableContext ctx)
	{
		final String currentdatevar = "$ADAPTER_CURRENT_DATE_";
		final String startdatevar = "$ADAPTER_START_DATE_";

		SimpleDateFormat format = null;
		Date date = null;

		if ("$ADAPTER_CURRENT_DATE".equals(name))
			format = new SimpleDateFormat(Misc.DATEFORMAT);
		else if (name.startsWith(currentdatevar))
			format = new SimpleDateFormat(name.substring(currentdatevar.length()));
		else if ("$ADAPTER_START_DATE".equals(name))
		{
			format = new SimpleDateFormat(Misc.DATEFORMAT);
			date = javaadapter.startdate;
		}
		else if (name.startsWith(startdatevar))
		{
			format = new SimpleDateFormat(name.substring(startdatevar.length()));
			date = javaadapter.startdate;
		}
		else if ("$ADAPTER_NAME".equals(name))
			return javaadapter.getName();

		if (format != null)
		{
			TimeZone tz = null;
			if (ctx != null) tz = ctx.getTimeZone();
			if (tz == null) tz = TimeZone.getTimeZone("UTC");
			format.setTimeZone(tz);

			if (date == null) date = new Date();
			return format.format(date);
		}

		DefaultVar var = defaultvars.get().get(name);
		return var == null ? null : var.getValue();
	}

	public static synchronized Map<String,DefaultVar> getDefaultVariables()
	{
		return defaultvars.get();
	}

	private static boolean isNameChar(char ch,boolean isfirst)
	{
		// See http://www.w3.org/TR/xml/#d0e804
		if (ch == ':' || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= '\u00c0' && ch <= '\u00d6') || (ch >= '\u00d8' && ch <= '\u00f6') || (ch >= '\u00f8' && ch <= '\u02ff') || (ch >= '\u0370' && ch <= '\u037d') || (ch >= '\u037f' && ch <= '\u1fff') || (ch >= '\u200c' && ch <= '\u200d') || (ch >= '\u2070' && ch <= '\u218f') || (ch >= '\u2c00' && ch <= '\u2fef') || (ch >= '\u3001' && ch <= '\ud7ff') || (ch >= '\uf900' && ch <= '\ufdcf') || (ch >= '\ufdf0' && ch <= '\ufffd')) return true;
		if (isfirst) return false;
		if (ch == '-' || ch == '.' || (ch >= '0' && ch <= '9') || ch == '\u00b7' || (ch >= '\u0300' && ch <= '\u036f') || (ch >= '\u203f' && ch <= '\u2040')) return true;
		return false;
	}

	private Element createElement(String name)
	{
		StringBuilder newname = new StringBuilder();
		boolean isFixed = false;
		for(int i = 0;i < name.length();i++)
		{
			if (isNameChar(name.charAt(i),i == 0))
				newname.append(name.charAt(i));
			else {
				newname.append('_');
				isFixed = true;
			}
		}

		Element element = dom.createElement(newname.length() == 0 ? "_" : newname.toString());

		if (isFixed || newname.length() == 0)
			element.setAttribute("javaadapter_name",name);

		return element;
	}

	public static String fixValue(String str)
	{
		if (str == null) return null;
		return xmlvaluepattern.matcher(str).replaceAll("");
	} 

	public void setInclude() throws AdapterXmlException
	{
		for(XML element:getElements())
		{
			if (!"include".equals(element.getTagName())) continue;
			XML include = new XML(element.getAttribute("filename"));
			if (!getTagName().equals(include.getTagName()))
				throw new AdapterXmlException("Can only include XML with tag name '" + getTagName() + "': " + element);

			for(XML add:include.getElements())
				element.addBefore(add);
			element.remove();
		}
	}
}
