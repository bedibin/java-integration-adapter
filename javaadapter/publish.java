import java.io.*;
import java.util.*;
import java.net.*;
import javax.net.ssl.*;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.KeyStore;
import javax.xml.rpc.*;
import javax.xml.namespace.*;

class AliasSelectorKeyManager implements X509KeyManager
{
	private X509KeyManager sourceKeyManager;
	private String alias;
 
	public AliasSelectorKeyManager(X509KeyManager keyManager,String alias)
	{
		this.sourceKeyManager = keyManager;        
		this.alias = alias;
	}
 
	public String chooseClientAlias(String[] keyType,Principal[] issuers,Socket socket)
	{   
		boolean aliasFound = false;
 
		for (int i=0; i<keyType.length && !aliasFound; i++)
		{
			String[] validAliases = sourceKeyManager.getClientAliases(keyType[i],issuers);
			if (validAliases != null)
				for (int j=0;j < validAliases.length && !aliasFound;j++)
					if (validAliases[j].equals(alias))
						aliasFound=true;
		}
 
		if (aliasFound) return alias;
		return null;
	}
 
	public String chooseServerAlias(String keyType,Principal[] issuers,Socket socket)
	{
		return sourceKeyManager.chooseServerAlias(keyType,issuers,socket);
	}
 
	public X509Certificate[] getCertificateChain(String alias)
	{
		return sourceKeyManager.getCertificateChain(alias);
	}
 
	public String[] getClientAliases(String keyType,Principal[] issuers)
	{
		return sourceKeyManager.getClientAliases(keyType,issuers);
	}
 
	public PrivateKey getPrivateKey(String alias)
	{
		return sourceKeyManager.getPrivateKey(alias);
	}
 
	public String[] getServerAliases(String keyType,Principal[] issuers)
	{
		return sourceKeyManager.getServerAliases(keyType,issuers);
	}
}

enum PublisherTypes { DIRECT, JMS, LDAP, SOAP, SOAPOPER, HTTP, POST, EXEC, FILE };

class Publisher
{
	class PublisherObject
	{
		private PublisherTypes type;
		private TopicPublish topicpublish;
		private Service service;
		private URL defaulturl;
		private String command;
		private File file;
		private ldap ld;
		private String publishername;

		public PublisherObject(PublisherTypes type)
		{
			this.type = type;
		}

		public PublisherObject(TopicPublish topicpublish)
		{
			type = PublisherTypes.JMS;
			this.topicpublish = topicpublish;
		}

		public PublisherObject(ldap ld)
		{
			type = PublisherTypes.LDAP;
			this.ld = ld;
		}

		public PublisherObject(Service service)
		{
			type = PublisherTypes.SOAPOPER;
			this.service = service;
		}

		public PublisherObject(URL url)
		{
			type = PublisherTypes.HTTP;
			defaulturl = url;
		}

		public PublisherObject(String command)
		{
			type = PublisherTypes.EXEC;
			this.command = command;
		}

		public PublisherObject(File file)
		{
			type = PublisherTypes.FILE;
			this.file = file;
		}

		public void setName(String name)
		{
			publishername = name;
		}

		public String sendHttpRequest(String body,XML xmlpub) throws Exception
		{
			return sendHttpRequest(body,null,xmlpub);
		}

		public String sendHttpRequest(String body,XML xml,XML xmlpub) throws Exception
		{
			String urlstr = xml == null ? Misc.substitute(xmlpub.getAttribute("url")) : Misc.substitute(xmlpub.getAttribute("url"),xml);
			if (urlstr == null && defaulturl == null) urlstr = "http://localhost/";
			URL url = urlstr == null ? defaulturl : new URL(urlstr);
			if (url == null) throw new AdapterException(xmlpub,"No URL provided");

			String method = xmlpub.getAttribute("method");
			if (method == null)
				method = "POST";
			else
				method = method.toUpperCase();
			if (Misc.isLog(12)) Misc.log("HTTP " + method + " URL: " + url.toString());

			HttpURLConnection rc = (HttpURLConnection)url.openConnection();
			if (rc instanceof HttpsURLConnection)
			{
				String notrustssl = xmlpub.getAttribute("notrustssl");
				boolean notrust = notrustssl != null && "true".equals(notrustssl);
				if (notrust)
				{
					HttpsURLConnection rcs = (HttpsURLConnection)rc;
					rcs.setSSLSocketFactory(new notrustsslsocket());
					rcs.setHostnameVerifier(new nohostverifier());
					if (Misc.isLog(3)) Misc.log("WARNING: Untrusted SSL with url " + url);
				}

				String clientkeystore = xmlpub.getAttribute("clientkeystore");
				if (clientkeystore != null)
				{
					SSLContext sc = clientsslcontexts.get(publishername);
					if (sc == null)
					{
						KeyStore ks = KeyStore.getInstance("jks");
						FileInputStream fis = new FileInputStream(clientkeystore);
						String clientkeystorepassword = xmlpub.getAttributeCrypt("clientkeystorepassword");
						if (clientkeystorepassword == null) clientkeystorepassword = "changeit";
						ks.load(fis,clientkeystorepassword.toCharArray());
						fis.close();

						KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
						kmf.init(ks,clientkeystorepassword.toCharArray());

						KeyManager[] kms = kmf.getKeyManagers();
						String alias = xmlpub.getAttributeCrypt("clientkeystorealias");
						if (alias != null)
							for (int i = 0;i < kms.length;i++)
            							if (kms[i] instanceof X509KeyManager)
                							kms[i]=new AliasSelectorKeyManager((X509KeyManager)kms[i],alias);

						sc = SSLContext.getInstance("TLS");
						sc.init(kms,null,null);
						clientsslcontexts.put(publishername,sc);
					}
					((HttpsURLConnection)rc).setSSLSocketFactory(sc.getSocketFactory());
				}
			}

			String jsessionid = getSessionID(publishername);

			rc.setConnectTimeout(default_soap_connect_timeout);
			if (jsessionid == null)
				rc.setReadTimeout(default_soap_first_read_timeout);
			else
				rc.setReadTimeout(default_soap_read_timeout);

			rc.setRequestMethod(method);
			rc.setDoOutput(true);
			rc.setDoInput(true); 
			if (jsessionid != null)
			{
				Misc.log(9,"JSESSION set to: " + jsessionid);
				rc.setRequestProperty("Cookie",jsessionid);
			}

			String username = xmlpub.getAttribute("username");
			if (username != null)
			{
				String password = xmlpub.getAttributeCrypt("password");
				if (password == null) password = "";
				String authstr = username + ":" + password;
				// Note: Service Manager is UTF-8, other SOAP servers might be different...
				String auth = new String(base64coder.encode(authstr.getBytes("UTF-8"))); 
				rc.setRequestProperty("Authorization","Basic " + auth);
			}

			PublisherTypes type = xmlpub.getAttributeEnum("type",PublisherTypes.class);
			switch(type) {
			case SOAP:
				rc.setRequestProperty("Content-Type","text/xml; charset=utf-8");
				String action = xmlpub.getAttribute("action");
				rc.setRequestProperty("SOAPAction",action == null ? "" : action);
				break;
			case POST:
			case HTTP:
				String content = xmlpub.getAttribute("content_type");
				if (content == null) content = "application/x-www-form-urlencoded";
				rc.setRequestProperty("Content-Type",content);
				break;
			}

			byte[] rawrequest = body.getBytes("UTF-8");
			int len = rawrequest.length;
			rc.setRequestProperty("Content-Length",Integer.toString(len));

			rc.connect();

			OutputStream out = null;
			if (!method.equals("DELETE"))
			{
				out = rc.getOutputStream();
				out.write(rawrequest);
				out.flush();
			}

			InputStreamReader read = null;

			try
			{
				read = new InputStreamReader(rc.getInputStream(),"UTF-8");
			}
			catch(IOException ex)
			{
				if (Misc.isLog(3)) Misc.log("HTTP exception: " + ex);
				// Not sure if Service Manager needs this: setSessionID(url.toString(),null);
				InputStream es = rc.getErrorStream();
				if (es == null)
					throw new AdapterException(xmlpub,"Cannot get error stream. Connection " + publishername + " is not connected or the server sent no useful data within maximum allowed period");

				read = new InputStreamReader(es,"UTF-8");
			}

			StringBuilder sb = new StringBuilder();   
			int ch = read.read();
			while(ch != -1)
			{
				sb.append((char)ch);
				ch = read.read();
			}

			String response = sb.toString();

			int code = rc.getResponseCode();
			if (code < 200 || code >= 300)
				throw new AdapterException("HTTP error code " + code + ": " + response);

			String header;
			for(int i=1;(header = rc.getHeaderFieldKey(i)) != null;i++)
			{
				if (!header.equals("Set-Cookie")) continue;
				String cookies = rc.getHeaderField(i);
				String[] cookielist = cookies.split("; ");
				for(String cookie:cookielist)
				{
					if (!cookie.startsWith("JSESSIONID=")) continue;
					if (jsessionid == null || !cookie.equals(jsessionid))
						setSessionID(publishername,cookie);
				}
			}

			if (out != null) out.close();
			read.close();

			return response;
		}

		public String send(String string,XML xml,XML xmlpub) throws Exception
		{
			String result = null;

			switch(type) {
			case JMS:
				topicpublish.publish(string);
				break;
			case SOAPOPER:
				String operation = xmlpub.getAttribute("operation");
				String port = xmlpub.getAttribute("port");

				Call call = (port == null) ? service.createCall() : service.createCall(new QName(port));
				if (operation != null)
					call.setOperationName(new QName(operation));

				result = (String)call.invoke(new Object[] { string });
				break;
			case EXEC:
				String charset = xmlpub.getAttribute("charset");
				if (charset == null) charset = "UTF-8";

				if (command.equals("-"))
				{
					XML resultxml = new XML();
					resultxml.add("output",Misc.exec(xml == null ? string : xml.getValue(),charset,null));
					result = resultxml.toString();
				}
				else
					result = Misc.exec(command,charset,string);
				break;
			case FILE:
				FileOutputStream os = new FileOutputStream(file,true);
				OutputStreamWriter out = new OutputStreamWriter(os,"UTF-8"); 
				out.write(string,0,string.length());
				out.write(Misc.CR);
				out.flush();
				os.close();
				result = string;
				break;
			case DIRECT:
				if (xml == null) throw new AdapterException("Direct publisher only supports XML");
				String subname = xmlpub.getAttribute("name");
				ArrayList<Subscriber> sublist = javaadapter.subscriberGet(subname);
				if (sublist == null)
				{
					Misc.log(1,"WARNING: Subscriber " + subname + " not found. Message discarded");
				}
				else
				{
					for(Subscriber sub:sublist)
					{
						XML resultxml = sub.run(xml);
						if (resultxml == null) return null;
						result = resultxml.toString();
					}
				}
				break;
			case LDAP:
				if (xml == null) throw new AdapterException("LDAP publisher only supports XML");
				XML resultxml = ld.oper(xml,xmlpub);
				if (resultxml == null) return null;
				result = resultxml.toString();
				break;
			default:
				try
				{
					result = sendHttpRequest(string,xml,xmlpub);
				}
				catch(Exception ex)
				{
					setSessionID(publishername,null);
					Misc.rethrow(ex,"Error publishing to " + publishername + ": " + string);
				}
				break;
			}

			return result;
		}
	}

	private int default_soap_connect_timeout = 10000;
	private int default_soap_first_read_timeout = 120000;
	private int default_soap_read_timeout = 60000;

	private static Publisher instance;
	private HashMap<String,String> sessionids = new HashMap<String,String>();
	private HashMap<String,SSLContext> clientsslcontexts = new HashMap<String,SSLContext>();

	static public HashMap<String,PublisherObject> publishers = new HashMap<String,PublisherObject>();

	private Publisher()
	{
		String str = System.getProperty("javaadapter.soap.timeout.connect");
		if (str != null) default_soap_connect_timeout = new Integer(str);

		str = System.getProperty("javaadapter.soap.timeout.read");
		if (str != null) default_soap_first_read_timeout = default_soap_read_timeout = new Integer(str);

		str = System.getProperty("javaadapter.soap.timeout.first.read");
		if (str != null) default_soap_first_read_timeout = new Integer(str);
	}

	public synchronized static Publisher getInstance()
	{
		if (instance == null)
			instance = new Publisher();
		return instance;
	}

	public synchronized void setSessionID(String name,String id)
	{
		if (Misc.isLog(9)) Misc.log("Setting JSESSION for name " + name + " with id " + id);
		if (id == null)
			sessionids.remove(name);
		else
			sessionids.put(name,id);
	}

	public synchronized String getSessionID(String name)
	{
		return sessionids.get(name);
	}

	public synchronized void publisherRemove(XML xmlpublisher) throws Exception
	{
		XML[] xmllist = xmlpublisher.getElements("publisher");
                if (xmllist.length == 0 && xmlpublisher.getTagName().equals("publisher") && Misc.checkActivate(xmlpublisher) != null)
                        xmllist = new XML[] { xmlpublisher };

		for(XML el:xmllist)
		{
			String name = el.getAttribute("name");
			if (name == null) continue;

			if (!publishers.containsKey(name)) continue;

			publishers.remove(name);
			setSessionID(name,null);
		}
	}

	private synchronized XML[] publisherInit(XML xmlpublisher) throws Exception
	{
		XML[] xmllist = xmlpublisher.getElements("publisher");
                if (xmllist.length == 0 && xmlpublisher.getTagName().equals("publisher") && Misc.checkActivate(xmlpublisher) != null)
                        xmllist = new XML[] { xmlpublisher };

		for(XML el:xmllist)
		{
			String name = el.getAttribute("name");
			if (name == null) continue;

			if (publishers.containsKey(name)) continue;

			PublisherTypes type = el.getAttributeEnum("type",PublisherTypes.JMS,PublisherTypes.class);
			System.out.print("Initializing publisher " + name + " (" + type.toString().toLowerCase() + ")... ");

			PublisherObject pub = null;

			switch(type) {
			case JMS:
				pub = new PublisherObject(new TopicPublish(name));
				break;
			case SOAPOPER:
				String wsdl = el.getAttribute("wsdl");
				ServiceFactory factory = ServiceFactory.newInstance();
				URL wsdlurl = new URL(wsdl);
				Service service = factory.createService(wsdlurl,new QName(name));
				pub = new PublisherObject(service);
				break;
			case EXEC:
				String command = el.getAttribute("command");
				pub = new PublisherObject(command);
				break;
			case FILE:
				String filename = el.getAttribute("filename");
				pub = new PublisherObject(new File(javaadapter.getCurrentDir(),filename));
				break;
			case LDAP:
				String url = el.getAttribute("url");
				String notrustssl = el.getAttribute("notrustssl");
				boolean notrust = notrustssl != null && "true".equals(notrustssl);

				pub = new PublisherObject(new ldap(url,el.getAttribute("username"),el.getAttributeCrypt("password"),null,el.getAttribute("authentication"),el.getAttribute("referral"),el.getAttribute("derefAliases"),notrust));
				break;
			default:
				pub = new PublisherObject(type);
				break;
			}

			if (pub != null)
			{
				pub.setName(name);
				publishers.put(name,pub);
			}

			System.out.println("Done");
		}

		return xmllist;
	}

	public synchronized PublisherObject publisherGet(String name) throws Exception
	{
		if (name == null)
			throw new AdapterException("Publisher name attribute is mandatory");
		return publishers.get(name);
	}

	private String publish(String string,XML xml,XML xmlpublisher) throws Exception
	{
		String result = null;
		if (string == null) return result;

		if (Misc.isLog(9)) Misc.log("Publishing: " + string);

		XML[] xmllist = publisherInit(xmlpublisher);

		for(int i = 0;i < xmllist.length;i++)
		{
			XML el = xmllist[i];
			String name = el.getAttribute("name");

			PublisherObject pub = publisherGet(name);
			if (Misc.isLog(6)) Misc.log("Sending message to " + name + ": " + string);
			result = pub.send(string,xml,el);
			if (Misc.isLog(9)) Misc.log("Sending " + i + " done. Result is: " + result);
		}
		return result;
	}

	public String publish(String string,XML xmlpublisher) throws Exception
	{
		return publish(string,null,xmlpublisher);
	}

	public XML publish(XML xml,XML xmlpublisher) throws Exception
	{
		String str = xml.rootToString();
		String result = publish(str,xml,xmlpublisher);

		if (javaadapter.isShuttingDown()) return null;

		if (result == null)
		{
			if (Misc.isLog(5)) Misc.log("WARNING: No publisher response. Message was: " + str);
			return null;
		}

		if (result.length() == 0)
		{
			if (Misc.isLog(5)) Misc.log("WARNING: Empty publisher response. Message was:" + str);
			return null;
		}

		if (result.toLowerCase().startsWith("<html>"))
			throw new AdapterException(xmlpublisher,"HTML received from publisher when XML is expected: " + result + Misc.CR + "Message was: " + str);

		if (result.startsWith("{") && result.endsWith("}"))
			return new XML(new org.json.JSONObject(result));
		return new XML(new StringBuilder(result));
	}
}
