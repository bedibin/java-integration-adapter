import java.io.*;
import java.util.*;
import java.net.*;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.X509Certificate;
import org.json.JSONObject;
import org.json.JSONArray;

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
 
		if (Misc.isLog(30))
		{
			for (int k=0; k<issuers.length; k++)
				Misc.log("Alias issuer:" + issuers[k]);
		}

		for (int i=0; i<keyType.length && !aliasFound; i++)
		{
			if (Misc.isLog(30)) Misc.log("Alias key type:" + keyType[i]);
			String[] validAliases = sourceKeyManager.getClientAliases(keyType[i],issuers);
			if (validAliases != null)
				for (int j=0;j < validAliases.length && !aliasFound;j++)
				{
					if (Misc.isLog(30)) Misc.log("Alias valid:" + validAliases[j]);
					if (validAliases[j].equals(alias))
						aliasFound=true;
				}
		}
 
		if (aliasFound)
		{
			if (Misc.isLog(30)) Misc.log("Alias found: " + alias);
			return alias;
		}

		if (Misc.isLog(30)) Misc.log("Alias not found: " + keyType.length);
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

class AdapterExtendPublish extends AdapterExtend
{
	public void publish(String string,XML xmlpublish) throws AdapterException
	{
		PublishBase ctx = (PublishBase)getInstance(xmlpublish);
		String name = xmlpublish.getAttribute("name");
		ctx.publish(name,string);
	}
}

class PublishBase extends AdapterExtendBase
{
	void publish(String name,String message) throws AdapterException { throw new AdapterException("Publish not supported"); }
}

enum PublisherTypes { DIRECT, JMS, LDAP, SOAP, HTTP, POST, EXEC, FILE };
enum HttpMethods { GET, POST, PUT, DELETE };

class Publisher
{
	class PublisherObject
	{
		private PublisherTypes type;
		private URL defaulturl;
		private String command;
		private File file;
		private ldap ld;
		private String publishername;

		public PublisherObject(PublisherTypes type)
		{
			this.type = type;
		}

		public PublisherObject(ldap ld)
		{
			type = PublisherTypes.LDAP;
			this.ld = ld;
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

		public String sendHttpRequest(String body,XML xmlpub) throws GeneralSecurityException,IOException,AdapterException
		{
			return sendHttpRequest(body,null,xmlpub);
		}

		public String sendHttpRequest(String body,XML xml,XML xmlpub) throws GeneralSecurityException,IOException,AdapterException
		{
			String urlstr = xml == null ? Misc.substitute(xmlpub.getAttribute("url")) : Misc.substitute(xmlpub.getAttribute("url"),xml);
			if (urlstr == null && defaulturl == null) urlstr = "http://localhost/";
			URL url = urlstr == null ? defaulturl : new URL(urlstr);
			if (url == null) throw new AdapterException(xmlpub,"No URL provided");

			String charset = xmlpub.getAttribute("charset");
			if (charset == null) charset = "UTF-8";

			HttpMethods method = xmlpub.getAttributeEnum("method",HttpMethods.POST,HttpMethods.class);
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
						// https://stackoverflow.com/questions/53709608/how-do-i-use-client-certificates-in-a-client-java-application
						String clientkeystoreformat = xmlpub.getAttributeCrypt("clientkeystoreformat");
						if (clientkeystoreformat == null) clientkeystoreformat = KeyStore.getDefaultType();
						KeyStore ks = KeyStore.getInstance(clientkeystoreformat);

						FileInputStream fis = new FileInputStream(clientkeystore);
						String clientkeystorepassword = xmlpub.getAttributeCrypt("clientkeystorepassword");
						if (clientkeystorepassword == null) clientkeystorepassword = "changeit";
						ks.load(fis,clientkeystorepassword.toCharArray());
						fis.close();

						KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
						String clientkeypassword = xmlpub.getAttributeCrypt("clientkeystorekeypassword");
						if (clientkeypassword == null) clientkeypassword = clientkeystorepassword;
						kmf.init(ks,clientkeypassword.toCharArray());
						TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
						tmf.init(ks);

						KeyManager[] kms = kmf.getKeyManagers();
						TrustManager[] tms = tmf.getTrustManagers();

						String alias = xmlpub.getAttributeCrypt("clientkeystorealias");
						if (alias != null)
							for (int i = 0;i < kms.length;i++)
            							if (kms[i] instanceof X509KeyManager)
                							kms[i]=new AliasSelectorKeyManager((X509KeyManager)kms[i],alias);

						sc = SSLContext.getInstance("TLSv1.2");
						sc.init(kms,tms,new SecureRandom());
						clientsslcontexts.put(publishername,sc);
					}
					((HttpsURLConnection)rc).setSSLSocketFactory(sc.getSocketFactory());
				}
			}

			rc.setConnectTimeout(default_soap_connect_timeout);
			rc.setRequestMethod(method.toString());
			rc.setDoOutput(true);
			rc.setDoInput(true); 
			setRequestProperties(publishername,rc);

			String auth = xmlpub.getAttribute("authorization");
			if (auth == null)
			{
				String username = xmlpub.getAttribute("username");
				if (username != null)
				{
					String password = xmlpub.getAttributeCrypt("password");
					if (password == null) password = "";
					String authstr = username + ":" + password;
					auth = new String(base64coder.encode(authstr.getBytes(charset)));
					rc.setRequestProperty("Authorization","Basic " + auth);
				}
			}
			else
				rc.setRequestProperty("Authorization",Misc.substitute(auth));

			String boundary = null;
			PublisherTypes type = xmlpub.getAttributeEnum("type",PublisherTypes.class);
			switch(type) {
			case SOAP:
				rc.setRequestProperty("Content-Type","text/xml; charset=" + charset);
				String action = xmlpub.getAttribute("action");
				rc.setRequestProperty("SOAPAction",action == null ? "" : action);
				break;
			case POST:
			case HTTP:
				String content = xmlpub.getAttribute("content_type");
				if (content == null) content = "application/x-www-form-urlencoded";
				if (content.startsWith("multipart/form-data"))
				{
					boundary = "------------" + System.currentTimeMillis();
					content += "; boundary=" + boundary;
				}
				rc.setRequestProperty("Content-Type",content);
				break;
			}

			byte[] rawrequest = null;
			String filename = xmlpub.getAttribute("filename");
			if (filename != null) body = Misc.readFile(xmlpub);

			if (body != null && method != HttpMethods.GET && method != HttpMethods.DELETE)
			{
				if (boundary != null)
				{
					body = "--" + boundary + "\n" +
						"Content-Disposition: form-data; name=\"uploadFile\"" +
						(filename == null ? "\n" : "; filename=\"" + filename + "\"\n") +
						"Content-Type: application/octet-stream; charset=" + charset + "\n\n" +
						body + "\n" +
						"--" + boundary + "--\n";
					if (Misc.isLog(30)) Misc.log("Multipart HTTP content: " + body);
				}
				rawrequest = body.getBytes(charset);
				int len = rawrequest.length;
				rc.setRequestProperty("Content-Length",Integer.toString(len));
			}

			rc.connect();

			OutputStream out = null;
			if (rawrequest != null)
			{
				out = rc.getOutputStream();
				out.write(rawrequest);
				out.flush();
			}

			InputStreamReader read = null;

			try
			{
				read = new InputStreamReader(rc.getInputStream(),charset);
			}
			catch(IOException ex)
			{
				if (Misc.isLog(3)) Misc.log("HTTP exception: " + ex);
				InputStream es = rc.getErrorStream();
				if (es == null)
					throw new AdapterException(xmlpub,"Cannot get error stream. Connection \"" + publishername + "\" is not connected or the server sent no useful data within maximum allowed period");

				read = new InputStreamReader(es,charset);
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
			if (Misc.isLog(30)) Misc.log("http response code: " + code);
			if (code < 200 || code >= 300)
				throw new AdapterException("HTTP error code " + code + ": " + response);

			String header;
			for(int i=1;(header = rc.getHeaderFieldKey(i)) != null;i++)
			{
				if (!header.equals("Set-Cookie")) continue;
				if (Misc.isLog(30)) Misc.log("Set-Cookie header: " + rc.getHeaderField(i));
				String cookies = rc.getHeaderField(i);
				setSessionProperty(publishername,cookies);
			}

			if (out != null) out.close();
			read.close();

			return response;
		}

		public String send(String string,XML xml,XML xmlpub) throws Exception
		{
			String result = string;

			switch(type) {
			case JMS:
				JMS.getInstance().publish(string,xmlpub);
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
				try {
					out.write(string,0,string.length());
					out.write(Misc.CR);
					out.flush();
				} finally {
					out.close();
				}
				os.close();
				break;
			case DIRECT:
				if (xml == null) throw new AdapterException("Direct publisher only supports XML");
				String subname = xmlpub.getAttribute("name");
				List<Subscriber> sublist = javaadapter.subscriberGet(subname);
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
				catch(IOException ex)
				{
					clearSession(publishername);
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
	private Map<String,Map<String,String>> sessioncookies = new HashMap<>();
	private HashMap<String,SSLContext> clientsslcontexts = new HashMap<>();

	private static HashMap<String,PublisherObject> publishers = new HashMap<>();

	private Publisher()
	{
		String str = System.getProperty("javaadapter.soap.timeout.connect");
		if (str != null) default_soap_connect_timeout = Integer.parseInt(str);

		str = System.getProperty("javaadapter.soap.timeout.read");
		if (str != null) default_soap_first_read_timeout = default_soap_read_timeout = Integer.parseInt(str);

		str = System.getProperty("javaadapter.soap.timeout.first.read");
		if (str != null) default_soap_first_read_timeout = Integer.parseInt(str);
	}

	public synchronized static Publisher getInstance()
	{
		if (instance == null)
			instance = new Publisher();
		return instance;
	}

	public synchronized void clearSession(String name)
	{
		sessioncookies.remove(name);
	}

	public synchronized void setSessionProperty(String name,String property)
	{
		String[] props = property.split(";\\s*");
		for(String prop:props)
		{
			String[] token = prop.split("\\s*=\\s*",2);
			if (token.length != 2) return;
			String key = token[0];
			String value = token[1];
		
			if (Misc.isLog(9)) Misc.log("Setting property for name " + name + " with key " + key + (Misc.isLog(30) ? ": " + value : ""));
			Map<String,String> cookies = sessioncookies.get(name);
			if (cookies == null)
			{
				cookies = new LinkedHashMap<String,String>();
				sessioncookies.put(name,cookies);
			}
			cookies.put(key,value);
		}
	}

	public synchronized void setRequestProperties(String name,HttpURLConnection rc)
	{
		Map<String,String> cookies = sessioncookies.get(name);
		if (cookies == null)
		{
			rc.setReadTimeout(default_soap_first_read_timeout);
			return;
		}

		rc.setReadTimeout(default_soap_read_timeout);
		String sep = null;
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String,String> cookie:cookies.entrySet())
		{
			String key = cookie.getKey();
			String value = cookie.getValue();
			sb.append((sep == null ? "" : sep) + key + "=" + value);
			sep = "; ";
		}

		if (sep != null)
		{
			if (Misc.isLog(30)) Misc.log("Cookie: " + sb.toString());
			rc.setRequestProperty("Cookie",sb.toString());
		}
	}

	public synchronized void publisherRemove(XML xmlpublisher) throws AdapterException
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
			clearSession(name);
		}
	}

	private synchronized XML[] publisherInit(XML xmlpublisher) throws AdapterException
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

				try {
					pub = new PublisherObject(new ldap(url,el.getAttribute("username"),el.getAttributeCrypt("password"),null,el.getAttribute("authentication"),el.getAttribute("referral"),el.getAttribute("derefAliases"),notrust));
				} catch(IOException | javax.naming.NamingException ex) {
					throw new AdapterException(ex);
				}
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

	public synchronized PublisherObject publisherGet(String name) throws AdapterException
	{
		if (name == null)
			throw new AdapterException("Publisher name attribute is mandatory");
		return publishers.get(name);
	}

	private String publish(String string,XML xml,XML xmlpublisher) throws AdapterException
	{
		String result = null;
		if (Misc.isLog(9)) Misc.log("Publishing: " + string);

		XML[] xmllist = publisherInit(xmlpublisher);

		for(int i = 0;i < xmllist.length;i++)
		{
			XML el = xmllist[i];
			String name = el.getAttribute("name");

			PublisherObject pub = publisherGet(name);
			if (Misc.isLog(6)) Misc.log("Sending message to " + name + ": " + string);
			try {
				result = pub.send(string,xml,el);
			} catch(Exception ex) {
				throw new AdapterException(ex);
			}
			if (Misc.isLog(9)) Misc.log("Sending " + i + " done. Result is: " + result);
		}
		return result;
	}

	public String publish(String string,XML xmlpublisher) throws AdapterException
	{
		return publish(string,null,xmlpublisher);
	}

	public XML publish(XML xml,XML xmlpublisher) throws AdapterException
	{
		if (xml == null) return null;
		if (Misc.isLog(5) && xml.isEmpty()) Misc.log("WARNING: Nothing to publish");

		String str;
		if ("raw".equals(xml.getTagName()))
			str = xml.getValue();
		else if ("jsonarray".equals(xml.getTagName()))
		{
			List<JSONObject> array = new ArrayList<>();
			XML[] list = xml.getElements(null);
			for(XML element:list)
				array.add(element.toJSON().getJSONObject("json"));
			str = (new JSONArray(array)).toString();
		}
		else if ("json".equals(xml.getTagName()))
			str = xml.toJSON().getJSONObject("json").toString();
		else
			str = xml.rootToString();
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
			return new XML();
		}

		if (result.toLowerCase().startsWith("<html>"))
			throw new AdapterException(xmlpublisher,"HTML received from publisher when XML is expected: " + result + Misc.CR + "Message was: " + str);

		if (result.startsWith("{") && result.endsWith("}"))
			return new XML(new org.json.JSONObject(result));
		return new XML(new StringBuilder(result));
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("Usage: config_file input_file");
			System.exit(1);
		}

		javaadapter.init(args[0]);
		Publisher publisher = getInstance();

		String filename = args[1];
		String file = Misc.readFile(filename);
		if (file == null)
		{
			System.err.println("File not found: " + filename);
			System.exit(1);
		}

		String result = publisher.publish(file,javaadapter.getConfiguration());
		System.out.println("Result: " + result);
	}
}
