/* IFDEF JAVA6 */
import java.util.*;
import java.io.*;
import java.util.regex.*;
import java.net.URLDecoder;
import java.net.InetSocketAddress;
import javax.net.ssl.*;
import java.security.*;
import com.sun.net.httpserver.*;
import com.sun.net.httpserver.spi.*;

class SoapServerStandAlone extends SoapServer implements HttpHandler
{
	private String dirtarget;
	private String httptype;

	class ServerAuthenticator extends BasicAuthenticator
	{
		private String username;
		private String password;

		public ServerAuthenticator(String realm,String username,String password)
		{
			super(realm);
			this.username = username;
			this.password = password == null ? "" : password;
		}

		public boolean checkCredentials(String username,String password)
		{
			if (!username.equals(this.username)) return false;
			if (!password.equals(this.password)) return false;
			return true;
		}
	}

	public SoapServerStandAlone(XML xml) throws AdapterException
	{
		super(xml);

		System.out.print("Soap server standalone initialisation... ");

		httptype = xml.getAttribute("type");
		XML fileserver = xml.getElement("fileserver");
		if (fileserver != null) dirtarget = fileserver.getValue();

		HttpServerProvider provider = HttpServerProvider.provider();
		InetSocketAddress addr = new InetSocketAddress(new Integer(xml.getAttribute("port")));
		HttpServer server;

		String keystore = xml.getAttribute("keystore");
		try {
			if (keystore == null)
				server = provider.createHttpServer(addr,0);
			else
			{
				String pass = xml.getAttributeCrypt("passphrase");
				if (pass == null) pass = "changeit";
				char[] passphrase = pass.toCharArray();

				KeyStore ks = KeyStore.getInstance("JKS");
				ks.load(new FileInputStream(keystore),passphrase);

				KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
				kmf.init(ks,passphrase);

				TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(ks);

				SSLContext ssl = SSLContext.getInstance("TLS");
				ssl.init(kmf.getKeyManagers(),tmf.getTrustManagers(),null);

				HttpsServer serverssl = provider.createHttpsServer(addr,0);
				serverssl.setHttpsConfigurator(new HttpsConfigurator(ssl));
				server = serverssl;
			}
		} catch(GeneralSecurityException | IOException ex) {
			throw new AdapterException(ex);
		}

		HttpContext context = server.createContext("/javaadapter",this);
		String username = xml.getAttribute("username");
		if (username != null)
			context.setAuthenticator(new ServerAuthenticator("javaadapter",username,xml.getAttributeCrypt("password")));
		server.setExecutor(null);
		server.start();

		System.out.println("Done");
	}

	void rawSend(HttpExchange exchange,int code,String type,byte[] raw) throws IOException
	{
		Headers response = exchange.getResponseHeaders();
		if (type != null) response.set("Content-Type",type);
		exchange.sendResponseHeaders(code,raw.length);
		OutputStream out = exchange.getResponseBody();
		out.write(raw);
		out.close();
	}

	void rawSend(HttpExchange exchange,int code,String type,String text) throws IOException
	{
		byte[] raw = text.getBytes("UTF-8");
		type = type == null ? null : type + ";charset=utf-8";
		rawSend(exchange,code,type,raw);
		if (code != 200) Misc.log("ERROR: " + text);
	}

	void htmlSend(HttpExchange exchange,String title,String body) throws IOException
	{
		String html = "<html><head><title>" + title;
		html += "</title></head><body>" + body;
		html += "</body></html>";
		rawSend(exchange,200,"text/html",html);
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException
	{
		String path = exchange.getRequestURI().getPath();
		String method = exchange.getRequestMethod().toUpperCase();
		String pathcontext = exchange.getHttpContext().getPath();
		Headers headers = exchange.getRequestHeaders();

		if (!path.startsWith(pathcontext)) return;
		path = path.substring(pathcontext.length());

		if (Misc.isLog(4)) Misc.log("soap server handle " + method + ": " + path);

		if (dirtarget != null && path.startsWith("/get"))
		{
			String message = null;

			if ("POST".equals(method))
			{
				String type =  headers.get("Content-type").get(0);
				Misc.log("Type: " + type);
				Pattern pat = Pattern.compile("boundary=((?:[^; ])+)");
				Matcher matcher = pat.matcher(type);
				String boundary = null;
				if (matcher.find()) boundary = "--" + matcher.group(1);
				Misc.log("Boundary: " + boundary);
				int len = new Integer(headers.get("Content-length").get(0));
				Misc.log("Length: " + len);
				byte[] raw = new byte[len];

				DataInputStream in = new DataInputStream(exchange.getRequestBody());
				in.readFully(raw);
				in.close();

				String firstboundary = null;
				StringBuilder sb = null;
				String uploadfile = null;
				int i = 0;
				int line = 0;
				do
				{
					sb = new StringBuilder();
					for(;i < raw.length;i++)
					{
						if (raw[i] == '\r') continue;
						if (raw[i] == '\n')
						{
							i++;
							break;
						}
						sb.append((char)raw[i]);
					}
					Misc.log("line: " + sb);
					if (line == 0) firstboundary = sb.toString();
					pat = Pattern.compile("Content-Disposition: .*filename=\"(.+)\"");
					matcher = pat.matcher(sb.toString());
					if (matcher.find())
					{
						uploadfile = matcher.group(1);
						Misc.log("Filename: " + uploadfile);
					}
					line++;
				} while(sb.length() != 0);

				String endboundary = new String(raw,len - boundary.length() - 4,boundary.length());
				Misc.log("End boundary " + len + "," + boundary.length() + ": " + endboundary);

				if (boundary == null)
					message = "No boundary in HTTP header";
				else if (!boundary.equals(firstboundary))
					message = "MIME payload doesn't have boundary as first line";
				else if (!boundary.equals(endboundary))
					message = "MIME payload doesn't have boundary as last line";
				else if (uploadfile == null)
					message = "No filename specified";
				else
				{
					try
					{
						File file = new File(javaadapter.getCurrentDir(),dirtarget + File.separator + uploadfile);
						FileOutputStream stream = new FileOutputStream(file,false);
						int size = len - i - boundary.length() - 6;
						stream.write(raw,i,size);
						stream.close();
						message = "File " + uploadfile + " (size=" + size + ") uploaded successfully";
					}
					catch(IOException ex)
					{
						message = ex.toString();
					}
				}
			}

			if (path.startsWith("/get/"))
			{
				String filename = dirtarget + File.separator + path.substring("/get/".length());
				File file = new File(javaadapter.getCurrentDir(),filename);
				if (!file.isFile())
				{
					rawSend(exchange,500,null,filename + " is not a file or not found");
					return;
				}
				Misc.log("Sending file " + file.getPath());
				byte[] raw = new byte[(int)file.length()];
				FileInputStream fis = new FileInputStream(file);
				DataInputStream in = new DataInputStream(fis);
				in.read(raw);
				in.close();
				rawSend(exchange,200,"application/octet-stream",raw);
				return;
			}

			String body = "";
			if (message != null) body += message + "<br>";
			body += "<table border=1><tr><th>Filename</th></tr>";
			File dir = new File(javaadapter.getCurrentDir(),dirtarget);
			File[] files = dir.listFiles();
			if (files == null)
			{
				rawSend(exchange,500,null,"Directory " + dirtarget + " not found");
				return;
			}
			for(int i = 0;i < files.length;i++)
				body += "<tr><td><a href=\"get/" + files[i].getName() + "\">" + files[i].getName() + "</a></td></tr>";

			body += "</table>";
			body += "<form action=\"get\" method=\"POST\" enctype=\"multipart/form-data\"><input type=\"file\" name=\"file\">";
			body += "<input type=\"submit\" value=\"Send file\"></form>";

			htmlSend(exchange,"Available files",body);
			return;
		}

		if (Misc.isLog(30)) for(Map.Entry<String,List<String>> entry:headers.entrySet())
			Misc.log("HTTP Headers " + entry.getKey() + ": " + entry.getValue());

		String charset = "utf-8";
		List<String> typelist = headers.get("Content-Type");
		if (typelist == null) typelist = headers.get("Content-type");
		if (typelist != null)
		{
			String contenttype = typelist.get(0);
			if (Misc.isLog(30)) Misc.log("Content type: " + contenttype);
			Pattern pat = Pattern.compile("(?<=charset=)[^;]*");
			Matcher m = pat.matcher(contenttype);
			if (m.find())
				charset = m.group(0);
		}

		if (Misc.isLog(30)) Misc.log("Using charset: " + charset);

		InputStreamReader in = new InputStreamReader(exchange.getRequestBody(),charset);
		//InputStream in = exchange.getRequestBody();
		StringBuilder sb = new StringBuilder();
		int ch = in.read();
		while(ch != -1)
		{
			sb.append((char)ch);
			ch = in.read();
		}
		if (Misc.isLog(30)) Misc.log("RAW HTTP Data: " + sb.toString());

		try
		{
			String action = null;
			boolean issoap = "POST".equals(method);

			if (!issoap && (httptype == null || !httptype.equals("post")))
				action = path.substring(1);
			else if (issoap && "/soap".equals(path) && (httptype == null || !httptype.equals("get")))
			{
				List<String> list = headers.get("SOAPAction");
				if (list != null)
					action = list.get(0).replaceAll("^\"","").replaceAll("\"$","");
			}

			if (action != null)
			{
				if (Misc.isLog(12)) Misc.log("Action received: " + action);

				for(Subscriber sub:getSubscribers())
				{
					if (Misc.isLog(12)) Misc.log("Comparing action with " + sub.getName());
					if (!action.equals(sub.getName())) continue;

					if (issoap)
					{
						String response = processXML(sb.toString(),sub);
						if (response != null)
						{
							rawSend(exchange,200,"text/xml",response);
							return;
						}
					}
					else
					{
						XML xmlinput = new XML();
						XML xmlroot = xmlinput.add("root");
						String query = exchange.getRequestURI().getQuery();
						if (query != null) for(String param:query.split("&"))
						{
							String pair[] = param.split("=");
							String name = URLDecoder.decode(pair[0],"utf-8");
							if (pair.length > 1)
								xmlroot.add(name,URLDecoder.decode(pair[1],"utf-8"));
							else
								xmlroot.add(name);
						}

						XML xmlresponse = sub.run(xmlinput);
						if (xmlresponse != null)
						{
							rawSend(exchange,200,"text/" + ("html".equals(xmlresponse.getTagName()) ? "html" : "xml"),xmlresponse.rootToString());
							return;
						}
					}
				}
			}
		}
		catch(AdapterException ex)
		{
			Misc.log(ex);
			try
			{
				SoapRequest request = new SoapRequest(ex);
				rawSend(exchange,500,null,request.rootToString());
				return;
			}
			catch(AdapterException ex2)
			{
				Misc.log(ex2);
			}
		}

		if ("GET".equals(method))
		{
			htmlSend(exchange,"Test","Java Adapter HTTP service is working");
			return;
		}

		rawSend(exchange,500,null,"Unhandled SOAP request");
	}
}
/* */
