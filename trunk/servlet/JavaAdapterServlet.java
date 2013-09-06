import java.util.*;
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

public final class JavaAdapterServlet extends HttpServlet
{
	private static final long serialVersionUID = -2926431505511013528L;
	private ArrayList<SoapServer> soapservers = javaadapter.soapservers;

	public void init(ServletConfig config) throws ServletException
	{
		super.init(config);
	}

	private boolean checkAuthentication(HttpServletRequest request,XML xml) throws Exception
	{
		String username = xml.getAttribute("username");
		if (username == null) return true;

		if (Misc.isLog(10)) Misc.log("Username: " + username);

		String auth = request.getHeader("Authorization");
		if (auth != null && auth.startsWith("Basic "))
		{
			if (Misc.isLog(10)) Misc.log("Auth: " + auth);
			auth = auth.substring(6);
			String authstr = new String(base64coder.decode(auth),"UTF-8");
			if (Misc.isLog(10)) Misc.log("Decoded auth: " + authstr);
			String[] split = authstr.split(":");
			if (split.length == 2)
			{
				String password = xml.getAttributeCrypt("password");
				if (password == null) password = "";

				if (username.equals(split[0]) && password.equals(split[1]))
					return true;
			}
		}

		return false;
	}

	private String read(HttpServletRequest request) throws IOException
	{
		InputStream in = request.getInputStream();
		StringBuilder sb = new StringBuilder();
		int ch = in.read();
		while(ch != -1)
		{
			sb.append((char)ch);
			ch = in.read();
		}

		return sb.toString();
	}

	private void write(HttpServletResponse response,String type,String text) throws IOException
	{
		response.setContentType("text/" + type + ";charset=utf-8");

		OutputStream out = response.getOutputStream();
		out.write(text.getBytes("UTF-8"));
		out.close();
	}

	public void doGet(HttpServletRequest request,HttpServletResponse response) throws IOException,ServletException
	{

		String path = request.getServletPath();
		if (Misc.isLog(10)) Misc.log("Get servlet path: " + path);
		if (path == null || path.equals(""))
		{
			path = request.getPathInfo();
			if (Misc.isLog(10)) Misc.log("Get path info: " + path);
		}
		if (path == null || path.equals(""))
			path = "/";

		read(request);

		for(SoapServer server:soapservers)
		{
			try
			{
				XML xml = server.getXML();

				String type = xml.getAttribute("type");
				if (type == null || type.equals("post")) break;

				if (!checkAuthentication(request,xml))
				{
					response.sendError(401,"Authorization Required");
					return;
				}

				ArrayList<Subscriber> subs = server.getSubscribers();
				for(Subscriber sub:subs)
				{
					String action = path.substring(1);
					if (Misc.isLog(12)) Misc.log("Action received: " + action);

					if (!action.equals(sub.getName())) continue;

					XML xmlinput = new XML();
					XML xmlroot = xmlinput.add("root");
					for (Enumeration e = request.getParameterNames();e.hasMoreElements();)
       					{
						String name = (String)e.nextElement();
						String[] values = request.getParameterValues(name);
						if (values == null)
							xmlroot.add(name);
						else
							for(String value:values)
								xmlroot.add(name,value);
					}

					XML xmlresponse = sub.run(xmlinput);
					if (xmlresponse != null)
					{
						write(response,"html".equals(xmlresponse.getTagName()) ? "html" : "xml",xmlresponse.rootToString());
						return;
					}
				}
			}
			catch(Exception ex)
			{
				Misc.log(ex);
				response.sendError(500,ex.toString());
				return;
			}
		}

		write(response,"html","<html><head><title>Test</title></head><body>Java Adapter servlet is working</body></html>");
	}

	public void doPost(HttpServletRequest request,HttpServletResponse response) throws IOException,ServletException
	{
		String path = request.getServletPath();
		Misc.log(4,"soap servlet path: " + path);
		if (path == null || path.equals(""))
		{
			path = request.getPathInfo();
			Misc.log(4,"soap path info: " + path);
		}

		if (!path.startsWith("/soap"))
		{
			response.sendError(500,"Invalid SOAP request path");
			return;
		}

		String input = read(request);

		String actionstring = request.getHeader("SOAPAction");
		if (actionstring == null)
		{
			response.sendError(500,"SOAPAction required");
			return;
		}

		if (soapservers != null) for(SoapServer server:soapservers)
		{
			try
			{
				XML xml = server.getXML();

				String type = xml.getAttribute("type");
				if (type != null && type.equals("get")) break;

				if (!checkAuthentication(request,xml))
				{
					response.sendError(401,"Authorization Required");
					return;
				}

				ArrayList<Subscriber> subs = server.getSubscribers();
				for(Subscriber sub:subs)
				{
					if (!actionstring.equals(sub.getName())) continue;
					String respstr = server.processXML(input,sub);
					if (respstr != null)
					{
						write(response,"xml",respstr);
						return;
					}
				}
			}
			catch(Exception ex)
			{
				Misc.log(ex);
				try
				{
					SoapRequest soaprequest = new SoapRequest(ex);
					response.sendError(500,soaprequest.rootToString());
					return;
				}
				catch(Exception ex2)
				{
					Misc.log(ex2);
				}
			}
		}

		response.sendError(500,"Unhandled SOAP request");
	}
}

