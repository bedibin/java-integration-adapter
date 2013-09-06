import java.io.*;
import javax.servlet.*;

public final class JavaAdapterListener implements ServletContextListener
{
	public void contextInitialized(ServletContextEvent event)
	{
		ServletContext ctx = event.getServletContext();
		String cfgfile = ctx.getInitParameter("configurationFile");
		String path = ctx.getRealPath("/");
		javaadapter.currentdir = path + File.separatorChar + "WEB-INF";
		System.out.println("Java Adapter current directory: " + javaadapter.getCurrentDir());

		try
		{
			javaadapter.init(cfgfile);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

		System.out.println("Java Adapter listener context created");
	}

	public void contextDestroyed(ServletContextEvent event)
	{
		javaadapter.shutdown.run();
		System.out.println("Java Adapter listener context destroyed");
	}

}

