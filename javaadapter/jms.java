import java.util.*;
import javax.jms.*;

class JMS extends AdapterExtend
{
	static protected JMS instance;

	public synchronized static JMS getInstance()
	{
		if (instance == null)
		{
			instance = new JMS();
			instance.defaultclass = "JNDITopic";
		}
		return instance;
	}
}

abstract class JMSBase extends AdapterExtendBase
{
	void setMessageListener(String name,MessageListener listener) throws JMSException {};
	void recover(String name) throws JMSException {};
	void start(String name) throws JMSException {};
}

class JMSServer
{
	class JMSInfo
	{
		private Subscriber subscriber;
		private JMSBase ctx;
		private String name;
		private int delay;

		public JMSInfo(JMSBase ctx,int delay)
		{
			this.ctx = ctx;
			this.delay = delay;
		}

		public void setSubscriber(Subscriber subscriber)
		{
			this.subscriber = subscriber;
		}
	}

	class JMSListener implements MessageListener
	{
		private JMSInfo info;

		public JMSListener(JMSInfo info) throws JMSException
		{
			this.info = info;
			javaadapter.setForShutdown(this);
			info.ctx.setMessageListener(srvname,this);
		}

		public void onMessage(Message msg)
		{
			if (Misc.isLog(12)) Misc.log("onMessage");

			Subscriber subscriber = info.subscriber;
			if (subscriber == null) return;
			if (javaadapter.isShuttingDown()) return;

			TextMessage msgtxt = (TextMessage)msg;
			try
			{
				if (Misc.isLog(12)) Misc.log("Msg received from " + info.name + ": " + msgtxt.getText());

				XML xml = new XML(new StringBuilder(msgtxt.getText()));
				XML function = subscriber.getFunction();
				if (Misc.isLog(12)) Misc.log("Subscriber " + subscriber.getName() + " class " + subscriber.getClass().getName());

				if (Misc.isFilterPass(function,xml))
				{
					if (Misc.isLog(9)) Misc.log("Processing message: " + msgtxt.getText());
					subscriber.run(xml);
				}

				if (Misc.isLog(12)) Misc.log("onMessage ack");
				msg.acknowledge();
			}
			catch(AdapterXmlException ex)
			{
				// Error if message is not valid XML. Just discard message.
				try
				{
					if (Misc.isLog(2)) Misc.log("WARNING: Discarded message from " + info.name + ": " + msgtxt.getText());
					msg.acknowledge();
				}
				catch(JMSException subex)
				{
				}
				Misc.log(ex);
			}
			catch(JMSException | AdapterException ex)
			{
				Misc.log(ex);
				try
				{
					if (info.delay <= 0)
						msg.acknowledge();
					else
					{
						if (Misc.isLog(9)) Misc.log("Waiting " + info.delay + " seconds before recovering...");
						for(int i = 0;i < info.delay;i++)
						{
							if (javaadapter.isShuttingDown()) return;
							Misc.sleep(1000);
						}
						info.ctx.recover(srvname);
					}
				}
				catch(JMSException | AdapterException subex)
				{
					Misc.log(subex);
				}
			}

			if (Misc.isLog(12)) Misc.log("onMessage done");
		}
	}

	private ArrayList<Subscriber> sublist;
	private ArrayList<JMSInfo> infolist;
	private String srvname;

	public JMSServer(String name,JMSBase ctx,XML xml) throws JMSException,AdapterException
	{
		srvname = name;
		sublist = Misc.initSubscribers(xml);
		infolist = new ArrayList<JMSInfo>();

		String delaystr = xml.getAttribute("exceptiondelay");
		int delay = 60;
		if (delaystr != null) delay = new Integer(delaystr);

		for(Subscriber subscriber:sublist)
			infolist.add(new JMSInfo(ctx,delay));

		ctx.start(srvname);

		System.out.println("Done");

		Misc.activateSubscribers(sublist);
		javaadapter.setForShutdown(this);

		for(int i = 0;i < sublist.size();i++)
		{
			JMSInfo info = infolist.get(i);
			info.setSubscriber(sublist.get(i));
			new JMSListener(info);
		}
	}
}
