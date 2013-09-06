import java.util.*;
import javax.jms.*;
import javax.naming.*;
//import weblogic.jms.extensions.*;

class JMSContext
{
	private InitialContext ctx;
	private TopicConnectionFactory factory;
	private static JMSContext instance;

	private JMSContext()
	{
	}

	public synchronized static JMSContext getInstance()
        {
		if (instance == null)
			instance = new JMSContext();
		return instance;
        }

	public void setContext(InitialContext ctx,TopicConnectionFactory factory)
	{
		this.ctx = ctx;
		this.factory = factory;
	}

	public InitialContext getContext()
	{
		return ctx;
	}

	public TopicConnectionFactory getFactory()
	{
		return factory;
	}
}

class TopicPublish
{
	private TopicSession session;
	private TopicPublisher publisher;

	public TopicPublish(String name) throws Exception
	{
		JMSContext context = JMSContext.getInstance();
		TopicConnection conn = context.getFactory().createTopicConnection();
		session = conn.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
		Topic topic = (Topic)context.getContext().lookup(name);
		publisher = session.createPublisher(topic);
	}

	public void publish(String string) throws Exception
	{
		TextMessage message = session.createTextMessage();
		message.setText(string);
		publisher.publish(message);
	}
}

class TopicServer
{
	class TopicInfo
	{
		private Subscriber subscriber;
		private TopicSubscriber topicsub;
		private TopicSession session;
		private String name;
		private int delay;

		public TopicInfo(TopicSubscriber topicsub,TopicSession session,int delay) throws Exception
		{
			this.topicsub = topicsub;
			this.session = session;
			name = topicsub.getTopic().getTopicName();
			this.delay = delay;
		}

		public void setSubscriber(Subscriber subscriber)
		{
			this.subscriber = subscriber;
		}
	}

	class TopicListener implements MessageListener
	{
		private TopicInfo topic;

		public TopicListener(TopicInfo topic) throws Exception
		{
			this.topic = topic;
			javaadapter.setForShutdown(this);
			TopicSubscriber topicsub = topic.topicsub;
			topicsub.setMessageListener(this);
		}

		public void close() throws Exception
		{
			TopicSubscriber topicsub = topic.topicsub;
			if (topicsub != null)
				topicsub.close();
		}

		public void onMessage(Message msg)
		{
			if (Misc.isLog(12)) Misc.log("onMessage");

			Subscriber subscriber = topic.subscriber;
			if (subscriber == null) return;
			if (javaadapter.isShuttingDown()) return;

			TextMessage msgtxt = (TextMessage)msg;
			try
			{
				if (Misc.isLog(12)) Misc.log("Msg received from topic " + topic.name + ": " + msgtxt.getText());

				XML xml = new XML(msgtxt);
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
			catch(org.xml.sax.SAXParseException ex)
			{
				// Error if message is not valid XML. Just discard message.
				try
				{
					if (Misc.isLog(2)) Misc.log("WARNING: Discarded message from " + topic.name + ": " + msgtxt.getText());
					msg.acknowledge();
				}
				catch(Exception subex)
				{
				}
				Misc.log(ex);
			}
			catch(Exception ex)
			{
				Misc.log(ex);
				try
				{
					if (topic.delay <= 0)
						msg.acknowledge();
					else
					{
						if (Misc.isLog(9)) Misc.log("Waiting " + topic.delay + " seconds before recovering...");
						for(int i = 0;i < topic.delay;i++)
						{
							if (javaadapter.isShuttingDown()) return;
							Thread.sleep(1000);
						}
						topic.session.recover();
					}
				}
				catch(Exception subex)
				{
					Misc.log(subex);
				}
			}

			if (Misc.isLog(12)) Misc.log("onMessage done");
		}
	}

	private ArrayList<Subscriber> sublist;
	private ArrayList<TopicInfo> topiclist;
	private TopicConnection conn;

	public TopicServer(XML xml,InitialContext ctx,TopicConnectionFactory factory) throws Exception
	{
		String topicname = xml.getAttribute("name");

		System.out.print("Initializing topic " + topicname);

		Topic topic = (Topic)ctx.lookup(topicname);
		conn = factory.createTopicConnection();
		String clientid = xml.getAttribute("clientid");
		if (clientid != null)
		{
			conn.setClientID(clientid);
			System.out.print(" (" + clientid + ")");
		}
		System.out.print("... ");

		sublist = Misc.initSubscribers(xml);
		topiclist = new ArrayList<TopicInfo>();

		String delaystr = xml.getAttribute("exceptiondelay");
		int delay = 60;
		if (delaystr != null) delay = new Integer(delaystr);

		for(Subscriber subscriber:sublist)
		{
			TopicSession sess = conn.createTopicSession(false,Session.CLIENT_ACKNOWLEDGE);
			TopicSubscriber topicsub = clientid == null ? sess.createSubscriber(topic) : sess.createDurableSubscriber(topic,subscriber.getName());

//			WLTopicSession wlsess = (WLTopicSession)sess;
//			wlsess.setRedeliveryDelay(60000);

			topiclist.add(new TopicInfo(topicsub,sess,delay));
		}

		conn.start();

		System.out.println("Done");

		Misc.activateSubscribers(sublist);
		javaadapter.setForShutdown(this);

		for(int i = 0;i < sublist.size();i++)
		{
			TopicInfo info = topiclist.get(i);
			info.setSubscriber(sublist.get(i));
			new TopicListener(info);
		}
	}

	public void close() throws Exception
	{
		if (conn != null)
			conn.close();
	}
}

