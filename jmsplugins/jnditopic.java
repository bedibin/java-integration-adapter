import java.util.*;
import javax.jms.*;
import javax.naming.*;
//import weblogic.jms.extensions.*;

class JNDITopic extends JMSBase
{
	private InitialContext ctx;
	private TopicConnectionFactory factory;

	class TopicInfo
	{
		private TopicConnection connection;
		private TopicSession session;
		private Topic topic;
		private TopicPublisher publisher;
		private TopicSubscriber subscriber;
		private String clientid;
		private String name;

		TopicInfo(String name) throws JMSException
		{
			this.name = name;
			connection = factory.createTopicConnection();
			session = connection.createTopicSession(false,Session.CLIENT_ACKNOWLEDGE);
			try {
				topic = (Topic)ctx.lookup(name);
			} catch(NamingException ex) {
				throw new JMSException(ex.getMessage());
			}
		}

		void setClientId(String clientid)
		{
			this.clientid = clientid;
		}

		TopicConnection getConnection() { return connection; }
		TopicSession getSession() { return session; }

		void send(String message) throws JMSException
		{
			if (publisher == null) publisher = session.createPublisher(topic);
			TextMessage text = session.createTextMessage();
			text.setText(message);
			publisher.publish(text);
		}

		TopicSubscriber getSubscriber() throws JMSException
		{
			if (subscriber == null)
			{
				if (clientid == null)
					subscriber = session.createSubscriber(topic);
				else
				{
					connection.setClientID(clientid);
					subscriber = session.createDurableSubscriber(topic,name);
				}
			}
			return subscriber;
		}
	}

	private HashMap<String,TopicInfo> infos = new HashMap<String,TopicInfo>();

	private synchronized TopicInfo getInfo(String name) throws JMSException
	{
		TopicInfo info = infos.get(name);
		if (info == null)
		{
			info = new TopicInfo(name);
			infos.put(name,info);
		}
		return info;
	}

	public JNDITopic(XML xml) throws Exception
	{
		System.out.print("Connection to JMS (JNDI/Topic) bus... ");
		Hashtable<String,String> env = new Hashtable<String,String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,xml.getValue("context","weblogic.jndi.WLInitialContextFactory"));
		env.put(Context.PROVIDER_URL,xml.getValue("url"));
		env.put(Context.SECURITY_PRINCIPAL,xml.getValue("username"));
		env.put(Context.SECURITY_CREDENTIALS,xml.getValueCrypt("password"));
		ctx = new InitialContext(env);
		//shutdown.setCloseList(xml);

		factory = (TopicConnectionFactory)ctx.lookup(xml.getValue("connectionfactory"));
		System.out.println("Done");

		XML[] jmslist = xml.getElements("topic");
			for(XML jmsxml:jmslist)
			{
				String name = jmsxml.getAttribute("name");
				String clientid = xml.getAttribute("clientid");

				TopicInfo info = getInfo(name);
				if (clientid != null) info.setClientId(clientid);
				info.getSubscriber();

				new JMSServer(name,this,jmsxml);
			}

	}

	void publish(String name,String message) throws JMSException
	{
		getInfo(name).send(message);
	}

	void setMessageListener(String name,MessageListener listener) throws JMSException
	{
		getInfo(name).getSubscriber().setMessageListener(listener);
	}

	void start(String name) throws JMSException
	{
		getInfo(name).getConnection().start();
	}

	void recover(String name) throws JMSException
	{
		getInfo(name).getSession().recover();
	}

	String read(String name) throws JMSException
	{
		Message msg = getInfo(name).getSubscriber().receive(1);
		if (msg == null) return null;
		String text = ((TextMessage)msg).getText();
		msg.acknowledge();
		return text;
	}

}
