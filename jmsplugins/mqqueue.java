import java.util.*;
import javax.jms.*;
import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

class MQQueue extends JMSBase
{
	private JMSContext ctx;

	class MQInfo
	{
		private Destination destination;
		private JMSProducer producer;
		private JMSConsumer consumer;

		MQInfo(String name)
		{
			destination = ctx.createQueue("queue:///" + name);
		}

		void send(String message)
		{
			if (producer == null) producer = ctx.createProducer();
			producer.send(destination,message);
		}

		JMSConsumer getConsumer()
		{
			if (consumer == null) consumer = ctx.createConsumer(destination);
			return consumer;
		}
	};

	private HashMap<String,MQInfo> infos = new HashMap<String,MQInfo>();

	private synchronized MQInfo getInfo(String name)
	{
		MQInfo info = infos.get(name);
		if (info == null)
		{
			info = new MQInfo(name);
			infos.put(name,info);
		}
		return info;
	}

	public MQQueue(XML xml) throws Exception
	{
		// https://stackoverflow.com/questions/52775733/problem-connecting-a-java-client-jms-to-a-ibm-mq
		// https://stackoverflow.com/questions/2692070/connecting-to-a-websphere-mq-in-java-with-ssl-keystore
		// https://www.ibm.com/developerworks/websphere/library/techarticles/0510_fehners/0510_fehners.html
		// https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_8.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/msg/client/wmq/common/CommonConstants.html#WMQ_HOST_NAME
		System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","false");
		String keystore = xml.getValue("keystore",null);
		if (keystore != null) System.setProperty("javax.net.ssl.keyStore",keystore);
		XML keystorepass = xml.getElement("keystore_password");
		if (keystorepass != null) System.setProperty("javax.net.ssl.keyStorePassword",keystorepass.getValueCrypt());

		System.out.print("Connection to JMS (MQ/Queue) bus... ");
		JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
		JmsConnectionFactory cf = ff.createConnectionFactory();
		cf.setStringProperty(WMQConstants.WMQ_HOST_NAME,xml.getValue("host"));
		cf.setIntProperty(WMQConstants.WMQ_PORT,new Integer(xml.getValue("port","1414")));
		cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE,WMQConstants.WMQ_CM_CLIENT);
		cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME,javaadapter.getName());
		cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP,true);
		String cipher = xml.getValue("cipher",null);
		if (cipher != null) cf.setStringProperty(WMQConstants.WMQ_SSL_CIPHER_SUITE,cipher);
		String channel = xml.getValue("channel",null);
		if (channel != null) cf.setStringProperty(WMQConstants.WMQ_CHANNEL,channel);
		String qmgr = xml.getValue("qmgr",null);
		if (qmgr != null) cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER,qmgr);
		String username = xml.getValue("username",null);
		if (username != null) cf.setStringProperty(WMQConstants.USERID,username);
		XML password = xml.getElement("password");
		if (password != null) cf.setStringProperty(WMQConstants.PASSWORD,password.getValueCrypt());
		ctx = cf.createContext();
		System.out.println("Done");

		XML[] jmslist = xml.getElements("queue");
			for(XML jmsxml:jmslist)
			{
				String name = jmsxml.getAttribute("name");
				getInfo(name).getConsumer();
				new JMSServer(name,this,jmsxml);
			}
	}

	void publish(String name,String message)
	{
		getInfo(name).send(message);
	}

	void setMessageListener(String name,MessageListener listener)
	{
		getInfo(name).getConsumer().setMessageListener(listener);
	}
}
