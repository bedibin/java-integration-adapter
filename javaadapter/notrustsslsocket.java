import java.net.*;
import javax.net.*;
import java.security.cert.*;
import javax.net.ssl.*;
import java.io.IOException;
import java.security.SecureRandom;

class DummyTrustmanager implements X509TrustManager
{
	public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException
	{
	}

	public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException
	{
	}

	public X509Certificate[] getAcceptedIssuers()
	{
		return null;
	}
}

public class notrustsslsocket extends SSLSocketFactory
{
	private SSLSocketFactory socketFactory;

	public notrustsslsocket()
	{
		try {
			SSLContext ctx = SSLContext.getInstance("TLS");
			ctx.init(null,new TrustManager[] { new DummyTrustmanager() }, new SecureRandom());
			socketFactory = ctx.getSocketFactory();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}

	public static SocketFactory getDefault()
	{
		return new notrustsslsocket();
	}

	@Override
	public String[] getDefaultCipherSuites()
	{
		return socketFactory.getDefaultCipherSuites();
	}

	@Override
	public String[] getSupportedCipherSuites()
	{
		return socketFactory.getSupportedCipherSuites();
	}

	@Override
	public Socket createSocket(Socket socket, String string, int i, boolean bln) throws IOException
	{
		return socketFactory.createSocket(socket, string, i, bln);
	}

	@Override
	public Socket createSocket(String string, int i) throws IOException, UnknownHostException
	{
		return socketFactory.createSocket(string, i);
	}

	@Override
	public Socket createSocket(String string, int i, InetAddress ia, int i1) throws IOException, UnknownHostException
	{
		return socketFactory.createSocket(string, i, ia, i1);
	}

	@Override
	public Socket createSocket(InetAddress ia, int i) throws IOException
	{
		return socketFactory.createSocket(ia, i);
	}

	@Override
	public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1) throws IOException
	{
		return socketFactory.createSocket(ia, i, ia1, i1);
	}
}

class nohostverifier implements HostnameVerifier
{
	public nohostverifier()
	{
	}

	@Override
	public boolean verify(String hostname,javax.net.ssl.SSLSession sslSession)
	{
		return true;
	}
}
