import java.util.*;
import java.io.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.InitialContext;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.security.GeneralSecurityException;

class Crypto extends AdapterExtend
{
	protected static Crypto instance;

	public static synchronized Crypto getInstance()
	{
		if (instance == null) instance = new Crypto();
		return instance;
	}

	static CryptoBase getCryptoInstance(String name) throws AdapterException
	{
		return (CryptoBase)getInstance().getInstance(name);
	}
}

class CryptoBase extends AdapterExtendBase
{
	boolean detect(String message) { return false; };
	String encrypt(String message) { throw new AdapterRuntimeException("Encryption not supported"); }
	String decrypt(String bytes) { throw new AdapterRuntimeException("Decryption not supported"); }
}

class crypt
{
	public crypt()
	{
		Crypto crypto = Crypto.getInstance();
		crypto.setInstance(new StrongCrypto());
		crypto.setInstance(new DefaultCrypto());
		crypto.setInstance(new FileCrypto());
		crypto.setInstance(new EnvCrypto());
	}

	public crypt(XML xmlconfig) throws AdapterException
	{
		this();
		XML[] xmlciphers = xmlconfig.getElements("cipher");
		for(XML xml:xmlciphers)
			Crypto.getInstance().setInstance(xml);
	}

	String decrypt(String message)
	{
		for(Map.Entry<String,AdapterExtendBase> entry:Crypto.getInstance().getInstances().entrySet())
		{
			CryptoBase base = (CryptoBase)entry.getValue();
			if (base.detect(message))
				return base.decrypt(message);
		}
		return message;
	}
}

class StrongCrypto extends CryptoBase
{
	private String p = "$%gEHRT/HRyery3%Wrswt$Y/$RH35yerhEY%5&nf$";
	private static final String CHARSET = "UTF-8";
	private static final String KEYALGOSHA = "HmacSHA256";

	public byte[] deriveKey(String p,byte[] s,int i,int l) throws GeneralSecurityException
	{
		PBEKeySpec ks = new PBEKeySpec(p.toCharArray(),s,i,l);
		SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
		return skf.generateSecret(ks).getEncoded();
	}

	@Override
	public synchronized String encrypt(String str)
	{
		try
		{
			SecureRandom r = SecureRandom.getInstance("SHA1PRNG");
			byte[] esalt = new byte[20]; r.nextBytes(esalt);
			byte[] dek = deriveKey(p, esalt,100000,128);

			SecretKeySpec eks = new SecretKeySpec(dek, "AES");
			Cipher encryptCipherStrong = Cipher.getInstance("AES/CTR/NoPadding");
			encryptCipherStrong.init(Cipher.ENCRYPT_MODE,eks,new IvParameterSpec(new byte[16]));
			byte[] utf8 = str.getBytes(CHARSET);
			byte[] es = encryptCipherStrong.doFinal(utf8);

			byte[] hsalt = new byte[20]; r.nextBytes(hsalt);
			byte[] dhk = deriveKey(p, hsalt,100000,160);

			SecretKeySpec hks = new SecretKeySpec(dhk,KEYALGOSHA);
			Mac m = Mac.getInstance(KEYALGOSHA);
			m.init(hks);
			byte[] hmac = m.doFinal(es);

			byte[] os = new byte[40 + es.length + 32];
			System.arraycopy(esalt,0,os,0,20);
			System.arraycopy(hsalt,0,os,20,20);
			System.arraycopy(es,0,os,40,es.length);
			System.arraycopy(hmac,0,os,40 + es.length,32);

			String encstr = new String(base64coder.encode(os));
			return "!!" + encstr + "!";
		}
		catch (GeneralSecurityException | UnsupportedEncodingException e)
		{
			throw new AdapterRuntimeException("Could not encrypt: " + e.toString());
		}

	}

	@Override
	public synchronized String decrypt(String str)
	{
		try
		{
			str = str.substring(2,str.length() - 1);
			byte[] os = base64coder.decode(str.toCharArray());
			if (os.length <= 72) throw new AdapterRuntimeException("Could not decrypt: Key too short");

			byte[] esalt = Arrays.copyOfRange(os,0,20);
			byte[] hsalt = Arrays.copyOfRange(os,20,40);
			byte[] es = Arrays.copyOfRange(os,40,os.length - 32);
			byte[] hmac = Arrays.copyOfRange(os,os.length - 32,os.length);
			byte[] dhk = deriveKey(p,hsalt,100000,160);

			SecretKeySpec hks = new SecretKeySpec(dhk,KEYALGOSHA);
			Mac m = Mac.getInstance(KEYALGOSHA);
			m.init(hks);
			byte[] chmac = m.doFinal(es);

			if (!MessageDigest.isEqual(hmac,chmac)) throw new AdapterRuntimeException("Could not decrypt: Key is invalid");

			byte[] dek = deriveKey(p,esalt,100000,128);

			SecretKeySpec eks = new SecretKeySpec(dek,"AES");
			Cipher decryptCipherString = Cipher.getInstance("AES/CTR/NoPadding");
			decryptCipherString.init(Cipher.DECRYPT_MODE,eks,new IvParameterSpec(new byte[16]));
			byte[] s = decryptCipherString.doFinal(es);

			return new String(s,CHARSET);
		} catch (IOException | GeneralSecurityException e) {
			throw new AdapterRuntimeException("Could not decrypt: " + e.toString());
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith("!!") && message.endsWith("!") && base64coder.isbase64(message.substring(2,message.length()-1));
	}

}

class DefaultCrypto extends CryptoBase
{
	private Cipher encryptCipher;
	private Cipher decryptCipher;
	private static final String CHARSET = "UTF-8";

	public DefaultCrypto()
	{
		char[] str = "This is a test string for the HQ Java adapter!!".toCharArray();

		byte[] salt = {(byte)0xa3,(byte)0x21,(byte)0x24,(byte)0x2c,(byte)0xf2,(byte)0xd2,(byte)0x3e,(byte)0x19};

		init(str,salt);
	}

	public void init(char[] pass,byte[] salt)
	{
		try
		{
			PBEParameterSpec ps = new javax.crypto.spec.PBEParameterSpec(salt,20);
			SecretKeyFactory kf = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
			SecretKey k = kf.generateSecret(new javax.crypto.spec.PBEKeySpec(pass));

			encryptCipher = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
			encryptCipher.init(Cipher.ENCRYPT_MODE,k,ps);
			decryptCipher = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");

			decryptCipher.init(Cipher.DECRYPT_MODE,k,ps);
		} catch (GeneralSecurityException e) {
			throw new AdapterRuntimeException("Could not encrypt: " + e.toString());
		}
	}

	@Override
	public synchronized String encrypt(String str)
	{
		try
		{
			byte[] utf8 = str.getBytes(CHARSET);
			byte[] enc = encryptCipher.doFinal(utf8);

			String encstr = new String(base64coder.encode(enc));
			return "!" + encstr + "!";
		} catch (GeneralSecurityException | UnsupportedEncodingException e) {
			throw new AdapterRuntimeException("Could not encrypt: " + e.toString());
		}
	}

	@Override
	public synchronized String decrypt(String str)
	{
		try
		{
			str = str.substring(1,str.length() - 1);
			byte[] dec = base64coder.decode(str);
			byte[] utf8 = decryptCipher.doFinal(dec);

			return new String(utf8,CHARSET);
		} catch (IOException | GeneralSecurityException e) {
			throw new AdapterRuntimeException("Could not decrypt: " + e.toString());
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith("!") && message.endsWith("!") && base64coder.isbase64(message.substring(1,message.length()-1));
	}
}

class FileCrypto extends CryptoBase
{
	@Override
	public synchronized String decrypt(String str)
	{
		try {
			String file = str.substring(1,str.length() - 1);
			str = Misc.readFile(file);
			if (str == null)
				throw new AdapterException("File not found: " + file);
			BufferedReader reader = new BufferedReader(new StringReader(str));
			str = reader.readLine();
			return Misc.getCrypter().decrypt(str);
		} catch (AdapterException | IOException e) {
			throw new AdapterRuntimeException("Could not decrypt: " + e.toString());
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith(":") && message.endsWith(":");
	}
}

class EnvCrypto extends CryptoBase
{
	@Override
	public synchronized String decrypt(String str)
	{
		try {
			str = str.substring(1,str.length() - 1);
			Context ctx = new InitialContext();
			Context env = (Context)ctx.lookup("java:comp/env");
			str = (String)env.lookup(str);
			return Misc.getCrypter().decrypt(str);
		} catch (NamingException e) {
			throw new AdapterRuntimeException("Could not decrypt: " + e.toString());
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith(";") && message.endsWith(";");
	}
}
