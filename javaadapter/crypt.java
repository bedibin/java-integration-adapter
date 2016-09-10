import java.io.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.util.Arrays;

class crypt
{
	private Cipher encryptCipher;
	private Cipher decryptCipher;
	private String p = "$%gEHRT/HRyery3%Wrswt$Y/$RH35yerhEY%5&nf$";

	public crypt() throws SecurityException
	{
		char[] str = "This is a test string for the HQ Java adapter!!".toCharArray();

		byte[] salt = {(byte)0xa3,(byte)0x21,(byte)0x24,(byte)0x2c,(byte)0xf2,(byte)0xd2,(byte)0x3e,(byte)0x19};

		int iterations = 3;

		init(str,salt,iterations);
	}

	public byte[] deriveKey(String p,byte[] s,int i,int l) throws Exception
	{
		PBEKeySpec ks = new PBEKeySpec(p.toCharArray(),s,i,l);
		SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
		return skf.generateSecret(ks).getEncoded();
	}

	public void init(char[] pass,byte[] salt,int iterations) throws SecurityException
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
		} catch (Exception e) {
			throw new SecurityException("Could not encrypt: " + e.toString());
		}
	}

	public synchronized String encryptStrong(String str) throws SecurityException
	{
		try
		{
			SecureRandom r = SecureRandom.getInstance("SHA1PRNG");
			byte[] esalt = new byte[20]; r.nextBytes(esalt);
			byte[] dek = deriveKey(p, esalt,100000,128);

			SecretKeySpec eks = new SecretKeySpec(dek, "AES");
			Cipher encryptCipher = Cipher.getInstance("AES/CTR/NoPadding");
			encryptCipher.init(Cipher.ENCRYPT_MODE,eks,new IvParameterSpec(new byte[16]));
			byte[] utf8 = str.getBytes("UTF-8");
			byte[] es = encryptCipher.doFinal(utf8);

			byte[] hsalt = new byte[20]; r.nextBytes(hsalt);
			byte[] dhk = deriveKey(p, hsalt,100000,160);

			SecretKeySpec hks = new SecretKeySpec(dhk,"HmacSHA256");
			Mac m = Mac.getInstance("HmacSHA256");
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
		catch (Exception e)
		{
			throw new SecurityException("Could not encrypt: " + e.toString());
		}

	}

	public synchronized String encrypt(String str) throws SecurityException
	{
		try
		{
			byte[] utf8 = str.getBytes("UTF-8");
			byte[] enc = encryptCipher.doFinal(utf8);
			String encstr = new String(base64coder.encode(enc));
			return "!" + encstr + "!";
		} catch (Exception e) {
			throw new SecurityException("Could not encrypt: " + e.toString());
		}
	}

	public synchronized String decrypt(String str) throws SecurityException
	{
		try
		{
			if (str.startsWith(":") && str.endsWith(":"))
			{
				String file = str.substring(1,str.length() - 1);
				str = Misc.readFile(file);
				if (str == null)
					throw new AdapterException("File not found: " + file);
				BufferedReader reader = new BufferedReader(new StringReader(str));
				str = reader.readLine();
			}
			else if (str.startsWith(";") && str.endsWith(";"))
			{
				str = str.substring(1,str.length() - 1);
				Context ctx = new InitialContext();
				Context env = (Context)ctx.lookup("java:comp/env");
				str = (String)env.lookup(str);
			}

			if (!(str.startsWith("!") && str.endsWith("!")))
				return str;

			if (str.startsWith("!!"))
			{
				str = str.substring(2,str.length() - 1);
				byte[] os = base64coder.decode(str.toCharArray());
				if (os.length <= 72) throw new SecurityException("Could not decrypt: Key too short");

				byte[] esalt = Arrays.copyOfRange(os,0,20);
				byte[] hsalt = Arrays.copyOfRange(os,20,40);
				byte[] es = Arrays.copyOfRange(os,40,os.length - 32);
				byte[] hmac = Arrays.copyOfRange(os,os.length - 32,os.length);
				byte[] dhk = deriveKey(p,hsalt,100000,160);

				SecretKeySpec hks = new SecretKeySpec(dhk,"HmacSHA256");
				Mac m = Mac.getInstance("HmacSHA256");
				m.init(hks);
				byte[] chmac = m.doFinal(es);

				if (!MessageDigest.isEqual(hmac,chmac)) throw new SecurityException("Could not decrypt: Key is invalid");

				byte[] dek = deriveKey(p,esalt,100000,128);

				SecretKeySpec eks = new SecretKeySpec(dek,"AES");
				Cipher decryptCipher = Cipher.getInstance("AES/CTR/NoPadding");
				decryptCipher.init(Cipher.DECRYPT_MODE,eks,new IvParameterSpec(new byte[16]));
				byte[] s = decryptCipher.doFinal(es);

				return new String(s,"UTF-8");
			}

			str = str.substring(1,str.length() - 1);
			byte[] dec = base64coder.decode(str);
			byte[] utf8 = decryptCipher.doFinal(dec);

			return new String(utf8, "UTF-8");
		} catch (Exception e) {
			throw new SecurityException("Could not decrypt: " + e.toString());
		}
	}
}
