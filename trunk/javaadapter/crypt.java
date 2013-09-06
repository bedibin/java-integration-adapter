import java.io.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import javax.naming.Context;
import javax.naming.InitialContext;

class crypt
{
	private Cipher encryptCipher;
	private Cipher decryptCipher;

	public crypt() throws SecurityException
	{
		char[] str = "This is a test string for the HQ Java adapter!!".toCharArray();

		byte[] salt = {(byte)0xa3,(byte)0x21,(byte)0x24,(byte)0x2c,(byte)0xf2,(byte)0xd2,(byte)0x3e,(byte)0x19};

		int iterations = 3;

		init(str,salt,iterations);
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
		}
		catch (Exception e)
		{
			throw new SecurityException("Could not initialize crypt: " + e.toString());
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
		}
		catch (Exception e)
		{
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

			str = str.substring(1,str.length() - 1);
			byte[] dec = base64coder.decode(str);
			byte[] utf8 = decryptCipher.doFinal(dec);

			return new String(utf8, "UTF-8");
		}
		catch (Exception e)
		{
			throw new SecurityException("Could not decrypt: " + e.toString());
		}
	}
}
