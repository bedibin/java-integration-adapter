import java.io.*;
import java.util.*;
import net.sourceforge.jdpapi.*;

class DPAPIcrypto extends CryptoBase
{
	private final DataProtector protector;

	static
	{
		System.loadLibrary("jdpapi-native.dll");
	}

	public DPAPIcrypto(XML xml)
	{
		protector = new DataProtector();
		Misc.log("DPAPIcrypto");
	}

	@Override
	String encrypt(String message)
	{
		return new String(base64coder.encode(protector.protect(message)));
	}

	@Override
	String decrypt(String message)
	{
		return protector.unprotect(base64coder.decode(message));
	}
}
