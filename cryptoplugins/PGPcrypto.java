import java.io.*;
import java.util.*;
import java.security.*;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.*;
import org.bouncycastle.openpgp.operator.jcajce.*;

// https://stackoverflow.com/questions/3939447/how-to-encrypt-a-string-stream-with-bouncycastle-pgp-without-starting-with-a-fil
// https://stackoverflow.com/questions/14993223/getting-gpg-decryption-to-work-in-java-bouncy-castle
// gpg --gen-key
// gpg --list-keys
// gpg --encrypt root.crt
// gpg --decrypt root.crt.gpg
// If gpg2: gpg --armor --export-secret-keys > secret-keys.gpg

class PGPcrypto extends CryptoBase
{
	JcaKeyFingerprintCalculator calc;
	PGPDigestCalculatorProvider calcProvider;

	public PGPcrypto(XML xml) throws PGPException
	{
		Security.addProvider(new BouncyCastleProvider());
		calc = new JcaKeyFingerprintCalculator();
		calcProvider = new JcaPGPDigestCalculatorProviderBuilder().setProvider(BouncyCastleProvider.PROVIDER_NAME).build();

		Misc.log("PGPcrypto");
	}

	@Override
	String encrypt(String message)
	{
		try {
			XML xml = getXML();
			String filename = "filename";

			String charset = xml.getValue("charset","ISO-8859-1");
			String keyFilename = xml.getValue("keystore_public_filename");

			ByteArrayOutputStream out = new ByteArrayOutputStream();

			if (keyFilename == null)
				throw new AdapterRuntimeException("No PGP keystore provided");
			InputStream keyIn = new FileInputStream(new File(javaadapter.getCurrentDir(),keyFilename));

			PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(PGPCompressedData.ZIP);
			ByteArrayOutputStream bOut = new ByteArrayOutputStream();
			PGPLiteralDataGenerator lData = new PGPLiteralDataGenerator();
			byte[] bytesOut = message.getBytes(charset);
			OutputStream pOut = lData.open(comData.open(bOut),PGPLiteralData.BINARY,filename,bytesOut.length,new Date());
			pOut.write(bytesOut);
			comData.close();

			PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(SymmetricKeyAlgorithmTags.AES_256).setSecureRandom(new SecureRandom()).setProvider(BouncyCastleProvider.PROVIDER_NAME));

			PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(keyIn),calc);

			PGPPublicKey encKey = null;
			Iterator<PGPPublicKeyRing> keyRingIter = pgpPub.getKeyRings();
			if (!keyRingIter.hasNext())
				throw new PGPException("PGP keystore file '" + keyFilename + "' doesn't not contain a key ring");

			while(keyRingIter.hasNext())
			{
				PGPPublicKeyRing keyRing = keyRingIter.next();
				Iterator<PGPPublicKey> keyIter = keyRing.getPublicKeys();
				while (keyIter.hasNext())
				{
					encKey = keyIter.next();
					if (encKey.isEncryptionKey()) break;
					if (Misc.isLog(30)) Misc.log("Public key users: " + Misc.implode(encKey.getUserIDs()));
				}
				if (encKey.isEncryptionKey()) break;
			}

			cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider(BouncyCastleProvider.PROVIDER_NAME));
			byte[] bytes = bOut.toByteArray();
			OutputStream cOut = cPk.open(new ArmoredOutputStream(out),bytes.length);
			cOut.write(bytes);
			cOut.close();
			out.close();
			return new String(out.toByteArray(),charset);
		} catch(AdapterException | IOException | PGPException ex) {
			throw new AdapterRuntimeException(ex);
		}
	}

	@Override
	String decrypt(String message)
	{
		try {
			XML xml = getXML();

			String charset = xml.getValue("charset","ISO-8859-1");
			String keyFilename = xml.getValue("keystore_secret_filename");
			String password = xml.getValueCrypt("keystore_secret_password");

			InputStream in = new ByteArrayInputStream(message.getBytes(charset));

			if (keyFilename == null)
				throw new AdapterRuntimeException("No PGP keystore provided");
			InputStream keyIn = new FileInputStream(new File(javaadapter.getCurrentDir(),keyFilename));

			PGPObjectFactory pgpF = new PGPObjectFactory(PGPUtil.getDecoderStream(in),calc);
			PGPEncryptedDataList enc = null;
			Object o = pgpF.nextObject();
			if (o instanceof PGPEncryptedDataList)
				enc = (PGPEncryptedDataList) o;
			else
				enc = (PGPEncryptedDataList) pgpF.nextObject();
			Iterator it = enc.getEncryptedDataObjects();

			PGPPrivateKey sKey = null;
			PGPPublicKeyEncryptedData pbe = null;
			PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn),calc);
			if (!pgpSec.getKeyRings().hasNext())
				throw new PGPException("PGP keystore file '" + keyFilename + "' doesn't not contain a key ring");

			while (sKey == null && it.hasNext())
			{
				pbe = (PGPPublicKeyEncryptedData)it.next();
				PGPSecretKey pgpSecKey = pgpSec.getSecretKey(pbe.getKeyID());
				if (pgpSecKey == null) continue;
				if (Misc.isLog(30)) Misc.log("Secret key users: " + Misc.implode(pgpSecKey.getUserIDs()));
				PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder(calcProvider).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(password.toCharArray());
				try {
					sKey = pgpSecKey.extractPrivateKey(decryptor);
				} catch(PGPException ex) {
					throw new PGPException("Cannot decrypt private key: " + ex.getMessage());
				}
			}

			if (sKey == null)
				throw new IllegalArgumentException("PGP secret key for decryption not found");

			PublicKeyDataDecryptorFactory decrypter = new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(BouncyCastleProvider.PROVIDER_NAME).build(sKey);
			InputStream clear = pbe.getDataStream(decrypter);
			PGPObjectFactory pgpFact = new PGPObjectFactory(clear,calc);
			PGPCompressedData cData = (PGPCompressedData)pgpFact.nextObject();
			pgpFact = new PGPObjectFactory(cData.getDataStream(),calc);
			PGPLiteralData ld = (PGPLiteralData)pgpFact.nextObject();
			InputStream unc = ld.getInputStream();

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int ch;
			while ((ch = unc.read()) >= 0)
				out.write(ch);

			String result = new String(out.toByteArray(),charset);
			out.close();
			return result;
		} catch(AdapterException | IOException | PGPException ex) {
			throw new AdapterRuntimeException(ex);
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith("-----BEGIN PGP MESSAGE-----");
	}

}
