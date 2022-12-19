import java.io.*;
import java.util.*;
import com.google.gson.*;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.win.WinHttpClients;
import org.apache.hc.core5.http.ParseException;

class SecretSRVcrypto extends CryptoBase
{
	CloseableHttpClient client;
	private CacheManager<String> cache = new CacheManager<>();
	private static final String ID = "SECRETSERVER";

	public SecretSRVcrypto(XML xml)
	{
		if (!WinHttpClients.isWinAuthAvailable())
			throw new AdapterRuntimeException("Windows authentication not available");
		client = WinHttpClients.createDefault();
	}

	@Override
	String decrypt(String message)
	{
		try {
			String url = getXML().getValue("url");
			String id = message.substring(2 + ID.length(),message.length() - 1);

			String cached_value = cache.get(id);
			if (cached_value != null) return cached_value;

			url = url.replaceAll("/+$", "") + "/api/v1" + "/secrets/" + id;
			if (Misc.isLog(15)) Misc.log("Secret Server URL: " + url);

			HttpGet request = new HttpGet(url);
			CloseableHttpResponse response = client.execute(request);

			if (response.getCode() != 200) // HTTP 200 OK?
				throw new AdapterRuntimeException("Cannot get secret id " + id + ": " + response.getReasonPhrase());
			String content = EntityUtils.toString(response.getEntity());

			response.close();

			JsonParser parser = new JsonParser();
			JsonElement secretElement = parser.parse(content);
			JsonObject secret = secretElement.getAsJsonObject();
			JsonArray secretItems = secret.getAsJsonArray("items");

			for (int i = 0; i < secretItems.size(); i++)
			{
				JsonObject item = secretItems.get(i).getAsJsonObject();
				String name = item.get("fieldName").getAsString().toLowerCase();
				if (!name.equals("password")) continue;
				String value = item.get("itemValue").getAsString();
				cache.set(id,value);
				return value;
			}

			throw new AdapterRuntimeException("No password found in secret id " + id);
		} catch(AdapterException | IOException | ParseException ex) {
			throw new AdapterRuntimeException(ex);
		}
	}

	@Override
	boolean detect(String message)
	{
		return message.startsWith("!" + ID + ":") && message.endsWith("!");
	};
}
