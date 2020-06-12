import java.util.*;
import java.io.*;

class QueueDirPluginHook extends Hook
{
	class QueueDir
	{
		private ArrayList<Subscriber> sublist = new ArrayList<Subscriber>();
		private String dir;

		public QueueDir(XML xml) throws AdapterException
		{
			sublist = Misc.initSubscribers(xml);
			Misc.activateSubscribers(sublist);

			dir = xml.getValue("dir").trim();
		}

		public String getDir()
		{
			return dir;
		}

		public void run(XML xml) throws AdapterException
		{
			for(Subscriber sub:sublist)
			{
				XML response = sub.run(xml);
				if (response != null) break;
			}
		}
	}

	private ArrayList<QueueDir> queues = new ArrayList<QueueDir>();

	public QueueDirPluginHook() throws AdapterException
	{
		XML xmlcfg = javaadapter.getConfiguration();
		XML[] elements = xmlcfg.getElements("queuedir");

		for(XML element:elements)
		{
			queues.add(new QueueDir(element));
		}
	}

	@Override
	public void run() 
	{
		try
		{
			for(QueueDir queue:queues)
			{
				File dir = new File(javaadapter.getCurrentDir(),queue.getDir());
				File[] files = dir.listFiles();
				String[] filenames = new String[files.length];
				for(int j = 0;j < files.length;j++)
					filenames[j] = files[j].getPath();

				Arrays.sort(filenames);

				for(String filename:filenames)
				{
					Misc.log(2,"Processing queued XML file: " + filename);
					XML xml = null;
					try
					{
						xml = new XML(filename);
					}
					catch(Exception ex)
					{
						Misc.log(1,"Discarting invalid XML file " + filename);
					}
					if (xml != null) queue.run(xml);
					File file = new File(javaadapter.getCurrentDir(),filename);
					file.delete();
				}
			}
		}
		catch(Exception ex)
		{
			Misc.log(ex);
		}
	}

	public void close()
	{
	}
}

