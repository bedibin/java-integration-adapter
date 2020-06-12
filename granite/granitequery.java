import java.util.*;
import java.io.*;

class QueryChangeHook extends Hook
{
	class FileInfo
	{
		public String mintime;
		public String maxtime;
		public String lastid;
		public boolean result;
	}

	public static final String INFOFILENAME = "granitequery.info";
	public static final String LASTFILENAME = "granitequery.last";
	public static final int TIMEDELAY = 10;
	public static final String SQLDATEFORMAT = "'YYYY-MM-DD HH24:MI:SS'";

	private DB db;
	private XML xmlcfg;

	public QueryChangeHook() throws Exception
	{
		xmlcfg = new XML("granitequery.xml");
		db = new DB(xmlcfg);

		try
		{
			Publisher publisher = Publisher.getInstance();
			XML xml = new XML(LASTFILENAME);
			publisher.publish(xml,xmlcfg);
		}
		catch(Exception ex)
		{
		}
	}

	@Override
	public void run() 
	{
		try
		{
			query(xmlcfg,null,0,null,null);
		}
		catch(Exception ex)
		{
			Misc.log(ex);
		}
	}

	private FileInfo getFileInfo(XML request) throws Exception
	{
		FileInfo fileinfo = new FileInfo();
		fileinfo.result = false;

		XML position = request.getElement("position");
		if (position == null)
			return fileinfo;

		String requestname = request.getAttribute("name");

		java.util.Date currentdate = new java.util.Date();
		long maxtimemillisec = currentdate.getTime() - (TIMEDELAY * 1000);
		fileinfo.maxtime = Misc.dateformat.format(maxtimemillisec);

		XML xmlinfo = null;
		try
		{
			xmlinfo = new XML(INFOFILENAME);
		}
		catch(Exception ex)
		{
			xmlinfo = new XML();
			xmlinfo = xmlinfo.add("granitequeryinfo");
		}

		XML xmlname = xmlinfo.getElement(requestname);
		if (xmlname == null)
		{
			xmlname = xmlinfo.add(requestname);
			String currenttime = Misc.dateformat.format(currentdate.getTime());
			xmlname.add("lasttime",currenttime);

			XML xmlid = position.getElement("idfield");
			String idfield = xmlid.getValue().trim();
			XML sql = xmlid.getElement("sql");

			ArrayList<LinkedHashMap<String,String>> result = null;
			if (sql != null && idfield != null)
				result = db.execsql(sql.getAttribute("name"),sql.getValue());

			if (result != null && result.size() > 0)
			{
				fileinfo.lastid = result.get(0).get(idfield);
				xmlname.add("lastid",fileinfo.lastid);
			}

			xmlinfo.write(INFOFILENAME);

			return fileinfo;
		}

		fileinfo.mintime = xmlname.getValue("lasttime");
		fileinfo.lastid = xmlname.getValue("lastid",null);

		long mintimemillisec = Misc.dateformat.parse(fileinfo.mintime).getTime();
		long currenttimemillisec = currentdate.getTime();

		if (currenttimemillisec - (TIMEDELAY * 2000) < mintimemillisec)
			return fileinfo;

		fileinfo.result = true;

		return fileinfo;
	}

	private void putFileInfo(XML request,FileInfo fileinfo) throws Exception
	{
		if (fileinfo == null) return;

		String requestname = request.getAttribute("name");

		XML xmlinfo;

		try
		{
			xmlinfo = new XML(INFOFILENAME);
		}
		catch(Exception ex)
		{
			return;
		}

		XML xmlname = xmlinfo.getElement(requestname);
		if (xmlname == null) return;

		xmlname.setValue("lasttime",fileinfo.maxtime);
		xmlname.setValue("lastid",fileinfo.lastid);

		xmlinfo.write(INFOFILENAME);
	}

	private XML query(XML xmlquery,String idvalue,int level,XML xmlcontent,XML parentxml) throws Exception
	{
		XML[] xmllist = xmlquery.getElements("request");
		for(int i = 0;i < xmllist.length;i++)
		{
			XML request = xmllist[i];

			if (parentxml != null && !Misc.isFilterPass(request,parentxml)) continue;

			if (Misc.isLog(8)) Misc.log("Level " + level + " iter " + i);

			XML sql = request.getElement("sql");
			if (sql == null) continue;

			XML position = request.getElement("position");
			if (position == null) continue;

			String updatefield = position.getValue("updatefield",null);

			XML xmlid = position.getElement("idfield");
			String idfield = null;
			if (xmlid != null) idfield = xmlid.getValue().trim();

			String prevlastid = null;

			XML xmlparentid = position.getElement("parentidfield");
			String parentidfield = null;
			if (xmlparentid != null) parentidfield = xmlparentid.getValue().trim();

			String sqlstr = null;
			FileInfo fileinfo = null;

			if (level > 0)
			{
				if (xmlcontent == null) continue;

				if (idvalue != null && parentidfield != null)
					sqlstr = "select * from (" + sql.getValue() + ") where " + parentidfield + " = '" + idvalue + "'";
			}
			else
			{
				fileinfo = getFileInfo(request);
				if (!fileinfo.result) continue;

				prevlastid = fileinfo.lastid;

				if (updatefield != null)
					sqlstr = "select * from (" + sql.getValue() + ") where " + updatefield + " is not null and " + updatefield + " >= { ts '" + fileinfo.mintime + "'} and " + updatefield + " < { ts '" + fileinfo.maxtime + "'}";
				else if (idfield != null)
					sqlstr = "select * from (" + sql.getValue() + ") where " + idfield + " > '" + fileinfo.lastid + "'";
			}

			if (sqlstr == null) continue;

			ArrayList<LinkedHashMap<String,String>> result = db.execsql(sql.getAttribute("name"),sqlstr);

/*
			result = new ArrayList<HashMap<String,String>>();
			HashMap<String,String> r = new HashMap<String,String>();
			r.put("ID","942236");
			r.put("NEW","03");
			r.put("INST_ID","29345");
			r.put("SUBTYPE","r");
			r.put("FIELD","[3 - 0] Parametre / Loop");
			r.put("OLD","");
			r.put("TYPE","Q");
			r.put("CHG_TS","2012-05-02 11:19:25");
			result.add(r);
*/

			if (result != null) for(LinkedHashMap<String,String> row:result)
			{
				if (xmlcontent == null)
					xmlcontent = new XML();

				String requestname = null;
				String namefield = position.getValue("namefield",null);

				if (namefield != null && row.containsKey(namefield))
					requestname = row.get(namefield);
				if (requestname == null)
					requestname = request.getAttribute("name");
				if (requestname == null && row.containsKey("REQUESTNAME"))
					requestname = row.get("REQUESTNAME");

				XML newxmlcontent = new XML();
				XML record = newxmlcontent.add("record");

				record.setAttribute("name",requestname);

				String lastid = null;

				if (idfield != null)
				{
					lastid = row.get(idfield);
					if (prevlastid != null)
					{
						record.add("lastid",prevlastid);
						record.add("id",lastid);
					}
				}

				Iterator<String> itr = row.keySet().iterator();
				while(itr.hasNext())
				{
					String key = itr.next();
					String value = row.get(key);
					if (value == null || value.length() == 0) continue;
					XML el = record.add("field",value);
					el.setAttribute("name",key);
				}

				if (Misc.isLog(10)) Misc.log("Content is: " + xmlcontent);
				if (Misc.isLog(10)) Misc.log("Record is: " + record);
				xmlcontent = xmlcontent.add(record);

				xmlcontent = query(request,lastid,level+1,xmlcontent,newxmlcontent);

				if (level == 0)
				{
					if (!Misc.isFilterPass(request,xmlcontent))
					{
						xmlcontent = null;
						putFileInfo(request,fileinfo);
						continue;
					}

					xmlcontent.setRoot();
					if (Misc.isLog(9)) Misc.log("Query before transform: " + xmlcontent.toString());
					xmlcontent.transform(request);

					xmlcontent.write(LASTFILENAME);

					Publisher publisher = Publisher.getInstance();
					if (Misc.isLog(5)) Misc.log("Query publish: " + xmlcontent.toString());
					publisher.publish(xmlcontent,xmlcfg);

					xmlcontent = null;
					putFileInfo(request,fileinfo);
				}
			}
			else if (level == 0)
				putFileInfo(request,fileinfo);
		}

		return xmlcontent;
	}

	public void close()
	{
		db.close();
	}
}

class RTSSubscriber extends Subscriber
{
	@Override
	public XML run(XML xml) throws AdapterException
	{
		File file = new File(javaadapter.getCurrentDir(),QueryChangeHook.LASTFILENAME);
		file.delete();
		return null;
	}
}
