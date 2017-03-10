import java.util.*;
import java.util.regex.*;
import java.io.*;
import com.esotericsoftware.wildcard.Paths;

class DBSyncOper
{
	enum OPER { add, update, remove, end, start};

	class RateCounter
	{
		int total = 0;
		int add = 0;
		int remove = 0;
		int update = 0;
	}

	private int maxqueuelength;
	private static final int DEFAULTMAXQUEUELENGTH = 100;

	private XML xmlsync;

	private String dbsyncname;
	private String rootname;
	private ArrayList<XML> xmloperlist;
	private RateCounter counter;
	private DB db;
	private Date lastdate;
	private Date startdate;
	private boolean tobreak = false;
	private int breakcount = 0;
	private boolean checkcolumn = true;
	private boolean directmode = false;
	private Sync sourcesync;
	private Sync destinationsync;
	private DatabaseUpdateSubscriber update;
	private LinkedHashSet<String> displayfields;
	private Set<String> comparefields;
	private HashSet<String> tracekeys;
	private ArrayList<String> ignorefields;
	private Fields fields;
	private boolean iscache = false;

	// Global flags
	enum doTypes { TRUE, FALSE, ERROR };
	private doTypes doadd;
	private doTypes doremove;
	private doTypes doupdate;
	private boolean ignorecasekeys;
	private boolean ignorecasefields;

	public DBSyncOper() throws Exception
	{
		db = DB.getInstance();
		update = new DatabaseUpdateSubscriber();
	}

	public String getName()
	{
		return dbsyncname;
	}

	public Fields getFields()
	{
		return fields;
	}

	public doTypes getDoRemove()
	{
		return doremove;
	}

	public boolean getIgnoreCaseKeys()
	{
		return ignorecasekeys;
	}

	public HashSet<String> getTraceKeys()
	{
		return tracekeys;
	}

	public Sync getSourceSync()
	{
		return sourcesync;
	}

	public Sync getDestinationSync()
	{
		return destinationsync;
	}

	public Date getLastDate()
	{
		return lastdate;
	}

	public Date getStartDate()
	{
		return startdate;
	}

	public boolean isCache()
	{
		return iscache;
	}

	public String getDisplayKey(Set<String> keys,Map<String,String> map)
	{
		LinkedHashSet<String> set = new LinkedHashSet<String>();
		if (displayfields != null)
		{
			ArrayList<String> displayvalues = Misc.getKeyValueList(displayfields,map);
			if (displayvalues != null) set.addAll(displayvalues);
		}

		ArrayList<String> keyvalues = Misc.getKeyValueList(keys,map);
		if (keyvalues != null) set.addAll(keyvalues);

		return set.size() == 0 ? null : Misc.implode(set,"/");
	}

	public boolean isValidSync(String[] synclist,Sync sync) throws Exception
	{
		if (sync == null) return false;
		if (synclist == null) return true;
		if (Misc.isLog(30)) Misc.log("Field: Doing validation against " + Misc.implode(synclist));

		String name = sync.getName();
		if (name == null) return false;
		if (Misc.indexOf(synclist,name) != -1) return true;

		String sourcename = sourcesync == null ? null : sourcesync.getName();
		String destname = destinationsync == null ? null : destinationsync.getName();

		if (sourcename != null && destname != null)
			return Misc.indexOf(synclist,sourcename + "-" + destname) != -1;

		if (Misc.isLog(30)) Misc.log("Field: Validation not found in: " + name + "," + sourcename + "," + destname);
		return false;
	}

	private void flush() throws Exception
	{
		if (dbsyncplugin.preview_mode || directmode || destinationsync == null)
		{
			xmloperlist.clear();
			return;
		}

		breakcount++;
		// Uncomment for debugging: if (breakcount >= 10) tobreak = true;

		XML xml = new XML();

		XML xmlop = xml.add(rootname);
		xmlop.setAttribute("name",dbsyncname);
		if (displayfields != null)
			xmlop.setAttribute("display_keyfield",Misc.implode(displayfields));

		String[] attrs = {"instance","table","type","on_duplicates","merge_fields"};
		for(String attr:attrs)
			xmlop.setAttribute(attr,destinationsync.getXML().getAttribute(attr));

		if ((counter.add + counter.update + counter.remove) > 0)
		{
			for(XML xmloper:xmloperlist)
				xmlop.add(xmloper);

			Publisher publisher = Publisher.getInstance();
			publisher.publish(xml,xmlsync);
		}

		xmloperlist.clear();
	}

	private void push(OPER oper) throws Exception
	{
		if (destinationsync == null) return;

		XML xml = new XML();
		XML node = xml.add(oper.toString());
		if (oper == OPER.end)
		{
			node.setAttribute("total","" + counter.total);
			node.setAttribute("add","" + counter.add);
			node.setAttribute("remove","" + counter.remove);
			node.setAttribute("update","" + counter.update);
		}

		if (directmode)
		{
			if (!dbsyncplugin.preview_mode && destinationsync != null) update.oper(destinationsync.getXML(),xml);
			return;
		}

		xmloperlist.add(xml);
	}

	private void push(OPER oper,LinkedHashMap<String,String> row,LinkedHashMap<String,String> rowold) throws Exception
	{
		if (destinationsync == null) return;

		XML sourcexml = sourcesync.getXML();
		XML destinationxml = destinationsync.getXML();

		XML xml = new XML();
		XML xmlop = xml.add(oper.toString());

		Set<String> destinationheader = destinationsync.getHeader();
		Set<String> allfields = comparefields;
		if (allfields == null) allfields = row.keySet(); // If destination null, use source fields

		for(String key:allfields)
		{
			String sourcevalue = row.get(key);

			String newvalue = sourcevalue;
			if (newvalue == null) newvalue = "";

			XML xmlrow;
			if (newvalue.indexOf("\n") == -1)
				xmlrow  = xmlop.add(key,newvalue);
			else
			{
				newvalue = Misc.trimLines(newvalue);

				// This option is deprecated, use on_multiple on fields instead
				String ondupstr = sourcexml.getAttribute("on_duplicates");

				String[] duplist = null;
				String dupfields = sourcexml.getAttribute("on_duplicates_fields");
				if (dupfields != null) duplist = dupfields.split("\\s*,\\s*");

				if (duplist != null && Misc.indexOf(duplist,key) == -1);
				else if (ondupstr == null || ondupstr.equals("merge"));
				else if (ondupstr.equals("error"))
				{
					Misc.log("ERROR: [" + getDisplayKey(fields.getKeys(),row) + "] Rejecting record with a duplicated key on field " + key);
					return;
				}
				else if (ondupstr.equals("ignore"))
					return;
				else
					throw new AdapterException(sourcexml,"Invalid on_duplicates attribute");
				xmlrow  = xmlop.addCDATA(key,newvalue);
			}

			if (Misc.isLog(30)) Misc.log("Is info check " + sourcevalue + ":" + key + ":" + destinationheader);
			boolean isinfo = destinationheader == null ? false : (sourcevalue == null || !destinationheader.contains(key));
			if (isinfo) xmlrow.setAttribute("type","info");

			boolean ignorecase = false;

			for(Field field:fields.getFields())
			{
				String fieldname = field.getName();
				if (key.equals(fieldname) && (field.isValid(sourcesync) || field.isValid(destinationsync)))
				{
					if (Misc.isLog(30)) Misc.log("Matched field " + fieldname + " with key " + key);
					String type = field.getType();
					if (type != null)
					{
						if (type.equals("key") && rowold != null && "".equals(newvalue))
						{
							String oldvalue = rowold.get(key);
							if (oldvalue != null)
								xmlrow.setValue(oldvalue);
						}
						if (type.equals("infonull") && "".equals(newvalue))
							xmlrow.setAttribute("type","info");
						else if (isinfo && type.equals("noinfo"))
							xmlrow.removeAttribute("type");
						else if (!isinfo || !type.equals("infoapi"))
							xmlrow.setAttribute("type",type);
					}
					ignorecase = field.isIgnoreCase();
				}
			}

			if (rowold != null)
			{
				String oldvalue = rowold.get(key);
				if (oldvalue == null) oldvalue = "";
				boolean oldmulti = oldvalue.indexOf("\n") != -1;
				if (oldmulti) oldvalue = Misc.trimLines(oldvalue);

				boolean issame = ignorecasefields || ignorecase ? oldvalue.equalsIgnoreCase(newvalue) : oldvalue.equals(newvalue);
				if (!issame && (!isinfo || oldvalue.length() > 0))
				{
					if (oldmulti)
						xmlrow.addCDATA("oldvalue",oldvalue);
					else
						xmlrow.add("oldvalue",oldvalue);
				}
			}
		}

		ArrayList<String> changes = new ArrayList<String>();
		HashMap<String,String> newvalues = new HashMap<String,String>();

		for(XML entry:xml.getElements())
		{
			String tag = entry.getTagName();
			String value = entry.getValue();
			if (value == null) value = "";
			String type = entry.getAttribute("type");
			if (oper == OPER.update && "key".equals(type))
				;
			else if ("info".equals(type))
				;
			else if (oper == OPER.update)
			{
				if (ignorefields != null && ignorefields.contains(tag)) continue;
				XML old = entry.getElement("oldvalue");
				if (old != null)
				{
					String oldvalue = old.getValue();
					boolean initial = "initial".equals(type);
					if (!initial || ("initial".equals(type) && oldvalue == null))
					{
						if (oldvalue == null) oldvalue = "";
						changes.add(tag + "[" + oldvalue + "->" + value + "]");
						newvalues.put(tag,value);
					}
				}
			}
			else
			{
				changes.add(tag + ":" + value);
				if (oper == OPER.add) newvalues.put(tag,value);
			}
			
		}

		String prevkeys = getDisplayKey(fields.getKeys(),row);
		if (prevkeys == null)
		{
			Misc.log("ERROR: Discarting record with null keys: " + row);
			return;
		}

		if (oper == OPER.update && changes.isEmpty())
		{
			if (Misc.isLog(25)) Misc.log("Discarting update because nothing to do: " + xml);
			return;
		}
		if (Misc.isLog(2)) Misc.log(oper + ": " + prevkeys + " " + Misc.implode(changes,", "));
		fields.updateCache(newvalues);

		if (oper == OPER.update) counter.update++;
		else if (oper == OPER.add) counter.add++;
		else if (oper == OPER.remove) counter.remove++;
		xmlop.setAttribute("position","" + (counter.add + counter.remove + counter.update));

		if (directmode)
		{
			if (!dbsyncplugin.preview_mode && destinationsync != null) update.oper(destinationxml,xml);
			return;
		}

		xmloperlist.add(xml);

		if (xmloperlist.size() >= maxqueuelength)
			flush();
	}

	private void remove(LinkedHashMap<String,String> row) throws Exception
	{
		if (doremove == doTypes.ERROR) Misc.log("ERROR: [" + getDisplayKey(fields.getKeys(),row) + "] removing entry rejected: " + row);
		if (doremove != doTypes.TRUE) return;
		if (Misc.isLog(4)) Misc.log("quick_remove: " + row);
		push(OPER.remove,row,null);
	}

	private void add(LinkedHashMap<String,String> row) throws Exception
	{
		if (doadd == doTypes.ERROR) Misc.log("ERROR: [" + getDisplayKey(fields.getKeys(),row) + "] adding entry rejected: " + row);
		if (doadd != doTypes.TRUE) return;
		if (destinationsync == null) return;
		if (Misc.isLog(4)) Misc.log("quick_add: " + row);
		push(OPER.add,row,null);
	}

	private void update(LinkedHashMap<String,String> rowold,LinkedHashMap<String,String> rownew) throws Exception
	{
		if (doupdate == doTypes.ERROR) Misc.log("ERROR: [" + getDisplayKey(fields.getKeys(),rownew) + "] updating entry rejected: " + rownew);
		if (doupdate != doTypes.TRUE) return;
		if (Misc.isLog(4))
		{
			String delta = null;
			for(String key:rownew.keySet())
			{
				String newvalue = rownew.get(key);
				if (newvalue == null) newvalue = "";
				String oldvalue = rowold.get(key);
				if (oldvalue == null) oldvalue = "";
				if (!oldvalue.equals(newvalue))
				{
					if (delta != null) delta += ", ";
					else delta = "";
					delta += key + "[" + oldvalue + "->" + newvalue + "]";
				}
			}

			Misc.log("quick_update: " + getDisplayKey(fields.getKeys(),rownew) + " " + delta);
		}
		push(OPER.update,rownew,rowold);
	}

	public String getKey(LinkedHashMap<String,String> row)
	{
		if (row == null) return "";
		StringBuilder key = new StringBuilder();

		for(String keyfield:fields.getKeys())
		{
			/* Use exclamation mark since it is the lowest ASCII character */
			/* This code must match db.getOrderBy logic */
			String keyvalue = row.get(keyfield);
			if (keyvalue != null) key.append(keyvalue.trim().replace(' ','!').replace('_','!').replace('\t','!'));
			key.append("!");
		}

		return key.toString();
	}

	private Sync getSync(XML xml,XML xmlextra,XML xmlsource) throws Exception
	{
		try {
			return getSyncSub(xml,xmlextra,xmlsource);
		} catch(FileNotFoundException ex) {
			xml.setAttributeDeprecated("on_not_found","on_file_not_found");
			OnOper onnotfound = Field.getOnOper(xml,"on_file_not_found",OnOper.exception,EnumSet.of(OnOper.exception,OnOper.ignore,OnOper.warning,OnOper.error));
			switch(onnotfound)
			{
			case exception:
				Misc.rethrow(ex);
			case warning:
				Misc.log("WARNING: Ignoring sync operation since file not found: " + ex.getMessage());
				break;
			case error:
				Misc.log("ERROR: Ignoring sync operation since file not found: " + ex.getMessage());
				break;
			}
		}
		return null;
	}

	private Sync getSyncSub(XML xml,XML xmlextra,XML xmlsource) throws Exception
	{
		String type = xml.getAttribute("type");
		if (type == null) type = "db";

		if (xmlextra != null)
		{
			String destfilter = xmlextra.getValue("remotefilter",null);
			if (destfilter == null)
				xml.removeAttribute("filter");
			else
				xml.setAttribute("filter",destfilter);
		}

		if (type.equals("db"))
			return new SyncSql(this,xml);
		else if (type.equals("csv"))
			return new SyncCsv(this,xml);
		/* SOAP data source is now obsolete and is replaced by XML data source */
		else if (type.equals("soap"))
			return new SyncSoap(this,xml);
		else if (type.equals("xml"))
			return new SyncXML(this,xml,xmlsource);
		else if (type.equals("class"))
			return new SyncClass(this,xml);
		else if (type.equals("null"))
			return new Sync(this,xml);

		throw new AdapterException(xml,"Invalid sync type " + type);
	}

	private void compare() throws Exception
	{
		final String traceid = "COMPARE";
		xmloperlist = new ArrayList<XML>();
		counter = new RateCounter();

		if (Misc.isLog(2))
		{
			String sourcename = sourcesync.getDescription();
			String keyinfo = " (" + Misc.implode(fields.getKeys()) + ")";
			if (destinationsync == null)
				Misc.log("Reading source \"" + sourcename + "\"" + keyinfo + "...");
			else
			{
				String destinationname = destinationsync.getDescription();
				Misc.log("Comparing source \"" + sourcename + "\" with destination \"" + destinationname + "\"" + keyinfo + "...");
			}
		}

		push(OPER.start);

		LinkedHashMap<String,String> row = fields.getNext(sourcesync);
		LinkedHashMap<String,String> rowdest = (destinationsync == null) ? null : fields.getNext(destinationsync);

		comparefields = (destinationsync == null) ? null : destinationsync.getHeader();
		if (comparefields != null && ignorefields != null) comparefields.removeAll(ignorefields);

		/* keycheck is obsolete and should no longer be used */
		String keycheck = sourcesync.getXML().getAttribute("keycheck");
		boolean ischeck = !(keycheck != null && keycheck.equals("false"));

		if (checkcolumn && row != null && rowdest != null && ischeck)
		{
			String error = null;
			Set<String> keylist = fields.getKeys();

			if (!row.keySet().containsAll(keylist))
				error = "Source table must contain all keys: " + Misc.implode(keylist) + ": " + Misc.implode(row);

			if (!rowdest.keySet().containsAll(keylist))
				error = "Destination table must contain all keys: " + Misc.implode(keylist) + ": " + Misc.implode(rowdest);;

			if (error != null)
			{
				if (Misc.isLog(5)) Misc.log("Keys: " + keylist);
				if (Misc.isLog(5)) Misc.log("Source columns: " + row);
				if (Misc.isLog(5)) Misc.log("Destination columns: " + rowdest);
				throw new AdapterException("Synchronization " + dbsyncname + " cannot be done. " + error);
			}
		}

		String destkey = getKey(rowdest);
		String sourcekey = getKey(row);

		while(row != null || rowdest != null)
		{
			if (javaadapter.isShuttingDown()) return;
			if (tobreak) break;
			counter.total++;
			Misc.clearTrace(traceid);

			if ((row != null && tracekeys.contains(sourcekey)) || (rowdest != null && tracekeys.contains(destkey)))
			{
				Misc.setTrace(traceid);
				if (row != null) Misc.log("Source: " + Misc.implode(row));
				if (rowdest != null) Misc.log("Destination: " + Misc.implode(rowdest));
			}

			if (Misc.isLog(11)) Misc.log("Key source: " + sourcekey + " dest: " + destkey);

			if (rowdest != null && (row == null || (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) > 0))
			{
				remove(rowdest);

				rowdest = fields.getNext(destinationsync);
				destkey = getKey(rowdest);

				continue;
			}

			if (row != null && (rowdest == null || (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) < 0))
			{
				add(row);

				row = fields.getNext(sourcesync);
				sourcekey = getKey(row);

				continue;
			}

			if (row != null && (ignorecasekeys ? db.collator.compareIgnoreCase(sourcekey,destkey) : db.collator.compare(sourcekey,destkey)) == 0)
			{
				for(String key:comparefields)
				{
					String value = row.get(key);
					if (value == null) continue;
					String destvalue = rowdest.get(key);
					if (destvalue == null) destvalue = "";
					if (!value.equals(destvalue))
					{
						update(rowdest,row);
						break;
					}
				}

				row = fields.getNext(sourcesync);
				sourcekey = getKey(row);

				rowdest = fields.getNext(destinationsync);
				destkey = getKey(rowdest);

				continue;
			}
		}

		push(OPER.end);
		flush();
	}

	private void exec(XML xml,String oper) throws Exception
	{
		XML[] execlist = xml.getElements(oper);
		for(XML element:execlist)
		{
			String command = element.getValue();

			String type = element.getAttribute("type");
			if (type != null && type.equals("db"))
			{
				db.execsql(element.getAttribute("instance"),command);
				continue;
			}
			if (type != null && type.equals("xml"))
			{
				XML[] funclist = element.getElements("function");
				for(XML funcel:funclist)
				{
					Subscriber sub = new Subscriber(funcel);
					sub.run(new XML());
				}
				continue;
			}

			String charset = element.getAttribute("charset");
			Process process = Misc.exec(command,charset);
			int exitval = process.waitFor();
			if (exitval != 0)
				throw new AdapterException(element,"Command cannot be executed properly, result code is " + exitval);
		}
	}

	public void run() throws Exception
	{
		run(null,null);
	}

	public void run(XML xmlfunction) throws Exception
	{
		run(xmlfunction,null);
	}

	private doTypes getOperationFlag(XML xml,String attr) throws Exception
	{
		String dostr = xml.getAttribute(attr);
		if (dostr == null) return null;

		if (dostr.equals("true"))
			return doTypes.TRUE;
		else if (dostr.equals("false"))
			return doTypes.FALSE;
		else if (dostr.equals("error"))
			return doTypes.ERROR;
		else
			throw new AdapterException(xml,"Invalid " + attr + " attribute");
	}

	private Boolean getBooleanFlag(XML xml,String attr) throws Exception
	{
		String dostr = xml.getAttribute(attr);
		if (dostr == null) return null;

		if (dostr.equals("true"))
			return new Boolean(true);
		else if (dostr.equals("false"))
			return new Boolean(false);
		else
			throw new AdapterException(xml,"Invalid " + attr + " attribute");
	}
	private void setOperationFlags(XML xml) throws Exception
	{
		doTypes doresult = getOperationFlag(xml,"do_add");
		if (doresult != null) doadd = doresult;
		doresult = getOperationFlag(xml,"do_remove");
		if (doresult != null) doremove = doresult;
		doresult = getOperationFlag(xml,"do_update");
		if (doresult != null) doupdate = doresult;

		String casestr = xml.getAttribute("ignore_case");
		if (casestr != null)
		{
			if (casestr.equals("true"))
			{
				ignorecasekeys = true;
				ignorecasefields = true;
			}
			else if (casestr.equals("false"))
			{
				ignorecasekeys = false;
				ignorecasefields = false;
			}
			else if (casestr.equals("keys_only"))
			{
				ignorecasekeys = true;
				ignorecasefields = false;
			}
			else if (casestr.equals("non_keys_only"))
			{
				ignorecasekeys = false;
				ignorecasefields = true;
			}
			else
				throw new AdapterException(xml,"Invalid ignore_case attribute");
		}
	}

	private ArrayList<XML> getFilenamePatterns(XML sync) throws Exception
	{
		ArrayList<XML> results = new ArrayList<XML>();

		String filename = sync.getAttribute("filename");
		if (filename == null)
			results.add(sync);
		else
		{
			String fileescape = filename.replaceAll("\\.","\\.").replaceAll("\\*","\\*");
			Matcher matcherglob = Misc.substitutepattern.matcher(fileescape);
			String fileglob = matcherglob.replaceAll("*");
			if (Misc.isLog(10)) Misc.log("File glob: " + fileglob);

			fileescape = filename.replaceAll("[\\\\/]","[\\\\\\\\/]").replaceAll("\\.","\\.").replaceAll("\\*","\\.\\*");
			Matcher matchervar = Misc.substitutepattern.matcher(fileescape);
			String fileextract = matchervar.replaceAll("(.*)");
			if (Misc.isLog(10)) Misc.log("File extract: " + fileextract);
			Pattern patternextract = Pattern.compile(fileextract);

			Paths paths = new Paths(".",fileglob);
			String[] files = paths.getRelativePaths();
			if (files.length == 0) results.add(sync);

			for(String file:files)
			{
				XML newsync = sync.copy();

				if (Misc.isLog(10)) Misc.log("Filename: " + file);
				newsync.setAttribute("filename",file);

				Matcher matcherextract = patternextract.matcher(file);
				matchervar.reset();
				matchervar.find();
				int y = 0;
				while(matcherextract.find())
				{
					int count = matcherextract.groupCount();
					for(int x = 0;x < count;x++)
					{
						y++;
						if (y > matchervar.groupCount())
						{
							matchervar.find();
							y = 1;
						}
						if (Misc.isLog(10)) Misc.log("Variable from file name: " + matchervar.group(y) + "=" + matcherextract.group(x + 1));
						XML varxml = newsync.add("variable");
						varxml.setAttribute("name",matchervar.group(y));
						varxml.setAttribute("value",matcherextract.group(x + 1));
					}
				}

				results.add(newsync);
			}
		}

		return results;
	}

	public void run(XML xmlfunction,XML xmlsource) throws Exception
	{
		XML xmlcfg = javaadapter.getConfiguration();

		startdate = new Date();

		XML[] elements = xmlcfg.getElements("dbsync");
		for(int i = 0;i < elements.length;i++)
		{
			if (javaadapter.isShuttingDown()) return;

			xmlsync = elements[i];
			if (!Misc.isFilterPass(xmlfunction,xmlsync)) continue;

			XML[] publishers = xmlsync.getElements("publisher");
			directmode = (publishers.length == 0);

			exec(xmlsync,"preexec");

			dbsyncname = xmlsync.getAttribute("name");
			Misc.log(1,"Syncing " + dbsyncname + "...");

			rootname = xmlsync.getAttribute("root");
			if (rootname == null) rootname = "ISMDatabaseUpdate";

			XML jms = xmlcfg.getElement("jms");
			maxqueuelength = jms == null ? 1 : DEFAULTMAXQUEUELENGTH; // Only JMS requires buffering
			String maxqueue = xmlsync.getAttribute("maxqueuelength");
			if (maxqueue != null) maxqueuelength = new Integer(maxqueue);

			/* checkcolumns is obsolete and should no longer be used */
			checkcolumn = true;
			String checkstr = xmlsync.getAttribute("checkcolumns");
			if (checkstr != null && checkstr.equals("false"))
				checkcolumn = false;

			String keyfield = xmlsync.getAttribute("keyfield");
			if (keyfield == null) keyfield = xmlsync.getAttribute("keyfields");
			String displaykeyfield = xmlsync.getAttribute("display_keyfield");
			displayfields = displaykeyfield == null ? null : new LinkedHashSet<String>(Arrays.asList(displaykeyfield.split("\\s*,\\s*")));

			String cache = xmlsync.getAttribute("preload_cache");
			iscache = cache != null && cache.equals("true");

			tracekeys = new HashSet<String>();
			XML[] traces = xmlsync.getElements("trace");
			for(XML trace:traces)
			{
				String tracekey = trace.getAttribute("key");
				if (tracekey != null) tracekeys.add(tracekey);
			}

			XML[] destinations = xmlsync.getElements("destination");

			ArrayList<Field> globalfields = new ArrayList<Field>();
			XML[] globalfieldsxml = xmlsync.getElements("field");
			if (iscache)
				for(XML globalfieldxml:globalfieldsxml)
					globalfields.add(new Field(globalfieldxml,Scope.SCOPE_GLOBAL));

			for(XML rawsource:xmlsync.getElements("source"))
			{
				ArrayList<Field> sourcefields = new ArrayList<Field>();
				XML[] sourcefieldsxml = rawsource.getElements("field");
				if (iscache)
					for(XML sourcefieldxml:sourcefieldsxml)
						sourcefields.add(new Field(sourcefieldxml,Scope.SCOPE_SOURCE));

				for(XML source:getFilenamePatterns(rawsource))
				{
					if (source == null) continue;

					String filename_source = source.getAttribute("filename");
					if (filename_source != null)
						Misc.log(2,"Reading filename \"" + filename_source + "\"...");

					String keyfield_source = source.getAttribute("keyfield");
					if (keyfield_source == null) keyfield_source = source.getAttribute("keyfields");
					if (keyfield_source != null) keyfield = keyfield_source;
					if (keyfield == null) throw new AdapterException(xmlsync,"keyfield is mandatory");

					String ignoreattr = source.getAttribute("ignore_fields");
					ignorefields = ignoreattr == null ? null : new ArrayList<String>(Arrays.asList(ignoreattr.split("\\s*,\\s*")));

					int k = 0;
					for(;k < destinations.length;k++)
					{
						if (destinations[k] == null) continue;

						sourcesync = new Sync(this,source); // Set to a dummy sync since SortTable may call getName before sourcesync is initialised
						destinationsync = new Sync(this,destinations[k]);

						fields = new Fields(this,keyfield.split("\\s*,\\s*"));
						XML[] varsxml = source.getElements("variable");
						for(XML var:varsxml) fields.addDefaultVar(var);
						if (iscache)
						{
							for(Field globalfield:globalfields) fields.add(globalfield);
							for(Field sourcefield:sourcefields) fields.add(sourcefield);
						} else {
							for(XML globalfieldxml:globalfieldsxml) fields.add(new Field(globalfieldxml,Scope.SCOPE_GLOBAL));
							for(XML sourcefieldxml:sourcefieldsxml) fields.add(new Field(sourcefieldxml,Scope.SCOPE_SOURCE));
						}
						XML[] fieldsxml = destinations[k].getElements("field");
						for(XML field:fieldsxml) fields.add(new Field(field,Scope.SCOPE_DESTINATION));

						doadd = doupdate = doremove = doTypes.TRUE;
						ignorecasekeys = ignorecasefields = false;
						setOperationFlags(xmlsync);
						setOperationFlags(source);
						setOperationFlags(destinations[k]);

						sourcesync = getSync(source,destinations[k],xmlsource);
						if (sourcesync == null) continue;
						destinationsync = getSync(destinations[k],source,xmlsource);
						if (destinationsync == null) continue;

						compare();

						sourcesync = destinationsync = null;
						for(XML var:varsxml) fields.removeDefaultVar(var);
					}

					if (k == 0)
					{
						sourcesync = new Sync(this,source);

						fields = new Fields(this,keyfield.split("\\s*,\\s*"));
						XML[] varsxml = source.getElements("variable");
						for(XML var:varsxml) fields.addDefaultVar(var);
						if (iscache)
						{
							for(Field globalfield:globalfields) fields.add(globalfield);
							for(Field sourcefield:sourcefields) fields.add(sourcefield);
						} else {
							for(XML globalfieldxml:globalfieldsxml) fields.add(new Field(globalfieldxml,Scope.SCOPE_GLOBAL));
							for(XML sourcefieldxml:sourcefieldsxml) fields.add(new Field(sourcefieldxml,Scope.SCOPE_SOURCE));
						}

						sourcesync = getSync(source,null,xmlsource);
						if (sourcesync == null) continue;
						destinationsync = null;
						try
						{
							compare();
						}
						catch(java.net.SocketTimeoutException ex)
						{
							// Don't stop processing if a timeout occurs
							Misc.log(ex);
						}

						sourcesync = null;
						for(XML var:varsxml) fields.removeDefaultVar(var);
					}
				}
			}

			exec(xmlsync,"postexec");

			lastdate = startdate;

			Misc.log(1,"Syncing " + dbsyncname + " done");
		}
	}

	public void close()
	{
		db.close();
	}
}
