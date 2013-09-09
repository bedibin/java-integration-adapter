import java.util.*;
import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;

class directory
{
	class Search
	{
		private NamingEnumeration<SearchResult> results;
		private String basedn;
		private String search;
		private String[] attrs;
		private int count;

		Search(String basedn,String search,String[] attrs) throws Exception
		{
			results = null;
			this.basedn = basedn;
			this.search = search.trim();
			this.attrs = attrs;
			doSearch();
		}

		private void doSearch() throws Exception
		{
			SearchControls sc = new SearchControls();
			sc.setReturningAttributes(attrs);
			sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
			sc.setCountLimit(0);

			int limit = 0;
			String str = System.getProperty("javaadapter.ldap.timeout");
			if (str != null) limit = new Integer(str);
			sc.setTimeLimit(limit);

			if (Misc.isLog(15)) Misc.log("Searching " + search + " on basedn " + basedn);

			results = ctx.search(basedn == null ? "" : basedn,search,sc);
			if (results == null)
				Misc.log("WARNING: " + dirname + " search result is null");

			count = 0;
		}

		public Hashtable<String,String> next() throws Exception
		{
			if (results == null || !results.hasMore())
			{
				boolean continueSearch = setPagedNext(count);
				if (continueSearch) doSearch();
				if (results == null || !results.hasMore())
				{
					if (Misc.isLog(19)) Misc.log("Last " + dirname + " result");
					return null;
				}
			}

			count++;
			Hashtable<String,String> row = new Hashtable<String,String>();

			SearchResult entry = results.next();
			row.put("dn",entry.getNameInNamespace());

			Attributes set = entry.getAttributes();
			NamingEnumeration<String> ids = set.getIDs();
			while(ids != null && ids.hasMore())
			{
				String id = ids.next();
				Attribute attr = set.get(id);
				id = fixAttributeName(id);

				String value = row.get(id);
				if (value == null) value = "";

				for(int i = 0;i < attr.size();i++)
				{
					if (!"".equals(value))
						value += "\n";
					value += attr.get(i).toString();
				}

				row.put(id,value);
			}

			return row;
		}
	}

	private Search search_info = null;
	protected DirContext ctx;
	private String dirname = "DIRECTORY";

	protected directory(String name)
	{
		dirname = name;
	}

	public directory(String url,String context,String username,String password) throws Exception
	{
		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY,context);
		env.put(Context.PROVIDER_URL,url);
		if (username != null) env.put(Context.SECURITY_PRINCIPAL,username);
                if (password != null) env.put(Context.SECURITY_CREDENTIALS,password);
		ctx = new InitialDirContext(env);
	}

	protected String getRelativeName(String name) throws Exception
	{
		return name;
	}

	private XML read(String dn,XML sourcexml) throws Exception
	{
		// DN must be complete but relative to base DN
		XML xml = new XML();
		XML xmlroot = xml.add("result");

		String fieldstr = sourcexml.getAttribute("fields");
		String[] fields = fieldstr == null ? null :  fieldstr.split("\\s*,\\s*");

		String info = sourcexml.getAttribute("info");
		if (info != null) xml.setAttribute("info",info);

		String ns = ctx.getNameInNamespace();
		xmlroot.add("dn",dn + (ns.length() > 0 ? "," + ns : ""));

		Attributes set = null;
		try
		{
			set = ctx.getAttributes(dn,fields);
		}
		catch(Exception ex)
		{
			return null;
		}

		NamingEnumeration<String> ids = set.getIDs();
		while(ids != null && ids.hasMore())
		{
			String id = ids.next();
			Attribute attr = set.get(id);
			id = fixAttributeName(id);

			String value = "";
			for(int i = 0;i < attr.size();i++)
			{
				if (value.length() > 0)
					value += "\n";
				value += attr.get(i).toString();
			}

			xmlroot.add(id,value);
		}

		return xml;
	}

	protected String fixAttributeName(String name)
	{
		return name;
	}

	private void modAdd(ArrayList<ModificationItem> mods,int oper,XML xml) throws Exception
	{
		XML[] xmllist = xml.getElements(null);
		for(XML el:xmllist)
		{
			String name = el.getTagName();
			String value = el.getValue();

			BasicAttribute attr = new BasicAttribute(name);
			if (value != null)
			{
				attr = new BasicAttribute(name);
				String[] values = value.split("\\s*,\\s*");
				for(String val:values)
					attr.add(val);
			}

			ModificationItem mod = new ModificationItem(oper,attr);
			if (Misc.isLog(19)) Misc.log(dirname + " operation[" + oper + "] " + name + ": " + value);
			mods.add(mod);
		}
	}

	public XML oper(XML xml,XML node) throws Exception
	{
		String root = xml.getTagName();
		if (root == null)
		{
			Misc.log("ERROR: Invalid " + dirname + " operation on empty XML");
			return null;
		}

		String basedn = xml.getAttribute("basedn");
		if (basedn == null && node != null)
			basedn = node.getAttribute("basedn");

		if (root.equals("search"))
		{
			String fields = xml.getAttribute("fields");
			String[] attrs = fields == null ? null :  fields.split("\\s*,\\s*");

			Search search = new Search(basedn,xml.getValue(),attrs);
			XML result = new XML();
			XML rootresult = result.add("result");

			String info = xml.getAttribute("info");
			if (info != null) result.setAttribute("info",info);

			Hashtable<String,String> row;
			while((row = search.next()) != null)
				rootresult.add("row",row);

			return result;
		}

		String dn = xml.getAttribute("dn");
		if (dn == null)
		{
			Misc.log("ERROR: " + dirname + " dn attribute not provided: " + xml);
			return null;
		}

		dn = getRelativeName(dn);
		if (Misc.isLog(20)) Misc.log("LDAP operation on DN " + dn + " base " + basedn);
 
		if (root.equals("add"))
		{
			if (basedn != null && !Misc.endsWithIgnoreCase(dn,basedn))
				dn = dn + "," + basedn;

			if (Misc.isLog(15)) Misc.log(dirname + " operation " + root + " on DN " + dn);

			BasicAttributes attrs = new BasicAttributes();
			XML[] xmllist = xml.getElements(null);
			for(XML el:xmllist)
			{
				String name = el.getTagName();
				String value = el.getValue();
				if (value == null) value = "";
				if (Misc.isLog(19)) Misc.log(dirname + " attribute " + name + ": " + value);

				String[] values = value.split("\\s*,\\s*");
				BasicAttribute attr = new BasicAttribute(name);
				for(String val:values)
					attr.add(val);
				attrs.put(attr);
			}
			ctx.bind(dn,null,attrs);

			return read(dn,xml);
		}

		XML result = read(dn,xml);
		if (result == null)
		{
			SearchControls sc = new SearchControls();
			String[] params = {"dn"};
			sc.setReturningAttributes(params);
			sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
			sc.setCountLimit(1);

			NamingEnumeration<SearchResult> results = ctx.search(basedn == null ? "" : basedn,dn,sc);
                	if (results == null || !results.hasMore())
			{
				Misc.log("WARNING: " + root + " operation cannot be done since " + dn + " doesn't exist");
				return null;
			}
			SearchResult entry = (SearchResult)results.next();

			dn = entry.getName();
			if (basedn != null && !Misc.endsWithIgnoreCase(dn,basedn))
				dn = dn + "," + basedn;

			result = read(dn,xml);
			if (result == null)
				Misc.log("WARNING: DN " + dn + " read failed after search");
		}

		if (root.equals("query"))
			return result;

		if (root.equals("delete"))
			ctx.unbind(dn);
		else if (root.equals("update"))
		{
			ArrayList<ModificationItem> mods = new ArrayList<ModificationItem>();

			XML[] xmllist = xml.getElements(null);
			for(XML xmloper:xmllist)
			{
				String name = xmloper.getTagName();
				if (Misc.isLog(15)) Misc.log(dirname + " tag: " + name);

				if (name.equals("add"))
					modAdd(mods,DirContext.ADD_ATTRIBUTE,xmloper);
				else if (name.equals("update"))
					modAdd(mods,DirContext.REPLACE_ATTRIBUTE,xmloper);
				else if (name.equals("delete"))
					modAdd(mods,DirContext.REMOVE_ATTRIBUTE,xmloper);
			}

			int size = mods.size();
			if (size > 0)
			{
				ModificationItem[] modsarr = new ModificationItem[size];
				ctx.modifyAttributes(dn,mods.toArray(modsarr));
			}
			else
				Misc.log("WARNING: Null " + dirname + " update operation: " + xml);
		}
		else
		{
			Misc.log("ERROR: Invalid " + dirname + " operation: " + xml);
			return null;
		}

		return read(dn,xml);
	}

	public void search(String basedn,String search,String[] attrs) throws Exception
	{
		search_info = new Search(basedn,search,attrs);
	}

	protected boolean setPagedNext(int count) throws Exception
	{
		return false;
	}

	public Hashtable<String,String> searchNext() throws Exception
	{
		return search_info.next();
	}

	public void disconnect() throws Exception
	{
		if (ctx == null) return;
		ctx.close();
		ctx = null;
	}
}

class ldap extends directory
{
	private int pageSize = 100;
	private LdapContext ld;

	public ldap(String url,String username,String password,String[] sortattrs) throws Exception
	{
		super("LDAP");

		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL,url);
		if (username != null) env.put(Context.SECURITY_PRINCIPAL,username);
    		if (password != null) env.put(Context.SECURITY_CREDENTIALS,password);
		env.put("java.naming.ldap.derefAliases","never");
		env.put(Context.REFERRAL,"ignore");
		ld = new InitialLdapContext(env,null);

		String str = System.getProperty("javaadapter.ldap.pagesize");
		if (str != null) pageSize = new Integer(str);

		Control[] controls = new Control[sortattrs == null ? 1 : 2];
		controls[0] = new PagedResultsControl(pageSize,Control.NONCRITICAL);
		if (sortattrs != null)
		{
			if (Misc.isLog(19)) Misc.log("Enabling LDAP sort of fields " + Misc.implode(sortattrs));
			controls[1] = new SortControl(sortattrs,Control.CRITICAL);
		}
		ld.setRequestControls(controls);

		ctx = ld;
	}

	@Override
	protected String getRelativeName(String name) throws Exception
	{
		LdapName ln;
		try
		{
			ln = new LdapName(name);
		}
		catch(InvalidNameException ex)
		{
			return name;
		}

		String ns = ld.getNameInNamespace();
		if (ns.length() == 0) return name;

		LdapName nsn = new LdapName(ns);

		if (ln.startsWith(nsn))
		{
			// We have a full DN
			String newname = ln.getSuffix(ln.size() - nsn.size()).toString();
			return newname;
		}
		return ln.toString();
	}

	@Override
	protected boolean setPagedNext(int count) throws Exception
	{
		byte[] cookie = null;
		Control[] controls = ld.getResponseControls();
		if (controls == null) return false;

		for (Control control:controls)
		{
			if (control instanceof PagedResultsResponseControl)
			{
				PagedResultsResponseControl prrc = (PagedResultsResponseControl)control;
				int total = prrc.getResultSize();
				if (Misc.isLog(19)) Misc.log("LDAP control: Paged total: " + total);
				cookie = prrc.getCookie();
			}
			else if (control instanceof SortResponseControl)
			{
				SortResponseControl src = (SortResponseControl)control;
				if (Misc.isLog(19)) Misc.log("LDAP control: Is sorted: " + src.isSorted());
			}
		}

		controls[0] = new PagedResultsControl(pageSize,cookie,Control.NONCRITICAL);
		ld.setRequestControls(controls);

		if (cookie == null) return false;

		if (Misc.isLog(19)) Misc.log("Count is " + count + " and page size is " + pageSize);
		if (count > pageSize)
		{
			Misc.log("WARNING: LDAP server is returning more results than expected. Stopping extraction");
			return false;
		}

		return true;
	}

	@Override
	protected String fixAttributeName(String name)
	{
		int pos = name.indexOf(";range=");
		if (pos == -1) return name;
		return name.substring(0,pos);
	}

}
