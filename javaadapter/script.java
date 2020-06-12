import java.util.*;
import javax.script.*;

// Depend on Mozilla Rhino library from https://developer.mozilla.org/en-US/docs/Mozilla/Projects/Rhino
import org.mozilla.javascript.*;

class AdapterScriptException extends AdapterException
{
	private static final long serialVersionUID = -6121204203128410513L;

        public AdapterScriptException(String message)
        {
                super(message);
        }
}

class Script
{
	static Context ctx = null;
	static ScriptEngine engine = null;

	private static synchronized ScriptEngine getEngine(String name)
	{
		if (engine != null) return engine;
		
		ScriptEngineManager sem = new ScriptEngineManager();
		ScriptEngine engine = sem.getEngineByName(name);
		engine.put(ScriptEngine.FILENAME,"script");
		ScriptEngineFactory f = engine.getFactory();

		return engine;
	}

	private static synchronized Context getRhinoContext()
	{
		if (ctx != null) return ctx;

		ctx = Context.enter();
		ctx.setOptimizationLevel(-1);
		ctx.setLanguageVersion(170);
		return ctx;
	}

	public static String executeEngine(String engine,String program,Map<String,?> vars) throws AdapterScriptException
	{
		ScriptEngine e = getEngine(engine);
		Bindings bind = e.createBindings();
		bind.putAll(XML.getDefaultVariables());
		if (vars != null) for(Map.Entry<String,?> var:vars.entrySet())
		{
			String key = var.getKey();
			if (key == null || key.isEmpty()) continue;
			bind.put(key,var.getValue());
		}

		if (Misc.isLog(30)) Misc.log("Executing script: " + program);
		try {
			Object obj = e.eval(program,bind);
			if (obj instanceof Undefined) return null;
			if (obj != null) return obj.toString();
			obj = bind.get("result");
			if (obj instanceof Undefined) return null;
			if (obj != null) return obj.toString();
		} catch(ScriptException ex) {
			throw new AdapterScriptException(ex.getMessage());
		}
		return null;
	}

	public static String executeRhino(String program,Map<String,?> vars) throws AdapterScriptException
	{
		Context ctx = getRhinoContext();
		ScriptableObject scope = ctx.initStandardObjects();
		for(Map.Entry<String,?> var:XML.getDefaultVariables().entrySet())
			scope.putProperty(scope,var.getKey(),var.getValue());
		if (vars != null) for(Map.Entry<String,?> var:vars.entrySet())
		{
			String key = var.getKey();
			if (key == null || key.isEmpty()) continue;
			// Note: Use this["varname"] if variable name contains invalid characters like spaces or quotes
			scope.putProperty(scope,key,var.getValue());
		}

		if (Misc.isLog(30)) Misc.log("Executing Rhino script: " + program);
		try {
			Object obj = ctx.evaluateString(scope,program,"script",1,null);
			if (obj instanceof Undefined) return null;
			if (obj != null) return obj.toString();
		} catch (org.mozilla.javascript.JavaScriptException ex) {
			throw new AdapterScriptException(ex.getMessage());
		}

		return null;
	}

	public static String execute(XML xml,Map<String,?> vars) throws AdapterException
	{
		if (xml == null) return null;
		String program = xml.getValue();

		XML xmlcfg = javaadapter.getConfiguration();
		if (xmlcfg != null)
		{
			StringBuilder sb = new StringBuilder(program);
			XML[] xmlscripts = xmlcfg.getElements("script");
			for(XML xmlscript:xmlscripts)
				sb.append(xmlscript.getValue());
			program = sb.toString() + program;
		}

		String engine = xml.getAttribute("engine");
		if (engine != null) return executeEngine(engine,program,vars);
		return executeRhino(program,vars);
	}
}

public class script
{
	public static void main(String [] args) throws Exception
	{
		HashMap<String,Object> vars = new HashMap<String,Object>();
		vars.put("test",new Integer(5));

		String program = 
			"var a = 2;\n" +
			"var b = 3;\n" +
			"var c = a + b + test;\n" + 
			"java.lang.System.out.println(\"a + b = \" + c);\n" + 
			"c;";
		System.out.println("Result: " + Script.executeRhino(program,vars));
		System.out.println("Result: " + Script.executeEngine("ECMAScript",program,vars));

		program =
			"a = 2\n" +
			"b = 3\n" +
			"c = a + b + test\n" + 
			"print(\"a + b = \" + str(c))\n" + 
			"result = c";
		System.out.println("Result: " + Script.executeEngine("python",program,vars));
	}
}
