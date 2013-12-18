/* IFDEF JAVA6 */
import java.util.*;
import javax.script.*;

class Script
{
	static ScriptEngine engine = null;

	private static synchronized ScriptEngine getEngine()
	{
		if (engine != null) return engine;
		
		ScriptEngineManager sem = new ScriptEngineManager();
		ScriptEngine engine = sem.getEngineByName("ECMAScript");
		engine.put(ScriptEngine.FILENAME,"script");
		ScriptEngineFactory f = engine.getFactory();

		return engine;
	}

	public static String execute(String program,Map<String,String> vars) throws Exception
	{
		if (program == null) return null;
		ScriptEngine e = getEngine();
		Bindings bind = e.createBindings();
		bind.putAll(XML.getDefaultVariables());
		for(Map.Entry<String,String> var:vars.entrySet())
		{
			String key = var.getKey();
			if (key == null || key.isEmpty()) continue;
			bind.put(key.replace(':','_'),var.getValue());
		}

		if (Misc.isLog(30)) Misc.log("Executing script: " + program);
		Object obj = e.eval(program,bind);
		if (obj != null) return obj.toString();
		return null;
	}
}

public class script
{
	public static void main(String [] args) throws Exception
	{
		String program = 
			"var a = 2;\n" +
			"var b = 3;\n" +
			"var c = a + b;\n" + 
			"print(\"a + b = \" + c + \"\\n\");" + 
			"c;";
		System.out.println("Result: " + Script.execute(program,null));
	}
}
/* */
