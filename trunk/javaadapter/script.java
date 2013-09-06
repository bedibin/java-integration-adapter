import java.util.*;
/* IFDEF JAVA6 */
import javax.script.*;
/* */

class Script
{
	public static String execute(String program,Map<String,String> vars) throws Exception
	{
		if (program == null) return null;
/* IFDEF JAVA6 */
		ScriptEngineManager sem = new ScriptEngineManager();
		ScriptEngine e = sem.getEngineByName("ECMAScript");
		ScriptEngineFactory f = e.getFactory();
		Bindings bind = e.createBindings();
		bind.putAll(XML.getDefaultVariables());
		for(Map.Entry<String,String> var:vars.entrySet())
			bind.put(var.getKey().replace(':','_'),var.getValue());

		if (Misc.isLog(30)) Misc.log("Executing script: " + program);
		Object obj = e.eval(program,bind);
		if (obj != null) return obj.toString();
/* */
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
