import java.util.*;
import org.mozilla.javascript.*;

class AdapterScriptException extends AdapterException
{
        public AdapterScriptException(String message)
        {
                super(message);
        }
}

class Script
{
	static Context ctx = null;

	private static synchronized Context getContext()
	{
		if (ctx != null) return ctx;

		ctx = Context.enter();
		ctx.setOptimizationLevel(-1);
		ctx.setLanguageVersion(170);
		return ctx;
	}

	public static String execute(String program,Map<String,String> vars) throws AdapterScriptException
	{
		if (program == null) return null;
		Context ctx = getContext();
		ScriptableObject scope = ctx.initStandardObjects();
		for(Map.Entry<String,String> var:XML.getDefaultVariables().entrySet())
			scope.putProperty(scope,var.getKey(),var.getValue());
		if (vars != null) for(Map.Entry<String,String> var:vars.entrySet())
		{
			String key = var.getKey();
			if (key == null || key.isEmpty()) continue;
			scope.putProperty(scope,key.replace(':','_'),var.getValue());
		}

		if (Misc.isLog(30)) Misc.log("Executing script: " + program);
		try {
			Object obj = ctx.evaluateString(scope,program,"script",1,null);
			if (obj != null) return obj.toString();
		} catch (org.mozilla.javascript.JavaScriptException ex) {
			throw new AdapterScriptException(ex.getMessage());
		}
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
