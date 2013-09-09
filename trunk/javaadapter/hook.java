import java.util.*;
import org.tiling.scheduling.*;

class HookIterator implements ScheduleIterator
{
	private String interval;
	private String name = "";

	public HookIterator(String interval)
	{
		this.interval = interval == null ? null : interval.trim();
	}

	public HookIterator(String interval,String name)
	{
		this.interval = interval == null ? null : interval.trim();
		if (name != null) this.name = name;
	}

	public Date next()
	{
		Date currenttime = new Date();
		if (interval == null || interval.length() == 0)
		{
			interval = (new Integer(Integer.MAX_VALUE)).toString();
			return currenttime;
		}

		String[] pos = interval.split(":");
		if (pos.length < 1 || pos.length > 3) return null;

		if (pos.length == 1)
			return new Date(currenttime.getTime() + ((long)(new Integer(interval)) * 1000));

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currenttime);
		calendar.set(Calendar.MILLISECOND,0);
		if (pos.length == 2)
		{
			calendar.set(Calendar.MINUTE,new Integer(pos[0]));
			calendar.set(Calendar.SECOND,new Integer(pos[1]));
			if (calendar.getTime().before(currenttime))
				calendar.add(Calendar.HOUR_OF_DAY,1);
		}
		else
		{
			calendar.set(Calendar.HOUR_OF_DAY,new Integer(pos[0]));
			calendar.set(Calendar.MINUTE,new Integer(pos[1]));
			calendar.set(Calendar.SECOND,new Integer(pos[2]));
			if (calendar.getTime().before(currenttime))
				calendar.add(Calendar.DATE,1);
		}

		if (Misc.isLog(5)) Misc.log("Next schedule for " + name + " is at " + Misc.dateformat.format(calendar.getTime()));
		return calendar.getTime();
	}
}

class Hook extends Operation
{
	private String interval;

	protected Hook() {}

	public Hook(String classname,XML function) throws Exception
	{
		super(classname,function);
	}

	public Hook(XML function) throws Exception
	{
		super(function);
	}

	@Override
	public void run()
	{
		try
		{
			XML xml = new XML();
			runFunction(xml,function);
		}
		catch(Exception ex)
		{
			Misc.log(ex);
		}
	}

	protected void setOperation(Hook sub) throws Exception
	{
		interval = sub.interval;
		super.setOperation(sub);
	}

	public void start()
	{
		Scheduler scheduler = new Scheduler();
		HookIterator iterator = new HookIterator(interval,getClassName());
		scheduler.schedule(this,iterator);
	}

	public void setInterval(String interval)
	{
		this.interval = interval;
	}
}
