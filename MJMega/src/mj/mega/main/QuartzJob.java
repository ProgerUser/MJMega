package mj.mega.main;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzJob implements Job {
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		MjTask mytask = new MjTask();
		mytask.Run();
	}
}
