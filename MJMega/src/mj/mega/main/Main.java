package mj.mega.main;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class Main {

	private static final String TRIGGER_NAME = "MEGA_TRIGGER";
	private static final String GROUP = "MEGA_GROUP";
	private static final String JOB_NAME = "MEGA_JOB";
	private static Scheduler scheduler;
	public static Logger logger = Logger.getLogger(Main.class);

	public static Properties prop = new Properties();

	public static void main(String[] args) throws Exception {
		DOMConfigurator.configure(Main.class.getResource("/log4j.xml"));
		// load a properties file
		try {
			InputStream input = new FileInputStream(System.getenv("IbankFiz") + "/config.properties");
			Main.logger.info(System.getenv("IbankFiz"));
			prop.load(input);
		} catch (Exception e) {
			Main.logger.error(getStackTrace(e));
			System.exit(0);
		}
		//
		scheduler = new StdSchedulerFactory().getScheduler();
		scheduler.start();
		Trigger trigger = buildCronSchedulerTrigger();
		scheduleJob(trigger);
	}

	private static void scheduleJob(Trigger trigger) throws Exception {
		JobDetail someJobDetail = JobBuilder.newJob(QuartzJob.class).withIdentity(JOB_NAME, GROUP).build();
		scheduler.scheduleJob(someJobDetail, trigger);
	}

	private static Trigger buildCronSchedulerTrigger() {
		Main.logger.info(prop.getProperty("Cron"));
		String CRON_EXPRESSION = prop.getProperty("Cron");
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(TRIGGER_NAME, GROUP)
				.withSchedule(CronScheduleBuilder.cronSchedule(CRON_EXPRESSION)).build();
		return trigger;
	}
	
	public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }
}