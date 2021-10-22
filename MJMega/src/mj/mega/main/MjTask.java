package mj.mega.main;

import org.apache.log4j.Logger;

public class MjTask {

	public MjTask() {
		Main.logger = Logger.getLogger(getClass());
	}

	
	public void Run() {
		Main.logger.info("RunJob");
	}

}