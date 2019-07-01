package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class MyTimerTask extends TimerTask {

	private String configFilePath;

	private Properties properties;
	private boolean isFirstRun;
	private Timer timer;

	public MyTimerTask(Timer t, String configFilePath) {
		this.timer = t;
		this.configFilePath = configFilePath;
		this.isFirstRun=true;
		try{
			initProperties();
		}catch (Exception e){}
	}

	public int getIntreval(){
		return Integer.parseInt(properties.getProperty("execution.intreval.hours"));
	}

	@Override
	public void run() {
		try {
			refreshPropertiesIfNeeded();
			boolean endCondition = Boolean.parseBoolean(properties
					.getProperty("execution.terminate"));
			if (endCondition) {
				shutDown();
			} else {
				System.out.println(Thread.currentThread().getId() + ">> "
						+ new Date());
			}
		} catch (Exception e) {
			shutDown();
			e.printStackTrace();
		}
	}

	private void refreshPropertiesIfNeeded() throws FileNotFoundException,
			IOException {
		if(!isFirstRun){
			initProperties();
		}
		else{
			isFirstRun = false;
		}
	}

	public Timer getTimer() {
		return timer;
	}

	public void setTimer(Timer t) {
		this.timer = t;
	}

	private void initProperties() throws FileNotFoundException, IOException {
		System.out.println("Properties Loaded ");
		properties = new Properties();
		properties.load(new FileReader(new File(configFilePath)));
	}

	private void shutDown() {
		this.cancel();
		timer.cancel();
	}

}
