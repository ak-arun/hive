package test;

import java.util.Date;
import java.util.Timer;

public class Main {

	public static void main(String[] args) {
		Timer timer = new Timer();
		MyTimerTask task = new MyTimerTask(timer,"test.properties");
		timer.scheduleAtFixedRate(task, new Date(), 1000*3600*task.getIntreval());
	}
	
}
