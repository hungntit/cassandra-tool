package org.oversea.utils;

public class TimeUtils {
	public static String showNiceTime(long timeInMiliSeconds){
		long ms =  timeInMiliSeconds% 1000;
		long sec = timeInMiliSeconds/1000;
		long min = sec/60;
		sec = sec % 60;
		long hour = min/60;
		min = min % 60;
		long day = hour/24;
		hour = hour%24;
		
		StringBuffer sb = new StringBuffer();
		if(day>0) sb.append(day).append("d ");
		if (hour>0) sb.append(hour).append("h ");
		if (min>0) sb.append(min).append("m ");
		if (sec>0) sb.append(sec).append("s ");
		sb.append(ms).append("ms");
		return sb.toString();
	}
}
