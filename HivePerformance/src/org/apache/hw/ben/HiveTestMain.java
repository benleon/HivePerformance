package org.apache.hw.ben;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;


/**
 * main class to execute the test suite
 * @author bleonhardi
 *
 */
public class HiveTestMain {
	
	public static String propertyFileLocation;
	public static String defaultPropertyFile  = "hivetest.conf";
	public static Properties hiveTestProperties;

	
	public static void main(String[] args) {
		System.out.println("Start Hive Performance Test");
		System.out.println("Date at start is: " + new Date());
		if (args.length > 0) propertyFileLocation = args[0];
		
		
		if (readPropertyFile() == true)
		{
			HiveExecutor exec = new HiveExecutor(hiveTestProperties);
			exec.start();
		}
	}
	
	/**
	 * reading property file or writing error.
	 */
	public static boolean readPropertyFile()
	{
		if (propertyFileLocation == null) propertyFileLocation = defaultPropertyFile;
		
		hiveTestProperties = new Properties();
		File propFile = new File(propertyFileLocation);
		try {
			FileInputStream in = new FileInputStream(propFile);
			hiveTestProperties.load(in);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Cannot read specified Property File: " + propertyFileLocation);
			e.printStackTrace();
			return false;
		} 
		
		

	}
	
}
