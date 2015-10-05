package org.apache.hw.ben;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.phoenix.jdbc.PhoenixDriver;


import org.apache.hive.jdbc.HiveDriver;

public class PhoenixConnection extends HiveConnection {

	public PhoenixConnection(Properties prop) {
		super(prop);
	}
	
	public PhoenixConnection(Properties prop, int hiveServerId) {
		super(prop, hiveServerId);
	}
	
	public boolean connect()
	{
		this.con = getConnection();	
		if (con == null) return false;
		return true;
	}
	
	public Connection getConnection()
	{
		
		//String database = "default";
		String zkhbase="hbase-unsecure";
		String port = "2181";
		String host = "localhost";
		String user = "hive";
		String password = "";
		
		//String databasePropString = "database";
		String zkhbasePropString = "zkhbase";
		String portPropString = "port";
		String hostPropString = "host";
		String userPropString = "user";
		String passwordPropString = "password";
		
		
		
		// first get default values from the base connection
		if (prop.getProperty(zkhbasePropString) != null) zkhbase = prop.getProperty(zkhbasePropString);
		if (prop.getProperty(portPropString) != null) port = prop.getProperty(portPropString);
		if (prop.getProperty(hostPropString) != null) host = prop.getProperty(hostPropString);
		if (prop.getProperty(userPropString) != null) user = prop.getProperty(userPropString);
		if (prop.getProperty(passwordPropString) != null) password = prop.getProperty(passwordPropString);
		
		//now get overwrite values for specific connection
		
		if (this.hiveServerId != 0)
		{
			zkhbasePropString = zkhbasePropString + hiveServerId;
			portPropString = portPropString + hiveServerId;
			hostPropString = hostPropString + hiveServerId;
			userPropString = userPropString + hiveServerId;
			passwordPropString = passwordPropString + hiveServerId;
		}
		
		if (prop.getProperty(zkhbasePropString) != null) zkhbase = prop.getProperty(zkhbasePropString);
		if (prop.getProperty(portPropString) != null) port = prop.getProperty(portPropString);
		if (prop.getProperty(hostPropString) != null) host = prop.getProperty(hostPropString);
		if (prop.getProperty(userPropString) != null) user = prop.getProperty(userPropString);
		if (prop.getProperty(passwordPropString) != null) password = prop.getProperty(passwordPropString);
				
		//Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
	    //conn =  DriverManager.getConnection("jdbc:phoenix:localhost:/hbase-unsecure");
	   // System.out.println("got connection");
		//jdbc:phoenix:localhost:2181:/hbase-unsecure
		connectionString = "jdbc:phoenix:"  + host + ":" + port + ":/" + zkhbase;
		try {
			System.out.println("Phoenix URL" + connectionString);
			//Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			DriverManager.registerDriver(new PhoenixDriver());
			//System.out.println("before connection create");

			Connection con =  DriverManager.getConnection(connectionString);
			//System.out.println("Created connection");
			return con;
		} catch (SQLException e) {
			System.out.println("Could not get Hive connection with URL: " + connectionString + " and user " + user + " and password " + password );
			e.printStackTrace();
		}
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			System.out.println("Phoenix Jar not in classpath ");
//			e.printStackTrace();
//		}
		return null;
	
	}
	
}
