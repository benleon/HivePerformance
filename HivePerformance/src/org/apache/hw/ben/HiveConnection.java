package org.apache.hw.ben;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import org.apache.hive.jdbc.HiveDriver;

/**
 * Class will connect to a Hive server and execute a set of Java 
 * queries
 * @author bleonhardi
 *
 */
public class HiveConnection {
	
	public Properties prop;
	
	public Connection con = null;
	public HiveHelper helper= null;
	
	// in case there are more than one hiveserver defined in the property file
	public int hiveServerId = 0;
	
	public String connectionString = "";
	
	public HiveConnection(Properties prop) {
		this.prop = prop;
		 helper = new HiveHelper(this.prop);
	}
	
	public HiveConnection(Properties prop, int hiveServerId) {
		this.hiveServerId = hiveServerId;
		this.prop = prop;
		 helper = new HiveHelper(this.prop);
	}
	
	public boolean connect()
	{
		this.con = getConnection();
//		try {
//			this.con.setAutoCommit(true);
//		}
//		catch ( Exception e)
//		{ 
//			e.printStackTrace();
//		}
		if (helper.propertyTrue("removeATS"))
		{
			try {
				Statement queryStatement = con.createStatement();
				queryStatement.execute("set hive.exec.pre.hooks=");
				queryStatement.execute("set hive.exec.post.hooks=");
				queryStatement.execute("set hive.exec.failure.hooks=");
				// for DAL and Tez tasks with 1GB 
				queryStatement.execute("set tez.runtime.io.sort.mb = 333");
				queryStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
				//System.out.println("Query Failure: " +  query.printQuery() );
				//e.printStackTrace();
			}
		}
		
		if (con == null) return false;
		return true;
	}
	
	public void runQuery(HiveTestQuery query, int threadId)
	{
		query.threadId = threadId;
		if (con != null)
		{
			ResultSet  rs = null;
			Statement queryStatement = null;
			try {
				if (helper.verbose) System.out.println("Query To execute " + query.queryString);
				query.queryStarted = new Date();
				queryStatement = con.createStatement();
				queryStatement.setFetchSize(10000);
				rs = queryStatement.executeQuery(query.queryString);
				query.queryEnded = new Date();
				helper.testResult(rs, query);
				query.resultsFetched = new Date();
				
				rs.close();
				queryStatement.close();
			} catch (SQLException e) {
				query.queryEnded = new Date();
				query.resultsFetched = new Date();

				helper.writeError(query, e);
				//System.out.println("Query Failure: " +  query.printQuery() );
				//e.printStackTrace();
			}
			finally
			{
				if (rs != null )
					try {
						rs.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				if (queryStatement !=null)
					try {
						queryStatement.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
//			String testQueryString = "SELECT * FROM ORDERS";
		}
		else
		{
			System.out.println(" Couldn't open connection ");
		}
	}
	
	
	
	public Connection getConnection()
	{
		String database = "default";
		String port = "10000";
		String host = "localhost";
		String user = "hive";
		String password = "";
		
		String databasePropString = "database";
		String portPropString = "port";
		String hostPropString = "host";
		String userPropString = "user";
		String passwordPropString = "password";
		
		
		
		// first get default values from the base connection
		if (prop.getProperty(databasePropString) != null) database = prop.getProperty(databasePropString);
		if (prop.getProperty(portPropString) != null) port = prop.getProperty(portPropString);
		if (prop.getProperty(hostPropString) != null) host = prop.getProperty(hostPropString);
		if (prop.getProperty(userPropString) != null) user = prop.getProperty(userPropString);
		if (prop.getProperty(passwordPropString) != null) password = prop.getProperty(passwordPropString);
		
		//now get overwrite values for specific connection
		
		if (this.hiveServerId != 0)
		{
			databasePropString = databasePropString + hiveServerId;
			portPropString = portPropString + hiveServerId;
			hostPropString = hostPropString + hiveServerId;
			userPropString = userPropString + hiveServerId;
			passwordPropString = passwordPropString + hiveServerId;
		}
		
		if (prop.getProperty(databasePropString) != null) database = prop.getProperty(databasePropString);
		if (prop.getProperty(portPropString) != null) port = prop.getProperty(portPropString);
		if (prop.getProperty(hostPropString) != null) host = prop.getProperty(hostPropString);
		if (prop.getProperty(userPropString) != null) user = prop.getProperty(userPropString);
		if (prop.getProperty(passwordPropString) != null) password = prop.getProperty(passwordPropString);
				
		

		connectionString = "jdbc:hive2://"  + host + ":" + port + "/" + database;
		try {
			DriverManager.registerDriver(new HiveDriver());
			return DriverManager.getConnection(connectionString, user, password);
		} catch (SQLException e) {
			System.out.println("Could not get Hive connection with URL: " + connectionString + " and user " + user + " and password " + password );
			e.printStackTrace();
		}
		return null;
	
	}
	
	public void refreshConnection()
	{
		try {
			this.con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.con = getConnection();
	}
	
	public void testConnection(Connection con)
	{
//		HiveSQLHelper helper = new HiveSQLHelper(this.prop);
//		try {
//			Statement testQuery = con.createStatement();
//			String testQueryString = "SELECT * FROM ORDERS";
//			ResultSet  rs = testQuery.executeQuery(testQueryString);
//			helper.testResult(rs, "/Users/bleonhardi/in", testQueryString);
//		} catch (SQLException e) {
//			System.out.println("Problems executing query");
//			e.printStackTrace();
//		}
		
		
	}
	
	public void test()
	{
		for (int i = 0; i < 100; i++)
		{
			this.testConnection(this.getConnection());

		}
	}
	
	public String getLineAsString(ResultSet rs)
	{
		try {
			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();
			System.out.println("Number Columns" + cols);
			StringBuffer resultString = new StringBuffer();
			rs.next();
			for ( int i = 1; i < cols + 1; i ++)
			{
				resultString.append ( rs.getString(i)); 
				if ( i < cols -1 ) resultString.append ( "\t");
			}
			
		 	return resultString.toString();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "";
	}
	
	
		
		

}
