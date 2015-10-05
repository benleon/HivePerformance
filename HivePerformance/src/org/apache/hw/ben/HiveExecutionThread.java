package org.apache.hw.ben;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;


public class HiveExecutionThread implements Runnable {
	
	// this is used for the one thread for each folder case. 
	// in this case every thread gets a folder to execute
	public ArrayList<HiveTestQuery> queries = null;
	
	// this is used in case a number of threads are intended to execute 
	// all queries in parallel as fast as they can
	public QuerySet set = null;
	
	HiveConnection con = null;
	
	Date before = new Date();
	Date after = new Date ();
	
	int threadId = 0;
	
	int numQueries = 0;
	
	HiveHelper helper;
	
	
	
	//HiveConnection con = new HiveConnection(hiveTestProperties);
	//con.test();
	
	public HiveExecutionThread(ArrayList<HiveTestQuery> queries, int threadId, HiveConnection con, HiveHelper helper) {
		this.threadId = threadId;
		 this.con = con;
		this.queries = queries;
		this.helper = helper;
	}
	
	public HiveExecutionThread(QuerySet set, int threadId, HiveConnection con, HiveHelper helper) {
		this.threadId = threadId;
		this.con = con;
		this.set = set;
		this.helper = helper;
		//System.out.println(" in thread ");

	}

	public void executeQueries()
	{
		
		before = new Date();
		// executing fixed set of queries.
		if (this.queries != null)
		{
			System.out.println("Thread: " + threadId + " starting execution of " + queries.size() + " queries. Start time: "+ before);
			for ( int i = 0; i < queries.size(); i++)
			{
				con.runQuery(queries.get(i), this.threadId);
			}
			after = new Date ();
			this.numQueries = this.queries.size();
			System.out.println("Thread: " + threadId + " Finished executing queries, End time: " + after + " in total it took " + HiveHelper.getDifference(before,  after) + " seconds.");

		}
		// executing queries from pool
		if (this.set != null )
		{
			HiveTestQuery currentQuery = set.getNextQuery();
			System.out.println("Thread: " + threadId + " starting pool execution. Start time: "+ before);
			while ( currentQuery != null) {
				con.runQuery(currentQuery, this.threadId);
				this.numQueries ++;
				currentQuery = set.getNextQuery();
				
				if (helper.refreshConnection > 0)
				{
					if (numQueries % helper.refreshConnection == 0)
					{
						this.con.refreshConnection();
					}
				}
				// if (numQueries == 32) con.refreshConnection();
			}
			after = new Date ();
			
			System.out.println("Thread: " + threadId + " Finished executing " + this.numQueries + " queries, End time: " + after + " in total it took " + HiveHelper.getDifference(before,  after) + " seconds.");

		}
	}
	
	@Override
	public void run() {
		//System.out.println(" in thread connect and now execute");

		con.connect();
		//System.out.println(" in thread connect and now execute");

		// TODO Auto-generated method stub
		//super.run();
	//	 before = new Date();
		//System.out.println("Queries executed in Thread" + set.allqueries.queries.size() + " Time "+ new Date());
		this.executeQueries();
		//after = new Date ();
	//	System.out.println("Queries execution finished" + queries.size() + " Time "+ new Date() + " in total it took " + HiveHelper.getDifference(before,  after));
		
	}
	
	public String getSummaryCSV()
	{
		DecimalFormat dec = new DecimalFormat("#.##");
		//dec.setMaximumFractionDigits(2);
		return 
				"" + this.threadId + ","  +
				HiveHelper.getDifference(before, after) + "," +
				this.numQueries + "," + 
				con.connectionString + "\n";
				
	}
	
	
}
