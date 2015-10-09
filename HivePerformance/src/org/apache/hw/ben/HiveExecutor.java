package org.apache.hw.ben;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.apache.hadoop.yarn.exceptions.YarnException;


/**
 * main execution thread of test suite. 
 * @author bleonhardi
 *
 */
public class HiveExecutor {
	
	

	
	Properties hiveTestProperties = null;
	HiveHelper helper = null;
	ArrayList<HiveExecutionThread> threads = new ArrayList<HiveExecutionThread>(); 
	
	
	public HiveExecutor( Properties props)
	{
		this.hiveTestProperties = props;
		helper = new HiveHelper(hiveTestProperties);
	}
	
	/**
	 * starts test execution
	 */
	public void start()
	{
		if (helper.propertyTrue("killYarnApps"))
		{
			YarnConnector y = new YarnConnector();
			try {
				y.killAllApps();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
//		String page = helper.getWebPage("http://sandbox:8088/cluster");
//		ArrayList<String> tags = helper.getDataInTag(page, "tbody", "ui-widget-content");
//		
//		tags = helper.getDataInTag(tags.get(0), "td", "");
//		for (String a : tags) System.out.println(helper.memoryString2GB(a));
//		if ( 1 ==1 ) return;
		QuerySet set = new QuerySet(helper);
		Date globalStart = new Date();
		
		System.out.println("Starting Queries: " + globalStart);

		
		if (!helper.propertyTrue("skipExecution"))
		{
			if (hiveTestProperties.getProperty("poolExecution") ==null) 
			{
				runFolderQueries(set);
			}
			else
			{
				runPoolQueries(set);
			}
		}
		
		
		Date globalEnd = new Date();
		System.out.println("Ended Queries: " + globalEnd + " in total it took " + HiveHelper.getDifference(globalStart,  globalEnd));
		
		if (helper.propertyTrue("diffImpala"))
		{
			System.out.println("Perform Result Diff");

			for (int i = 0; i < set.allQueries.size() / helper.getNumberFromProperty("multiplyQueries", 1); i++)
			{
				set.allQueries.get(i).diffResults();
			}
			
		}
		if (!helper.propertyTrue("skipExecution"))
		{
			printQuerySummary(set);
			printThreadSummary();
			printTotalSummary(set, HiveHelper.getDifference(globalStart,  globalEnd));
		}
		
	}
	
	/*
	 * will write a couple of total summary lines to sysout
	 */
	public void printTotalSummary(QuerySet set, double totalTime)
	{
		int execQueries = set.allQueries.size();
		int failedQueries = 0;
		int diffQueries=0;
		int longQueries =0;
		int cutoff = helper.getNumberFromProperty("cutoff", 60);
		double queriesPerSec = 0.0;
		double minTime = 0.0;
		double maxTime = 0.0;
		double avgTime = 0.0;
		double totalSingleTime = 0.0;
		DecimalFormat dec = new DecimalFormat("#.##");
		for (HiveTestQuery q : set.allQueries)
		{
			if (q.queryFailed == true ) failedQueries++;
			if (q.rowsDifferent > 0 ) diffQueries++;
			if (q.totalExecTime() > cutoff ) longQueries++;
			minTime  = Math.min(minTime, q.totalExecTime());
			maxTime  = Math.max(maxTime, q.totalExecTime());
			totalSingleTime += q.totalExecTime();
		}
		avgTime = totalSingleTime / execQueries;
		queriesPerSec = execQueries / totalTime;

		System.out.println("QuerySummary: Threads=" + threads.size()
							+ ", Executed Queries=" + execQueries 
							+ ", Failed Queries=" + failedQueries 
							+ ", ResultsDifferentQueries=" + diffQueries
							+ ", QueriesOverCutoff=" + longQueries + "["+ cutoff + " sec]");
		
		System.out.println("Performance Summary: QueriesPerSec=" + dec.format(queriesPerSec)
							+ ", MinTimeNonFailed=" + dec.format(minTime)
							+ ", MaxTimeNonFailed=" + dec.format(maxTime)
							+ ", AverageTime=" + dec.format(avgTime));
		
	}
	
	public void printThreadSummary()
	{
		try
		{
			File threadFile = new File(hiveTestProperties.getProperty("testFolder") + "/" + "thread_" + (new Date()).getTime() + ".csv");
		
			threadFile.delete();
			threadFile.createNewFile();

			FileWriter outputWriter = new FileWriter (threadFile);
			outputWriter.write("Thread, TotalTime, QueriesExecuted, Connection\n");
			for (int i = 0; i < threads.size(); i ++ )
			{
				outputWriter.write(threads.get(i).getSummaryCSV());
			}
			outputWriter.close();
			
		}
		catch ( Exception e)
		{
			System.out.println("Could not print thread summary");
			e.printStackTrace();
		}
	}
	
	public void printStatsSummary(ArrayList<PerformanceNumbers> stats)
	{
		if ( (stats == null) || stats.size() < 1) return;
		try
		{
			File statsFile = new File(hiveTestProperties.getProperty("testFolder") + "/" + "stats_" + (new Date()).getTime() + ".csv");
		
			statsFile.delete();
			statsFile.createNewFile();
			
			FileWriter outputWriter = new FileWriter (statsFile);
			outputWriter.write(stats.get(0).returnCSVHeader());
			for (int i = 0; i < stats.size(); i ++ )
			{
				outputWriter.write(stats.get(i).toCSVLine());
			}
			outputWriter.close();
			
		}
		catch ( Exception e)
		{
			System.out.println("Could not print Stats summary");
			e.printStackTrace();
		}
	}

	public void printQuerySummary(QuerySet set)
	{
		try
		{
			File queryTime = new File(hiveTestProperties.getProperty("testFolder") + "/" + "query_" + (new Date()).getTime() + ".csv");
		
			queryTime.delete();
			queryTime.createNewFile();

			FileWriter outputWriter = new FileWriter (queryTime);
			outputWriter.write("Query, TotalTime, ExecTime, FetchTime, RowsReturned, ImpalaTime, ImpalaRows, RowsDifferent, Problem, MinDate, MaxDate, Thread");
			if (set.tdg != null)
			{
				if (set.tdg.identificationFields != null)
				{
					for (String s : set.tdg.identificationFields)
					{
						outputWriter.write(", " + s);
					}
				}
			}
			
			outputWriter.write("\n");
			for (int i = 0; i < set.allQueries.size(); i ++ )
			{
				outputWriter.write(set.allQueries.get(i).getSummaryCSV());
			}
			outputWriter.close();
			
		}
		catch ( Exception e)
		{
			System.out.println("Could not print query summary");
			e.printStackTrace();
		}
	}
	
	

	// runs one thread per folder of queries like old test bench
	public void runFolderQueries(QuerySet set)
	{
		System.out.println("Running one thread per folder " + set.folders.size() + " folders " + set.allQueries.size() + " queries");
		//con.test();
		
		//ArrayList<HiveExecutionThread> threads = null; 
		ExecutorService executor = Executors.newFixedThreadPool(set.folders.size());
		for ( int i = 0; i < set.folders.size(); i++)
		{
			HiveConnection con = null;
			if (helper.propertyEquals("databaseType", "phoenix"))
			{
				con = new PhoenixConnection(this.hiveTestProperties);
			} else
			{
				con = new HiveConnection(this.hiveTestProperties);
			}
			con.hiveServerId = i % helper.getNumberFromProperty("numberConnections", 1);
			
			//HiveExecutionThread thr = new HiveExecutionThread(set.folders.get(i));
			HiveExecutionThread worker = new HiveExecutionThread(set.folders.get(i), i, con, helper);
			threads.add(worker);
			executor.execute(worker);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// runs one thread per folder of queries like old test bench
		public void runPoolQueries(QuerySet set)
		{

			//con.test();
			int numThreads = helper.getNumberFromProperty("numberThreads", 1);
			
			System.out.println("Running pooled threads. " + numThreads + " threads " + set.allQueries.size() + " queries");

			//ArrayList<HiveExecutionThread> threads = null; 
			ExecutorService executor = Executors.newFixedThreadPool(numThreads);
			for ( int i = 0; i < numThreads; i++)
			{
				//HiveConnection con = new HiveConnection(this.hiveTestProperties);
				HiveConnection con = null;
				//System.out.println("creating connection");
				if (helper.propertyEquals("databaseType", "phoenix"))
				{
					con = new PhoenixConnection(this.hiveTestProperties);
				} else
				{
					con = new HiveConnection(this.hiveTestProperties);
				}
			//	System.out.println(" connection created");

				con.hiveServerId = i % helper.getNumberFromProperty("numberConnections", 1);
				//HiveExecutionThread thr = new HiveExecutionThread(set.folders.get(i));
				HiveExecutionThread worker = new HiveExecutionThread(set, i, con, helper);
				//System.out.println(" Thread created");

				threads.add(worker);
				executor.execute(worker);
			}
			if (!helper.verbose) System.out.print("[");
			executor.shutdown();
			try {
				//int lastPercentage = 0;
				int lastQueries = 0;
				int allQueries = set.allQueries.size();
				int checkSpeed = helper.getNumberFromProperty("checkSpeed", 10);
				DecimalFormat dec  = new DecimalFormat("#.##");
				ArrayList<PerformanceNumbers> stats= new ArrayList<PerformanceNumbers>();
				
				while ( !executor.isTerminated())
				{
					int currentPercentage = set.percentageFinished();
					int currentNumQueries = getExecutedQueries();
					double speed = (currentNumQueries  - lastQueries) / (double)checkSpeed;
					PerformanceNumbers currentStat = new PerformanceNumbers(allQueries, currentPercentage, currentNumQueries, speed);
					if (helper.resourceManager != null)
					{
						ArrayList<String> values = helper.getTags(helper.resourceManager);
						currentStat.applyValuesFromRM(values);
					}
					stats.add(currentStat);
					currentStat.printSummary();
				//	System.out.println(currentPercentage + " Percent finished. " + currentNumQueries + " of " + allQueries + " Queries executed. Speed " + dec.format(speed) + " Queries per second "); 
					lastQueries = currentNumQueries;
					Thread.sleep(checkSpeed * 1000);
					
				}
				this.printStatsSummary(stats);
				
			//	executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public int getExecutedQueries()
		{
			int ret = 0;
			for ( int i = 0; i < threads.size(); i++)
			{
				ret += threads.get(i).numQueries;
			}
			return ret;
		}

}
