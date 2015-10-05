package org.apache.hw.ben;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

public class YarnConnector {
	protected static Configuration conf = new YarnConfiguration();
	YarnClient client = null;
	
	public YarnConnector()
	{
		client = YarnClient.createYarnClient();
		client.init(conf);
	}
	
	public ArrayList<ApplicationReport> getRunningApps() throws YarnException, IOException
	{
		ArrayList<ApplicationReport> apps = new ArrayList<ApplicationReport>();
		List<ApplicationReport> allApps =  client.getApplications();
		for (ApplicationReport a: allApps )
		{
			if (a.getYarnApplicationState() == YarnApplicationState.RUNNING 
					|| a.getYarnApplicationState() == YarnApplicationState.SUBMITTED
					|| a.getYarnApplicationState() == YarnApplicationState.NEW)
			{
				apps.add(a);
			}
		}
		return apps;
	}
	
	@Test
	public void killAllApps() throws YarnException, IOException {
	
	System.out.println("Connecting to Yarn" + conf.get("hadoop.registry.zk.quorum"));
	client.start();
	System.out.println(client.getApplications().size());
	ArrayList<ApplicationReport> runningApps = this.getRunningApps();
//	for (ApplicationReport a : client.getApplications())
//	{
//		if (a.getFinalApplicationStatus() == getYarnApplicationState.RUNNING)
//		{
//			client.killApplication(a.getApplicationId());
//		}
//	}
	
	System.out.println("Running Applications " + runningApps.size());
	
	int waitForAppsToDie = 12;
	for ( int i = 0; i < waitForAppsToDie; i++)
	{
		ArrayList<ApplicationReport> currentlyRunningApps = this.getRunningApps();
		for (ApplicationReport a : currentlyRunningApps)
		{
			client.killApplication(a.getApplicationId());
		}

		if (currentlyRunningApps.size() > 0) 
		{
			System.out.println("Running Applications " + currentlyRunningApps.size());

		}
		else
		{
			System.out.println("No Apps running");
			return;
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	//client.killApplication(arg0);
	//client.createApplication();
	}
	
//	  public void killApplication(ApplicationId applicationId)
//	      throws YarnException, IOException {
//	    KillApplicationRequest request =
//	        Records.newRecord(KillApplicationRequest.class);
//	    request.setApplicationId(applicationId);
//
//	    try {
//	      int pollCount = 0;
//	      long startTime = System.currentTimeMillis();
//
//	      while (true) {
//	        KillApplicationResponse response =
//	            rmClient.forceKillApplication(request);
//	        if (response.getIsKillCompleted()) {
//	          LOG.info("Killed application " + applicationId);
//	          break;
//	        }
//
//	        long elapsedMillis = System.currentTimeMillis() - startTime;
//	        if (enforceAsyncAPITimeout() &&
//	            elapsedMillis >= this.asyncApiPollTimeoutMillis) {
//	          throw new YarnException("Timed out while waiting for application " +
//	            applicationId + " to be killed.");
//	        }
//
//	        if (++pollCount % 10 == 0) {
//	          LOG.info("Waiting for application " + applicationId + " to be killed.");
//	        }
//	        Thread.sleep(asyncApiPollIntervalMillis);
//	      }
//	    } catch (InterruptedException e) {
//	      LOG.error("Interrupted while waiting for application " + applicationId
//	          + " to be killed.");
//	    }
//	  }
}
