package org.apache.hw.ben;

import java.text.DecimalFormat;
import java.util.ArrayList;

public class PerformanceNumbers {
	
	

	public PerformanceNumbers(int allQueries, double percentFinished, int currentQueries,
			double speed, int appsSubmitted, int appsRunning, int appsPending,
			int containersRunning, double memoryUsed, double utilization) {
		this.allQueries = allQueries;
		this.percentFinished = percentFinished;
		this.currentQueries = currentQueries;
		this.speed = speed;
		this.appsSubmitted = appsSubmitted;
		this.appsRunning = appsRunning;
		this.appsPending = appsPending;
		this.containersRunning = containersRunning;
		this.memoryUsed = memoryUsed;
		this.utilizationPercent = utilization;
		this.setRMValues = true;
	}
	
	public PerformanceNumbers (int allQueries,  double percentFinished, int currentQueries, double speed)
	{
		this.allQueries = allQueries;
		this.percentFinished = percentFinished;
		this.currentQueries = currentQueries;
		this.speed = speed;
		
	}

	public void applyValuesFromRM(ArrayList<String> values)
	{
		this.appsSubmitted = new Integer ( values.get(0).trim()).intValue();
		this.appsRunning = new Integer ( values.get(2).trim()).intValue();
		this.appsPending = new Integer ( values.get(1).trim()).intValue();
		this.containersRunning = new Integer ( values.get(4).trim()).intValue();
		this.memoryUsed = this.memoryString2GB(values.get(5).trim());
		double totalMem = this.memoryString2GB(values.get(6).trim());
		this.utilizationPercent = memoryUsed / totalMem * 100;
		setRMValues = true;
	}
	
	public boolean setRMValues = false;
	
	public int allQueries = 0;
	public double percentFinished = 0.0;
	public int currentQueries = 0;
	public double speed = 0.0;
	public int appsSubmitted = 0;
	public int appsRunning = 0;
	public int appsPending = 0;
	public int containersRunning = 0;
	public double memoryUsed = 0.0;
	public double utilizationPercent = 0.0;
	
	public double memoryString2GB(String mem)
	{
		mem = mem.trim();
		if (mem == null) return 0.0;
		if (mem.equals("")) return 0.0;
		double number = 0.0;
		//System.out.println("hallo" + mem);
		try
		{
		if (mem.endsWith(" B"))
		{
			//System.out.println("why not");
			number = new Double(mem.substring(0, mem.indexOf(" "))).doubleValue() / 1000000000.0;
		}
		else if (mem.endsWith(" KB"))
		{
			number = new Double(mem.substring(0, mem.indexOf(" "))).doubleValue() / 1000000.0;
		}
		else if (mem.endsWith(" MB"))
		{
			number = new Double(mem.substring(0, mem.indexOf(" "))).doubleValue() * 1000.0;

		}
		else if (mem.endsWith(" GB"))
		{
			number = new Double(mem.substring(0, mem.indexOf(" "))).doubleValue();

		}
		else if (mem.endsWith(" TB"))
		{
			number = new Double(mem.substring(0, mem.indexOf(" "))).doubleValue() * 1000.0;

		}
		else
		{
			number = new Double(mem.substring(0)) / 1000000000.0;
		}
		}
		catch ( Exception e)
		{
			e.printStackTrace();
		}
		return number;
	}
	
	
	public String returnCSVHeader()
	{
		if (this.setRMValues)
		{
			return "PercentFinished, CurrentQueries, Speed, AppsSubmitted, AppsRunning, AppsPending, ContainersRunning, MemoryUsed, UtilizationPercent\n";
		}
		else
		{
			return "PercentFinished, CurrentQueries, Speed\n";
		}
	}
	
	public String toCSVLine()
	{
		DecimalFormat dec = new DecimalFormat("#.##");
		//dec.setMaximumFractionDigits(2);
		if (this.setRMValues)
		{
			return 
					dec.format(percentFinished)  + "," +
			currentQueries  + "," +
			dec.format(speed)  + "," +
			appsSubmitted  + "," +
			appsRunning  + "," +
			appsPending  + "," +
			containersRunning  + "," +
			dec.format(memoryUsed)  + "," +
			dec.format(utilizationPercent)  + "\n";		}
		else
		{
			return 
					dec.format(percentFinished)  + "," +
			currentQueries  + "," +
			dec.format(speed)  + "\n";
		}
		
		
		
	}
	
	public void printSummary()
	{
		DecimalFormat dec = new DecimalFormat("#.##");
		System.out.println(this.percentFinished + " Percent finished. " + this.currentQueries + " of " + allQueries + " Queries executed. Speed " + dec.format(speed) + " Queries per second ");

		if (this.setRMValues)
		{
			System.out.println(dec.format(this.utilizationPercent) + " % Utilization,  " + this.containersRunning + " containers used, " + this.memoryUsed + " Memory used, " + this.appsRunning + " Apps running, " + this.appsPending + " Apps Pending" );
		}
		
	}
	
	

}