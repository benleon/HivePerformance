package org.apache.hw.ben;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// this class defines a test query to run
public class HiveTestQuery {
	
	public String fileLocation = "";
	public String queryString = "";
	public Date queryStarted = new Date();
	public Date queryEnded = new Date();
	public Date resultsFetched = new Date();
	
	public int threadId = 0;
	
	public boolean queryFailed = false;
	
	public int rowsReturned = 0;
	
	int impalaRows = -1;
	double impalaTime = -1.0;
	
	int rowsDifferent = 0;
	
	
	String queryMinDate = "";
	String queryMaxDate = "";
	
	
	public HiveTestQuery(String fileLocation) {
		this.fileLocation = fileLocation;
		queryString = getStringFromFile(fileLocation);
		if (queryString.lastIndexOf(";") != -1)
		{
			queryString = queryString.substring(0, queryString.lastIndexOf(";"));
		}
		extractTimeRangeFromQuery();
	    readImpalaValues();
	}
	
	
	/**
	 * reads everything from an normal InputStream and puts it in a string 
	 * @param in the inputstream
	 * @return the returned String
	 */
	public String getStringFromFile(String fileName)
	{
		
		if (fileName.contains(".."))
		{
			System.out.println("Requested File contained ..");
		//	logger.error("FlashVis_Error_BadRequestLocation");
			return "";
		}
		DataInputStream in = null;
		StringBuffer strBuf = new StringBuffer();
		try
		{
			File file = new File(fileName);
			in = new DataInputStream(new FileInputStream(file));
			byte[] buffer = new byte[(int)file.length()];
			in.read(buffer, 0, (int)file.length());
			//System.out.println("filelength" + (int)file.length() );
			//System.out.println("buffer" + buffer);
			for (int i = 0; i < buffer.length; i++)
			{
				strBuf.append((char)buffer[i]);
			}
			//strBuf.append(buffer);
			return strBuf.toString();
		}
		catch (Exception e)
		{
			//logger.error("FlashVis_Error_FileLoadError", new String[]{fileName},e);
			e.printStackTrace();
		}
		finally
		{
			try
			{ 
				if (in != null) in.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		
		return "";
	}
	
	public String printQuery()
	{
		return this.fileLocation + "\t" + this.queryString;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.fileLocation;
	}
	
	public double execSeconds()
	{
		return HiveHelper.getDifference(this.queryStarted, this.queryEnded);
	}
	
	public double execFetch()
	{
		return HiveHelper.getDifference(this.queryEnded, this.resultsFetched);
	}
	
	public double totalExecTime()
	{
		return HiveHelper.getDifference(this.queryStarted, this.resultsFetched);
	}
	
	
	
	public String getSummaryCSV()
	{
		DecimalFormat dec = new DecimalFormat("#.##");
		//dec.setMaximumFractionDigits(2);
		return 
				this.fileLocation + ","  +
				dec.format(this.totalExecTime()) + "," +
				dec.format(this.execSeconds()) + "," +
				dec.format(this.execFetch()) + "," +
				this.rowsReturned + "," +
				dec.format(this.impalaTime) + "," +
				this.impalaRows + "," +
				this.rowsDifferent + "," +
				this.queryFailed + "," + 
				this.queryMinDate + "," +
				this.queryMaxDate + "," +
				this.threadId + "\n";
				
	}
	
	
	public void readImpalaValues()
	{
		String impalaFile = HiveHelper.replaceEnding(this.fileLocation, ".out");
		File f = new File(impalaFile);
		if (!f.exists()) return;

		FileInputStream fis;
		try {
			fis = new FileInputStream(impalaFile);

			// Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			
			while ((line = br.readLine()) != null) {
				if ( line.startsWith("Fetched"))
				{
					parseFetchStatement(line);
				}
				
			}

			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error reading Impala file");
			e.printStackTrace();
		}
		
	}
	
	
	public void parseFetchStatement(String impalaStatement)
	{
		try {
			String[] parts = impalaStatement.split(" ");
			if (parts.length < 5) return;
			
			
			String rows = parts[1];
			this.impalaRows = new Integer(rows).intValue();
			
			String time = parts[4].substring(0, parts[4].length() - 1);
			this.impalaTime = new Double(time).doubleValue();

		}
		catch (Exception e)
		{
			System.out.println("Couldn't parse Impala statement");
			e.printStackTrace();
			
		}
	}
	
	
	public void replaceValues(Map<String, String> mp)
	{
		for (Map.Entry<String, String> entry : mp.entrySet()) 
		{ 
			this.queryString = this.queryString.replaceAll(entry.getKey(), entry.getValue());
		}
		
	}
	
	
	public HiveTestQuery clone()
	{
		return new HiveTestQuery(this.fileLocation);
	}
	
	// this method tries to extract the date range from query
	// to compare with results
	public void extractTimeRangeFromQuery()
	{
		Pattern pattern = Pattern.compile("YYYYMMDD ?<=? ?([0-9]{8})");
        Matcher matcher = pattern.matcher(queryString);
        if (matcher.find())
        {
        	if (matcher.groupCount() > 0)
        	{
        		this.queryMaxDate = matcher.group(1);
        	}
        }
		pattern = Pattern.compile("YYYYMMDD ?>=? ?([0-9]{8})");
        matcher = pattern.matcher(queryString);
        if (matcher.find())
        {
        	if (matcher.groupCount() > 0)
        	{
        		this.queryMinDate = matcher.group(1).toString();
        	}
        }
		
	}
	
	
	// will diff Impala results and Hive Results by reading both files bringing them
	// in same format outputing in same format and sorting them. Then a diff is made and
	// output
	public void diffResults()
	{
		this.transformImpala();
		this.reorderHiveResults();
		this.diffImpalaHive();
	}
	
	public void transformImpala()
	{
		ArrayList<String> rows = new ArrayList<String>();
		String impalaFile = HiveHelper.replaceEnding(this.fileLocation, ".out");
		File f = new File(impalaFile);
		if (!f.exists()) return;

		FileInputStream fis;
		try {
			fis = new FileInputStream(impalaFile);

			// Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			
			//boolean querySetStarted = false;
			//boolean headerReached = false;
			//boolean resultSetFinished = false;
			int numDecoLines = 0;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("+--")) numDecoLines ++;
//				if (line.startsWith("+--"))
//				{
//					if (headerReached == false) headerReached = true;
//					else if ( querySetStarted == false) querySetStarted = true;
//					else if (resultSetFinished == false) resultSetFinished = true;
//					
//					
//				}
//				else
//				{
//					if (querySetStarted && !resultSetFinished)
//					{
//						String hiveLine= transformImpalaToHive(line);
//						rows.add(hiveLine);
//					}
//				}
				if ( line.startsWith("| ") && ((numDecoLines == 2) || ((numDecoLines -2) % 3 == 0)))
				{
					String hiveLine= transformImpalaToHive(line);
					rows.add(hiveLine);
				}
				
			}

			br.close();
			Collections.sort(rows);
			File outFile = new File(HiveHelper.replaceEnding(this.fileLocation, ".resultImpalaSorted"));
			outFile.delete();
			outFile.createNewFile();

			FileWriter outputWriter = new FileWriter (outFile);
			for ( int i = 0; i < rows.size(); i++)
			{
				outputWriter.write(rows.get(i)  + "\n");
			}
			outputWriter.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error reading Impala file");
			e.printStackTrace();
		}
	}
	
	public String transformImpalaToHive(String line)
	{
		StringTokenizer tok = new StringTokenizer("\\|");
		String res = new String();
		String[] cols = line.split("\\|");
		//System.out.println("Got following row" + line);
		
		if ( cols.length == 0 ) return "";
		for ( int i = 1; i < cols.length; i++)
		{
			String nextToken = cols[i].trim();
			if (nextToken.equals("NULL")) nextToken = "null";
			res = res + nextToken;
			if ( i != cols.length - 1)
			{
				res += "\t";
			}
		}
		//System.out.println("Changed it to" + res);
		return res;

		
	}
	
	public void reorderHiveResults()
	{
		ArrayList<String> rows = new ArrayList<String>();
		String hiveFile = HiveHelper.replaceEnding(this.fileLocation, ".result");
		File f = new File(hiveFile);
		if (!f.exists()) return;

		FileInputStream fis;
		try {
			fis = new FileInputStream(hiveFile);

			// Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			
			
			while ((line = br.readLine()) != null) {
				if (!line.startsWith("Query"))
				{
					rows.add(line);
					
				}
				
				
			}

			br.close();
			Collections.sort(rows);
			File outFile = new File(HiveHelper.replaceEnding(this.fileLocation, ".resultHiveSorted"));
			outFile.delete();
			outFile.createNewFile();

			FileWriter outputWriter = new FileWriter (outFile);
			for ( int i = 0; i < rows.size(); i++)
			{
				outputWriter.write(rows.get(i)  + "\n");
			}
			outputWriter.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error reading Hive file");
			e.printStackTrace();
		}
	}
	
	public void diffImpalaHive()
	{
		//ArrayList<String> rows = new ArrayList<String>();
		String hiveFile = HiveHelper.replaceEnding(this.fileLocation, ".resultHiveSorted");
		File fHive = new File(hiveFile);
		String impalaFile = HiveHelper.replaceEnding(this.fileLocation, ".resultImpalaSorted");
		File fimpala = new File(impalaFile);
		if (!fimpala.exists()) return;
		

		FileInputStream fHiveIS;
		FileInputStream fImpalaIS;
		
		File outFile = new File(HiveHelper.replaceEnding(this.fileLocation, ".resultDiff"));
		

		try {
			fHiveIS = new FileInputStream(hiveFile);
			fImpalaIS = new FileInputStream(impalaFile);
			// Construct BufferedReader from InputStreamReader
			BufferedReader brHive = new BufferedReader(new InputStreamReader(fHiveIS));
			BufferedReader brImpala = new BufferedReader(new InputStreamReader(fImpalaIS));
			outFile.delete();
			outFile.createNewFile();
			FileWriter outWriter = new FileWriter (outFile);

			String lineHive = null;
			String lineImpala = null;
			boolean Finished = false;
			lineHive = brHive.readLine();
			lineImpala = brImpala.readLine();
			while (lineHive != null || lineImpala != null)
			{
				if (lineHive == null)
				{
					outWriter.write("+Impala: " + lineImpala + "\n");
					this.rowsDifferent++;
					lineImpala = brImpala.readLine();
				} else if (lineImpala == null)
				{
					outWriter.write("+Hive: " + lineHive + "\n");
					this.rowsDifferent++;
					lineHive = brHive.readLine();
				}
				else
				{
					int c = lineHive.compareTo(lineImpala);
					if ( c == 0)
					{
						lineImpala = brImpala.readLine();
						lineHive = brHive.readLine();
					}
					else if (c < 0)
					{
						outWriter.write("+Hive: " + lineHive + "\n");
						this.rowsDifferent++;
						lineHive = brHive.readLine();
					}
					else if ( c > 0)
					{
						outWriter.write("+Impala: " + lineImpala + "\n");
						this.rowsDifferent++;
						lineImpala = brImpala.readLine();
					}
				}
			}
			outWriter.close();
			brImpala.close();
			brHive.close();
		}
		catch ( Exception e)
		{
			e.printStackTrace();
		}
		
			
	}
	

}
