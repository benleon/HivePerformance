package org.apache.hw.ben;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveHelper {
	
	public Properties prop = null;
	
	public boolean verbose = false;
	
	public boolean writeResultSetToFile = false;
	
	public boolean writeActualSQL = false;
	
	public FileWriter outputWriter = null;
	
	public boolean randomize = false;
	
	public int refreshConnection = -1;
	
	public String resourceManager = null;
	
	public HiveHelper(Properties prop) {
		this.prop = prop;
		if (propertyTrue("verbose")) this.verbose = true;
		if (propertyTrue("writeResultSetToFile")) this.writeResultSetToFile = true;
		if (propertyTrue("randomize")) this.randomize = true;
		if (propertyTrue("writeSQLToFile")) this.writeActualSQL = true;
		this.resourceManager = prop.getProperty("resourceManager");
		this.refreshConnection = this.getNumberFromProperty("refreshConnection", -1);
	}
	
	public void testResult(ResultSet rs, HiveTestQuery query)
	{
		String inFile = query.fileLocation;
		
		Date before = new Date();
		File outFile = null;
		
		if (writeResultSetToFile)
		{
			try {
				String ending = "";
				if (query.queryIdentification != null)
				{
					for ( String s : query.queryIdentification)
					{
						ending = ending + "-" + s ;
					}
					
				}
				ending = ending + ".result";
				
				outFile = new File(HiveHelper.replaceEnding(inFile, ending));
				outFile.delete();
				outFile.createNewFile();

				outputWriter = new FileWriter (outFile);
				if (this.writeActualSQL)
				{
					outputWriter.write("Query:\n" );
					outputWriter.write(query.queryString + "\n");
					outputWriter.write("\nResults:\n");
				}
			} catch (Exception e) {
				System.out.println("Couldn't create output File: " + outFile);
				e.printStackTrace();
			} 
		}

		try {
			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();
			int numRows = 0;
			while (rs.next())
			{
				numRows++;
				
				if (writeResultSetToFile)
				{
					StringBuffer buf = new StringBuffer();
					for (int i = 1; i <= cols; i++)
					{
						buf.append ( rs.getString(i)); 
						if ( i < cols ) buf.append ( "\t");
					}
					buf.append("\n");
					if (outputWriter != null)
					{
						outputWriter.write(buf.toString());
					}
				}
				
			}
			query.rowsReturned = numRows;
			Date after = new Date();
			query.resultsFetched = after;
			DecimalFormat dec = new DecimalFormat("#.##");
			double fetchTime = (after.getTime() - before.getTime()) / 1000.0;
			String summary = "Query: " + query + " returned " + numRows + " rows, fetching them took " + dec.format(fetchTime) + " seconds ";
			if (this.verbose)
			{
				System.out.println(summary);
			}
			if (writeResultSetToFile)
			{
				outputWriter.write(summary);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (writeResultSetToFile)
				{
					this.outputWriter.close();
				}
				//rs.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		
	}
	
	public static double getDifference (Date before, Date after)
	{
		return (after.getTime() - before.getTime()) / 1000.0;
	}

	public boolean propertyTrue(String name)
	{
		if (prop.getProperty(name) == null) return false;
		if (prop.getProperty(name).toLowerCase().equals("false")) return false;
		return true;

	}
	
	public boolean propertyEquals(String name, String value)
	{
		if (prop.getProperty(name) == null) return false;
		if (prop.getProperty(name).toLowerCase().equals(value.toLowerCase())) return true;
		return false;

	}
	
	public int getNumberFromProperty(String name, int def)
	{
		if (prop.getProperty(name) == null)
		{
			return def;
		}
		else
		{
			try 
			{
				Integer in = new Integer(prop.getProperty(name));
				return in.intValue();
			}
			catch ( Exception e)
			{
				
			}
			return def;
		}
	}
	
	public void writeError(HiveTestQuery query, SQLException sqlEx)
	{
		query.queryFailed = true;
		String inFile = query.fileLocation;
		File outFile = null;
		
		if (writeResultSetToFile)
		{
			try {
				outFile = new File(HiveHelper.replaceEnding(inFile, ".result"));
				outFile.delete();
				outFile.createNewFile();

				outputWriter = new FileWriter (outFile);
				outputWriter.write(sqlEx.getMessage());
				outputWriter.close();
				
			} catch (Exception e) {
				System.out.println("Couldn't create output File: " + outFile);
				e.printStackTrace();
			} 
		
		}
	}
	
	public String testFolder()
	{
		return prop.getProperty("testFolder");
	}
	
	
	/**
	 * replaces the ending with a new one. for example ben.sql becomes ben.out
	 * @param file
	 * @param newEnding
	 * @return
	 */
	public static String replaceEnding(String file, String newEnding)
	{
		int indexLastDot = file.lastIndexOf(".");
		return file.substring(0, indexLastDot) + newEnding;
	}
	
	
	public List<String> getListProperty(String propName)
	{
		//ArrayList<String> list = new ArrayList<String>();
		String propString = this.prop.getProperty(propName);
		if (propString == null || propString.equals("")) return null;
		String[] parts = propString.split(",");
		return Arrays.asList(parts);
	}
	
	public HashMap<String,String> getReplacements()
	{
		HashMap<String, String> replacements =new HashMap<String,String>();
		if (prop.getProperty("replace") != null)
		{
			String rep = prop.getProperty("replace");
			String[] repstrings = rep.split(";");
			for ( int i = 0; i < repstrings.length; i++)
			{
				String[] repPair = repstrings[i].split(":");
				if (repPair.length > 1)
				{
					replacements.put(repPair[0], repPair[1]);
				}
			}
		}
		return replacements;
	}
	
	public String getWebPage(String page) {
	    URL url;
	    InputStream is = null;
	    BufferedReader br;
	    String line;
	    StringBuffer buf = new StringBuffer();

	    try {
	        url = new URL(page);
	        is = url.openStream();  // throws an IOException
	        br = new BufferedReader(new InputStreamReader(is));

	        while ((line = br.readLine()) != null) {
	        	buf.append(line);
	        	buf.append("\n");
	        }
	        return buf.toString();
	    } catch (MalformedURLException mue) {
	         mue.printStackTrace();
	    } catch (IOException ioe) {
	         ioe.printStackTrace();
	    } finally {
	        try {
	            if (is != null) is.close();
	        } catch (IOException ioe) {
	            // nothing to see here
	        }
	    }
	    return null;
	}
	
	/**
	 * allows to return the content of an HTML tag and allows optionally to provide an attribute value
	 * in the tag. Note this is pretty basic it will only look for the attribute string in the opening tag
	 */
	public ArrayList<String> getDataInTag(String text, String tag, String attribute)
	{
		//String regexString = <tbody[^>]*ui-widget-content[^>]>([\s\S]*?)</tbody>
		String regexString = "<" + tag + "[^>]*" + attribute + "[^>]*>([\\s\\S]*?)</" + tag + ">";
		ArrayList<String> ret = new ArrayList<String>();
		
		Pattern pattern = Pattern.compile(regexString);

        Matcher matcher = pattern.matcher(text);
        while ( matcher.find())
        {
    		//ret.add(matcher.group(0));
    		//System.out.println("groups " + matcher.groupCount());
        	if (matcher.groupCount() > 0)
        	{
        		ret.add(matcher.group(1));
        	}
        }
        return ret;
	}
	
	public ArrayList<String> getTags(String rm)
	{
		ArrayList<String> ret = new ArrayList<String>();
		String html = this.getWebPage(rm);
		if  ( html == null ) return ret;
		ArrayList<String> body = this.getDataInTag(html, "tbody", "ui-widget-content");
		if ((body != null) && (body.size() > 0 ))
		{
			return getDataInTag(body.get(0), "td", "");
		}
		return ret;
	
	}
	
	
	
}
