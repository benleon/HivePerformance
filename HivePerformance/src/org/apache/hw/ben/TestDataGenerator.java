package org.apache.hw.ben;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * This class takes a query with wildcards and replaces them with valid values
 * It has the option to read values from a file or generate them randomly based on the 
 * wildcards.
 * 
 * @author bleonhardi
 *
 */
public class TestDataGenerator {
	
	public String testFile = null;
	
	// list of values. 
	public ArrayList<HashMap<String,String>> replacementValues = null;
	
	int currentItem = 0;
	
	String[] columnNames = null;
	
	public TestDataGenerator ( String inputFile )
	{
		this.testFile = inputFile;
		if ( testFile != null ) this.readFile(inputFile);
		
		
	}
	
//	public void randomize()
//	{
//		Collections.shuffle(this.replacementValues);
//	}
	
	public void next()
	{
		if ((replacementValues == null) || ( replacementValues.size() == 0)) return;
		currentItem ++;
		if (currentItem >= replacementValues.size()) currentItem = 0;
	}
	
	
	
	public  String getCurrentValue(String columnName)
	{
		if ((replacementValues == null) || ( replacementValues.size() == 0)) return null;
		HashMap<String,String> currentRow = this.replacementValues.get(currentItem);
		if (currentRow.containsKey(columnName))
		{
			return currentRow.get(columnName);
		}
		else
		{
			System.out.println("Test Data Generator was asked for column he doesn't have " + columnName);
		
		}
		return null;
	}
	
	
	public void readFile(String inputFile )
	{
		
		BufferedReader in = null;
		try {
			FileInputStream testDataFile = new FileInputStream(inputFile);
			
			in = new BufferedReader(new InputStreamReader(testDataFile));

			String line = null;
			
		//	String[] columns = null;
			
			String columnsString =  in.readLine();
			
			if (columnsString == null) 
			{
				System.out.println("test data file didn't have column header");
				return;
			}
			
			columnNames = columnsString.split("[|]");
			System.out.println("Reading Test Data File with columns: " + Arrays.toString(columnNames));
			this.replacementValues = new ArrayList<HashMap<String,String>>();
			while ((line = in.readLine()) != null)
			{
				String[] values = line.split("[|]");
				if (values.length != columnNames.length) 
				{
					System.out.println("Couldn't read test data files, row had different number of records than header " + line);
					return;
				}
				HashMap<String,String>  row= new HashMap<String,String>();
				for ( int i = 0; i < values.length; i++) 
				{

					row.put(columnNames[i], values[i]);
				}
				replacementValues.add(row);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Couldn't read test data file + inputFile");
			e.printStackTrace();
		}
		finally
		{
			try {
				if (in != null) in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/** This method replaces all wildcards in the query with actual values
	 * 
	 * @param queryString
	 * @return updated Query String
	 */
	public String replaceQuery(String queryString)
	{
		// We will try to replace any occurence of ${Value:columnName} with the 
		// actual value from the current row. 
		if (columnNames != null ) 
		{
			for ( int i = 0; i < columnNames.length; i++)
			{
				String wildcard = "\\$\\{Value:" + columnNames[i] + "\\}";
				queryString = queryString.replaceAll(wildcard, this.getCurrentValue(columnNames[i]));
			}
		}
		
		
		this.next();
		return queryString;
		
		
	}
	
	

}
