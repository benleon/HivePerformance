package org.apache.hw.ben;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

public class QuerySet {
	
	public String folder;
	public ArrayList<ArrayList<HiveTestQuery>> folders;
	public String[] queryFolders;
	
	public ArrayList<HiveTestQuery> allQueries = new ArrayList<HiveTestQuery>();
	
	public int currentQuery = 0;
	
	public HiveHelper helper = null;
	
	public QuerySet(HiveHelper helper) {
		this.folder = helper.testFolder();
		this.helper = helper;
		queryFolders = getSubFolders(folder);
		if (queryFolders == null) 
		{
			System.out.println("No Query folders found in " + folder);
		}
		else
		{
			folders = new ArrayList<ArrayList<HiveTestQuery>>();
			for (int i = 0; i < queryFolders.length; i++)
			{
				folders.add(getSQLQueries(folder + "/" + queryFolders[i]));
			}
			
		}
		sortQueries();
		
		
		
		
		if (HiveTestMain.hiveTestProperties.getProperty("queriesPerFolder") != null)
		{
			cutQueries();
		}
		
		
		
		// now I add all queries to a single queue in case the nextQuery functionality is
		// used
		
		for ( int i = 0; i < folders.size(); i++ )
		{
			for ( int j = 0; j < folders.get(i).size(); j++ )
			{
				
				this.allQueries.add(folders.get(i).get(j));
			}
		}
		
		if (helper.randomize) Collections.shuffle(allQueries);

		
		

		//multiply
		int multiply = helper.getNumberFromProperty("multiplyQueries", -1);
		if (multiply > 1)
		{
			int numQueries = allQueries.size();
			for ( int i = 0; i < multiply -1; i++)
			{
				
				for ( int j = 0; j < numQueries; j++)
				{
					allQueries.add(allQueries.get(j).clone());
				}
			}
		}
		
		replaceValues();
		
		if (HiveTestMain.hiveTestProperties.getProperty("testDataFile") != null)
		{
			// if a testDataFile has been specified we will use it to replace 
			// values in the queries ( like IOID numbers ) 
			TestDataGenerator tdg = new TestDataGenerator(HiveTestMain.hiveTestProperties.getProperty("testDataFile"));
			for (int i = 0; i < this.allQueries.size();  i++ )
			{
				allQueries.get(i).queryString = tdg.replaceQuery(allQueries.get(i).queryString);
			}
		}
		
		
		if ( helper.verbose) printQueries();
		
		if ( helper.prop.getProperty("outputSQLFiles") != null)
		{
			outputSQLFiles(helper.prop.getProperty("outputSQLFiles"));
		}
		
	}
	
	public void outputSQLFiles(String folder)
	{
		try {
			int threads = helper.getNumberFromProperty("numberThreads", 1);
			ArrayList<File> files = new ArrayList<File>();
			ArrayList<FileWriter> fileWriter = new ArrayList<FileWriter>();
			for ( int i = 0; i < threads; i++)
			{
				File f = new File(folder + "/queries" + i + ".sql");
				f.delete();
				f.createNewFile();
				
				FileWriter w = new FileWriter (f);
				files.add(f);
				fileWriter.add(w);
			}
			
			for (int i = 0; i < allQueries.size(); i++)
			{
				int writerIndex = i % fileWriter.size();
				fileWriter.get(writerIndex).write(allQueries.get(i).queryString);
				fileWriter.get(writerIndex).write(";\n");
			}
			
			for (int i = 0; i < threads; i++)
			{
				fileWriter.get(i).close();
				
			}
		}
		catch ( Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	public void cutQueries()
	{
		try
		{
			int cut = new Integer ( HiveTestMain.hiveTestProperties.getProperty("queriesPerFolder")).intValue();
			System.out.println("cut" + cut);
			for ( int i = 0; i < folders.size(); i++)
			{
				if (folders.get(i).size() > cut)
				folders.set(i, new ArrayList(folders.get(i).subList(0, cut)));
			}
		}
		catch ( Exception e)
		{
			
		}
	};
	
	
	public void replaceValues()
	{
		Map<String, String> mp = helper.getReplacements();
		for (HiveTestQuery q : allQueries ) 
		{
			 q.replaceValues(mp);
		}
	}
	
	
	public void printQueries()
	{
		for ( int i = 0; i < folders.size(); i++)
		{
			System.out.println(queryFolders[i]);
			for (int j = 0; j < folders.get(i).size(); j++)
			{
				System.out.print("\t");
				System.out.print(folders.get(i).get(j).fileLocation);
				System.out.print("\t");
				System.out.println(folders.get(i).get(j).queryString);
			}
		}
	}
	
	/** 
	 * Method sorts queries based on the format query.x.sql 
	 * and integer sorting if filename doesn't contain .x.sql lexicographical
	 * sorting is used
	 */
	public void sortQueries()
	{
		for (int i = 0; i < folders.size(); i ++)
		{
			Collections.sort(folders.get(i), new Comparator<HiveTestQuery>() {

						@Override
								public int compare(HiveTestQuery o1, HiveTestQuery o2) {
									int num1 = getQueryNumber(o1.fileLocation);
									int num2 = getQueryNumber(o2.fileLocation);
							//		System.out.println("comparison" + num1 + " " + num2);
									if (num1 == -1 || num2 == -1)
									{

										return o1.fileLocation.compareTo(o2.fileLocation);
									}
									else
									{

										Integer i1 = new Integer(num1);
										Integer i2 = new Integer(num2);
										return i1.compareTo(i2);
									}
								
								}
					});
		}
	}
	
	public Integer getQueryNumber(String query)
	{
		String[] pieces = query.split("[\\.]");
		if (pieces.length > 2)
		{
			Integer val = null;
			try {
				val = new Integer(pieces[pieces.length -2]);
			}
			catch ( Exception e)
			{}
			if (val != null)
			{
				return val.intValue();
			}
		}
		return -1;
	}
	
	
	public ArrayList<HiveTestQuery> getSQLQueries(String queryFolder)
	{
		File file = new File(queryFolder);
		//System.out.println("Print files in folder "+ queryFolder);
		String[] sqlFiles = file.list(new FilenameFilter() {
		  
	            public boolean accept(File dir, String name) {
	               if(name.lastIndexOf('.')>0)
	               {
	                  // get last index for '.' char
	                  int lastIndex = name.lastIndexOf('.');
	               //   System.out.println("name" + name);
	                  // get extension
	                  String str = name.substring(lastIndex);
	               //   System.out.println("ext" + str);

	                  // match path name extension
	                  if(str.equals(".sql"))
	                  {
	                     return true;
	                  }
	               }
	               return false;
	            }
	         
		});
		if (sqlFiles == null)
		{
			return new ArrayList<HiveTestQuery>();
		}
		else
		{
			ArrayList<HiveTestQuery> queries = new ArrayList<HiveTestQuery>();
			for ( int i = 0; i < sqlFiles.length; i++ )
			{
				HiveTestQuery query = new HiveTestQuery(queryFolder + "/"+ sqlFiles[i]);
				queries.add(query);
			}
			return queries;
		}
		
	}
	
	
	public String[] getSubFolders ( String folder )
	{
		File file = new File(folder);
		String[] directories = file.list(new FilenameFilter() {
		  @Override
		  public boolean accept(File current, String name) {
		    return new File(current, name).isDirectory();
		  }
		});
		return directories;
	}
	
	public synchronized HiveTestQuery getNextQuery()
	{
		
		if (this.currentQuery < this.allQueries.size())
		{
			this.currentQuery ++;
			return this.allQueries.get(this.currentQuery -1);
		}
		return null;
	}
	
	public int percentageFinished()
	{
		return (int)(this.currentQuery / (double)allQueries.size() * 100.0) ;
	}

}
