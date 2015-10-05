!!! Rough Performance Test suite for Hive/Phoenix !!! 


README for test suite

Testsuite to execute a set of Hive queries

Execution: 

Run jar file
java -jar ... <config file>

NOTE: in this setup compilation happens on the server, added scripts compileAndRunXXX.sh which compiles the tool and executes it. Each of them has one config script associated with it that configures the queries, target servers etc.
- compileAndRun.sh also takes config file as parameter now so ./compileAndRun.ssh hivetest25.conf is equivalent

if no config file is specified the tool looks for hivetest.conf in the execution folder


Configuration:

All configuration is driven by one property file containing the test information

A) Connection Info:
Properties: host,port,database,username,password
Defaults if not specified: localhost,10000,default,hive,password

Specifying multiple Hive server:
Property numberConnections=x
Plus hostx,portx, ... 

Connections are alterated between Execution threads. If you have 3 thread and 2 connections:
Thread1: con1
Thread2: con2
Thread3: con1

For example to connect to three Hive servers with db tpch, with default ports and usernames 
numberConnections=3
host=sandbox1
database=tpch
host1=sandbox2
host2=sandbox2

B) Query Input 

testFolder: folder containing sub folders with queries. Can be one of multiple.
	The tool will look for all .sql files in the subfolders. 
queriesPerFolder : Property for the old Yahoo Japan test suite, takes only x queries per folder
	sorting query names by queryname.x.sql ( numerical sorting ) 
multiplyQueries : Increasing number of queries by running the set multiple times. 
randomize : randomizes order to rule out any dependencies between queries

	
B) Query Execution

poolExecution: true means that all queries are executed as one pool. Execution threads take
	queries from pool. 
numberThreads: number of execution threads. 
	Only relevant for poolExecution otherwise one thread per folder
refreshConnection: resets the Hive connection for a thread after x queries
removeATS: Stops Hive from connecting to Timeline Server
resourceManager: Connects to ressource manager website to read utilization ( containers used, memory ... )
killYarnApps: kills all applications running in the cluster, need a yarn-site.xml in the same folder
replace: replaces Strings in the queries to for example change table names format oldvalue1:newvalue1;oldvalue2:newvalue2;...

Dynamic Queries:
Queries can have wildcards that are read from file.

parameter testDataFile: location of test data file with format 
column1Name|column2Name|..
value11|value21...
value12|value22...

If test data file is encountered tool will try to replace the following strings in each query and replace them
with the values from the testdatafile. First query will have values from row1, and so on.
Wildcard in query needs to be of format ${Value:column1Name} and will be replaced for the first query with value11 and for the second query with value12

C) Writing results

The testsuite writes basic information to the command line. Additional information is 
written to files. 

verbose: Writes additional information to sys out. 
writeResultSetToFile : writes fetched result sets to files with ending queryname.result
checkSpeed: Writes a summary line every x seconds to the sysout
readImpalaValues : expects Impala output files with the same name of the query file and ending .out 
outputSQLFiles: Writes a single Query file, one thread at a time for execution with beeline, hive cli, ...
diffImpala: runs a comparison of hive and impala results at the end, writes a file resultsDiff into the test folder

D) Created Output

Writes a status line every "checksped" seconds, Writes a summary line at the end. 

Also writes three files into the rashtest folder:
- query... : CSV of all queries, execution time, execution errors, rows returned, rows different than Impala, ...
- stats..  : CSV for the runtime statistics over time ( similar to the status line )
- threads  : CSV for all threads and their performance characteristics

Also writes the following file in the folder of the query:
- <queryname>.result (result data )
- <queryname>.resultHiveSorted ( result data sorted )
- <queryname>.resultImpalaSorted ( result data of Impala assuming it was in <queryname>.out sorted and in Hive format )
- <queryname>.resultDiff ( any differences in rows between resultHiveSorted and resultImpalaSorted with + notation )


