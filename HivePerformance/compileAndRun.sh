javac -cp "/usr/hdp/2.2.4.0-2633/hadoop/*:/usr/hdp/2.2.4.0-2633/hive/lib/*:/usr/hdp/2.2.4.0-2633/hadoop-yarn/*" org/apache/hw/b
en/*.java
jar cvfm test.jar META-INF/MANIFEST.FM org
java -cp "/usr/hdp/2.2.4.0-2633/hive/lib/*:/usr/hdp/2.2.4.0-2633/hadoop/*:/usr/hdp/2.2.4.0-2633/hadoop-yarn/*:/usr/hdp/2.2.4.0-
2633/hadoop-yarn/lib/*::test.jar"  org.apache.hw.ben.HiveTestMain $1