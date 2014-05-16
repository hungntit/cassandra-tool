 1. Resolve missing library.
 - To include apache-cassandra-1.2.x.jar into maven local repository. Issue following command : 
 mvn install:install-file -Dfile=libs/apache-cassandra-1.2.x.jar -DgroupId=org.apache.cassandra -DartifactId=apache-cassandra -Dversion=1.2.x -Dpackaging=jar
 
 2. Use json2sstable-fix tool:
   2.1. Build cassandra-tool-0.1.jar by command:
        mvn install
   2.2. Copy cassandra-tool-0.1.jar to $CASSANDRA_HOME/libs/
   2.3. Copy tools/json2sstable-fix to $CASSANDRA_HOME/bin/
3. Use convert-partitioner tool:
   3.1. Copy tools/convert-partitioner to $CASSANDRA_HOME/bin/
   
