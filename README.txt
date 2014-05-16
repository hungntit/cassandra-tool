 1. Resolve missing library.
 - To include apache-cassandra-1.2.16.jar into maven local repository. Issue following command : 
 mvn install:install-file -Dfile=libs/apache-cassandra-1.2.16.jar -DgroupId=org.apache.cassandra -DartifactId=apache-cassandra -Dversion=1.2.16 -Dpackaging=jar
 
 