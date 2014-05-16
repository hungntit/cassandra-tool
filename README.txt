 1. Resolve missing library.
 - To include apache-cassandra-1.2.x.jar into maven local repository. Issue following command : 
 mvn install:install-file -Dfile=libs/apache-cassandra-1.2.x.jar -DgroupId=org.apache.cassandra -DartifactId=apache-cassandra -Dversion=1.2.x -Dpackaging=jar
 
 
