#### avro-tools
http://archive.apache.org/dist/avro/avro-1.9.2/java/avro-tools-1.9.2.jar

brew install parquet-tools

#### usage
#mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hadoop-aws -Dversion=3.3.2-dlink-3  -Dpackaging=jar -Dfile=target/hadoop-aws-3.3.2.jar
- java -jar avro-tools-1.9.2.jar tojson *.avro
- parquet-tools meta
- parquet-tools cat -j 