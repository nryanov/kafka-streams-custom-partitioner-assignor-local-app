clean:
	mvn clean

jar:
	mvn package -DSkipTests

run:
	java -jar target/kafka-custom-partitioner-simple-java-app-1.0-SNAPSHOT-jar-with-dependencies.jar