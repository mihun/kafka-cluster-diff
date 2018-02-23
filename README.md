# Kafka Cluster Difference Tool
This application provides comparison kafka clusters based on data consuming per each Topic-Partition.

## Configuration
It is required to set mandatory custom properties in `resources/custom.properties` before run application.

There are next parameters:

`target_host` and `source_host` are consumer `bootstrap.servers` property for target and source clusters respectively

for example `source_host=localhost:9092`

`threads` property allows set number of threads to process clusters comparison.

`exclude_topics` allows set Topics which is not needed to validate. List of topics must be splitted by comma.

### Output results:
Logging configuration located in `resources/application.properties`

Edit this properties to change logging level or add logging to file

## Getting Started
For build application:

`gradle build`

or build without tests:

`gradle build -x test`

Run Application:

`java -jar build/libs/kafka-cluster-diff-0.1.0.jar`
