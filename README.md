# Kafka Cluster Difference Tool
This application provides comparison kafka clusters based on data consuming per each Topic-Partition.

## How to install
For build application:

`gradle build`

or build without tests:

`gradle build -x test`


## Configuration

There are 2 manadatory property files:

`--backup-consumer.config` and `--source-consumer.config` are properties for Kafka consumers.

Optional parameters:

`--threads` property allows set number of threads to process clusters comparison;

`--poll-timeout-ms` Poll timeout for consumers;

`--buffer-size` Buffer size to compare data;

`--custom.properties` file which contains property:

`exclude-topics` allows set Topics which is not needed to validate. List of topics must be splitted by comma.

### Output results:
Logging configuration located in `resources/application.properties`

Edit this properties to change logging level or add logging to file


## Run Application:

`java -jar build/libs/kafka-diff-tool-0.1.0.jar [OPTIONS]` 

Example:

`java -jar build/libs/kafka-diff-tool-0.1.0.jar --backup-consumer.config <path> --source-consumer.config <path>`

