# ItemGenerator

This is a Maven project:
https://maven.apache.org/install.html

Follow directions at link above to install Maven and build project with following command line:

mvn clean package shade:shade

Execute the jar as follows:

java -jar ItemGenerator.jar -t tableName -n numTransactions -s numSecurities -h targetHostName -l [listenerFlag]

The jar expects default AWS credentials to be configured. 
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-methods

You must have the following tables deployed in your account, substitute "tableName" for whatever you wish to use:<br>
tableName<br>
tableName_S

Both tables must be configured for global replication.

Execute the jar on two EC2 instances deployed in the source and target GT replication regions for the tables. Both EC2 instances must be able to accept network traffic from each other on TCP port 8080.
 
