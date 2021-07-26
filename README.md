# Amazon DynamoDB Cost Modeling Workshop 
![DynamoDB](https://dynamodb-images.s3.amazonaws.com/img/ddb-logo-100.png) 
![Lamp](https://dynamodb-images.s3.amazonaws.com/img/green-lamp-100.png)

Welcome! Here you will find a collection of resources
that will help you use Amazon DynamoDB in a cost efficient way.


## Intro
Amazon DynamoDB is a powerful serverless database that offers virtually unlimited scale 
when used efficiently. 

As a fully managed service, DynamoDB is essentially a data API in the cloud. 
Reads, writes, and storage requests are automatically distributed across a fleet
of many thousands of internal servers to support extreme scale.
Creating a new table is easy, you just need to define the table's primary key and 
accept the default capacity settings. With an understanding of a few core 
read and write API calls, your application is up and running.  The default capacity mode, 
called On Demand, has simple pay-per-request pricing with the ability to handle 
unpredictable workloads.  The table structure, data access patterns, and provisioning decisions 
you can make will have a big impact on the performance and cost efficiency of the solution.


## Designing for cost
A common question heard from from customers is, "How much will it cost to run my application on DynamoDB?" 
Experience with managing a traditional SQL database does not always apply when making the move to DynamoDB. 
Customers immediately recoup the costs of undifferentiated heavy lifting, such as the labor of 
managing server instances 24x7, not to mention the task of sizing the CPUs, memory, 
and maintaining the number of instances required. 
DynamoDB offers several enterprise features that customers can leverage, 
such as continuous backups, export to Amazon S3, change data capture, 
and multi-region replication, all of which would affect the total cost of operation.  

To estimate the cost of running an application on DynamoDB, first take a moment to review the
[DynamoDB Pricing Page](https://aws.amazon.com/dynamodb/pricing/).  

Early in the design process, it is important to capture and confirm the list of read and write 
access patterns needed.  A sample access patterns template is [provided here](https://dynamodb-images.s3.amazonaws.com/img/DynamoDB+Data+Access+Patterns.xlsx).
Be sure to estimate the item size, traffic levels and expected peak velocities as well.


Two topics stand out as opportunities for maximizing the efficiency of DynamoDB, that we will cover in the workshop.

The first is deciding when and how to use Global Secondary Indexes, or GSIs, to support data searching.
The second is deciding whether and how to run your workload with the 
lowest cost capacity mode, which is often Provisioned Capacity 
as compared to the default On Demand mode.  


### Pre-requisites
This workshop assumes you have some familiarity with AWS and DynamoDB, 
for example as a developer using the [AWS SDKs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.html),
or as a designer using the [NoSQL Workbench](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.html),
or perhaps as a cloud DBA who monitors [DynamoDB Metrics in Cloudwatch](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/monitoring-cloudwatch.html).

The exercises use only Excel spreadsheets to simulate the impact of design decisions.

-----

### Modules

1. **Search strategies** - [Filter versus Global Secondary Index](./FilterOrGSI/README.md)
2. **Capacity decisions** - [On Demand versus Provisioned with Auto Scaling](./AutoScaling/README.md)


### Extra Credit
 
 * Profile the cost of each DynamoDB request with Return Consumed Capacity: [Efficiency Demos](https://github.com/robm26/efficiencydemos)

