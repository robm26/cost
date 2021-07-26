# Provisioned Capacity with Auto Scaling

## Intro

The [DynamoDB Pricing Page](https://aws.amazon.com/dynamodb/pricing/) describes the two
main capacity modes for DynamoDB tables.  It is recommended that new tables begin in On Demand
mode for a few weeks, in order to reduce the risk of throttling or overprovisioning 
until traffic patterns become clear.

While Provisioned Capacity may seem at first to be less than 20% the cost of On Demand,
achieving this with real live workloads is quite difficult.  Few workloads are flat or very
steady, and adjusting the provisioned capacity levels every minute as traffic levels 
 fluctuate is impractical.  Many customers keep a headroom buffer to allow surges in 
 traffic to be handled while adjustments to capacity take a couple of minutes to take effect.
 
DynamoDB Auto Scaling is an option for Provisioned Capacity whereby the system creates 
Cloudwatch alarms that automatically raise and lower table capacity levels according to 
certain thresholds.  

Take a look at the doc pages for 
[Provisioned Capacity](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html)
and [Provisioned Capacity with Auto Scaling](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.html)
for a refresher.

An overarching goal for many customers is to plan and execute a move from On Demand mode to 
Provisioned Capacity mode in order to save costs.  For the right workloads and with some 
planning, this move can result in a significant cost reduction without impacting the ability
of the table to handle changes in traffic.

## Scenario

We are database consultants for a large financial services firm. 

The company maintains a table that serves moderate levels of customer requests during 
the day, while at night the table sustains heavy write traffic in the form of batch jobs 
that are launched by internal departments in the company.

You have been tasked with understanding the capacity modes and making overall
recommendations on how to reduce costs with DynamoDB while maintaining good read and 
write performance, and without requiring manual intervention of table settings, 
 where possible.  The table is currently in On Demand mode.

The table has a small baseline level of traffic of about 200 WCU at all times.

To simplify the nightly write traffic scenario, consider one batch job that begins at 
midnight, loads three million items at a rate of 1000 per second, and with an item size 
of 2 KB.  This causes an extra 2000 WCU to be attempted by the batch job in the first 
minute and lasting about 50 minutes.

![Batch Job Traffic](https://dynamodb-images.s3.amazonaws.com/img/AutoScalingBatch700.png)


### Solution 1 ? 

With our knowledge of Provisioned Capacity, and familiarity with the Update-Table API, we
advise the other team to add features to the batch job to first raise the level of 
provisioned WCU to 2500 a few minutes before the job starts, and drop the levels back down 
to 250 once the job completes.  We fire off an email with these suggestions and are dismayed
when the response comes back: 

> Dear consultants,
> We are Python data wranglers, not DBAs.  We are not feeling good about performing update 
> table commands on your production table.  Besides, what if a some other big jobs are
> already running at midnight, how would we know the final write capacity to set,
> given the total traffic needed for other table customers as well?  
> 
> I read something about Auto Scaling, that seems like the best bet to make all our 
> lives easier, yeah?
> 

### Solution 2 !

We start thinking more about Auto Scaling as an option.  What we need is a simulator
to show us how Auto Scaling might behave with sudden batch jobs like the one we have.  

## AutoScaling cost and performance spreadsheet

Open the model spreadsheet in this folder : 
 * [DynamoDB-Cost-AutoScaling.xlsx](./DynamoDB-Cost-AutoScaling.xlsx)
 
This is a preliminary attempt at creating an Auto Scaling simulator model, and is by no means 
an official AWS tool.  The goal is to show the scale-up trigger acting according to the 
three boundary parameters of Min, Max, and Target Utilization. 

It looks at the latest two minutes of traffic for a potential scale up trigger. 
For scale-down, it will consider the latest 15 minutes of traffic before reducing capacity.
The time to complete changes to capacity, after requested, could add additional couple 
of minutes delay as well. 

![Auto Scaling Model](https://dynamodb-images.s3.amazonaws.com/img/AutoScalingModel.png)

## Steps

1. This model shows three intersecting lines.  Turn off everything but the requested traffic
by clearing the values of cells J5 and J6 found at the top right. 

1. This shows the batch job traffic shape.  Try changing the green numbers in 
B9, B10, and B11.  Return them to the defaults of 60,000  3,000,000 and 2.

1. Type a "Y" into cell J5 to display the estimated Auto Scaling behavior in blue.  
1. Type a "Y" into cell J6 to display the throttle risk in yellow.  This is the gap between 
requested and provisioned capacity, by minute.  You can see the raw model data by
scrolling down to the data table.  There is a serious risk of throttling happening.
DynamoDB can usually apply unused capacity from the past five minutes towards bursts
of up to five minutes, which should alleviate some of the throttle risk.  The AWS SDK will 
also retry any throttled requests automatically, about ten times with exponential backoff,
which may help somewhat but would be unlikely to succeed if there are sustained periods of
under-provisioning.

1. Adjust the Auto Scaling Min parameter in cell B20 to 1000 and then 2000. 
Does it reduce the duration of the throttle risk?  If we know heavy traffic is coming,
we can adjust the Min levels in advance, and remember to bring them back down once the 
write traffic subsides.  Many companies have windows from about midnight to 5pm where they 
can expect heavy write traffic.  However we still wish to have more automation and less 
manual intervention with our final strategy.

1. There is a Ramp Duration parameter at cell B12.  This represents a change in requested
traffic, such that the traffic starts slowly and gains momentum each minute in order to
allow Auto Scaling to have a chance to react and scale up in concert.  Change the ramp 
duration to 5, then 10, then 15.  Does the throttle risk go away?  We would need to convince
the batch job application team to adopt a ramp like this in order to support our goal of using
Provisioned Capacity with Auto Scaling.  

1. Notice the baseline WCU level in cell B24.  This is the background noise level of the table. 
Adjust it anywhere between 1 and 1000 to see the affect on Auto Scaling.  It seems Auto Scaling 
has a head start to scale up faster in an already busy table.  This may support the idea of 
overloading all your record types and all external customers into one big table, so that the
overall traffic pattern is more steady and less spiky.  With DynamoDB, excess capacity in one
part of the table flows to where it is needed the most.  


1. Simulate a short but sudden spike in traffic.  Set the following:
80,000 - 300,000 - 2 - 10 and set the Baseline WCU level in cell B24 to 20. You should
see a chart as shown.  Check out the estimated costs in yellow in B33 and B34, which is greater?  
For very spiky workloads, On Demand may be less expensive than Provisioned Capacity 
while reducing the risk of throttling.  On Demand effective scales back down to zero instantly
while Auto Scaling may take 15 or even 60 minutes to scale down.


![Auto Scaling Spike](https://dynamodb-images.s3.amazonaws.com/img/AutoScalingSpike.png)


Read more details on Auto Scaling in this blog post:

 * [Amazon DynamoDB auto scaling: Performance and cost optimization at any scale](https://aws.amazon.com/blogs/database/amazon-dynamodb-auto-scaling-performance-and-cost-optimization-at-any-scale/)


### Summary

Plan to use Provisioned Capacity when you can invest some time to understand traffic patterns, 
are comfortable changing capacity via the API.  Use Auto Scaling when your traffic is steady and preditcable, 
or when you can slowly ramp up sudden batch or bulk loading jobs, or when you can schedule jobs to run in 
a pre-defined window where capacity can be pre-provisioned.

Send a :100: emoji in the chat if you are doing
this workshop at a live event to let us know you are done.  💯

[Return Home](../README.md)