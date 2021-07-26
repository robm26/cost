# Filter or Global Secondary Index ?


## Intro
Locating a record in a table by primary key is a core function of any database.  
The table maintains an index data structure that ensures uniqueness of items 
and allows fast and efficient lookup of the item even for very large tables.

DynamoDB uses a Partition Key attribute to enable key-value access via Put-Item and Get-Item.

With an optional Sort Key defined, the primary key is now a composite key, 
using both keys for key-value access, or just the Partition Key for 1:many requests via Query. 
Both Get-Item and Query are very efficient in their cost as they use the base table's index to 
locate just the item(s) needed.

All other attributes in the item are optional.  To retrieve an item or items by an attribute 
that is not part of the key schema, the only strategy is to perform a full table scan and 
then ignore any non-matching items. 
[DynamoDB can do this for you](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.FilterExpression) but it will require every
item in the table to be read, at a much larger cost that is proportional to the size of the table.

As an alternative, we might create a Global Secondary Index, or GSI, on the attribute 
which would allow for much more efficient searches by the attribute.  
The drawback of a GSI is that it replicates changes to the base table, 
so writes will be amplified and the total cost of writes could be doubled. 
Developers new to DynamoDB often hear "avoid scans, use a GSI instead".


## Scenario

We are database consultants maintaining a table of sales orders in DynamoDB. 

 * The table has 50 million items
 * Each item is 4 KB in size
 * The table receives writes at a rate of 20,000 items per hour
 
The customer would like you to produce a list of Car products and their price.
This list should be generated once per day.


| PartitionKey    | SortKey      | Product | Price | Serial Number | Status | Contract | 
| -----------     | -----------  | -----   | ----- | ----- | ----- | ----- |
| Customer111     | 2021-07-26   | Car   | 24000 | SN-774ac494 | DELIVERED | {JSON} |
| Customer111     | 2021-07-27   | Truck | 29000 | SN-8295cd0f | DELIVERED | {JSON} |
| Customer222     | 2021-07-26   | Car   | 23000 | SN-480239b4 | ORDERED   | {JSON} |
| Customer222     | 2021-07-29   | Bike  | 900   | SN-b39f5998 | QUOTED    | {JSON} |


## Cost Model Spreadsheet

Open the Excel worksheet in this folder, [DynamoDB-Cost-Filter-GSI.xlsx](./DynamoDB-Cost-Filter-GSI.xlsx)
You should see a cost model like this.  The green numbers in column C can be changed as desired, 
and the chart should automatically update. 


![Excel Screenshot](https://dynamodb-images.s3.amazonaws.com/img/fgsi01.png)

## Steps

1. Select the five cells in column C, from C7 to C11, and press the delete key to clear their values.
1. Click into the first cell, C7, which is next to Table Size.  Enter 50,000,000.
1. Click into the next cell below, C8, and enter 4 for the Average Item Size.
Notice we now know the size our table!  The model informs us the table is 191 GB in size.  
1. Scroll down to the blue cell at C36.  This is the cost to scan the entire table one time.
(The raw data in this section are calculated by Excel expressions and auto-fill.) 
1. Click on the last blue cell, C65.  This is the cost to scan the table 30 times: $187.50.

We now have a starting point for estimating the cost of any search with Scan.
Shall we inform the customer of the cost and code up the Scan + Filter function?
Not yet! Let's go further and model in a GSI and see if it would be cost effective.

6. Click on cell C9, the GSI Replication parameter.  Enter 100%.  
We put in 100% because generally a GSI replicates a copy of ALL the items in the base table,
just grouped differently.  Assume the GSI will be defined with GSI Partition Key of Product so that we 
can query the GSI for ```Product=Car```.
6. Notice the GSI cost has jumped up to $47.68.  This is just the storage cost for the GSI, 
as seen in the data table in orange.
6. Let's add in the expected write traffic to the base table. Click on cell C10 and enter 
20,000 for Table Writes per hour.  Notice the GSI total cost jumps again, to $119.68.
6. Finally, let's enter 5000 for Items Returned by Search in cell C11.  
The cost to query for 5000 items it just a few pennies per month as shown in the data table.

### Evaluate options 🔎

Which strategy works best for the customer?  Based on the chart, find where the two lines 
intersect.  This is where the models have equivalent total monthly cost. It looks like 
performing 18 searches or fewer per month would be less expensive with a Scan + Filter
strategy.  But performing 30 searches per month as they want is best done with a GSI. 

We estimated the number of items returned by the GSI Query to be 5000.  We would need to 
know a bit about the statistical probability of ```Product=Car``` appearing in the table
to make a good estimate here.  What if instead we learn there are five million such records
in the base table?  Go ahead and update C11 to 5,000,000.  Which option looks best now?

Revert the Items Returned back to 5000 before continuing. 

As developers we are provided the Scanned and Returned Item Count along with the results 
of any read operation.  It could be worth logging these to gauge how many items are 
affected by different searches. 


### Optimize the GSI 

Can we reduce the costs further for the customer?  A GSI by default will replicate every 
attribute from the base table.  The customer did not request a list with 
Serial Number, Status, and Contract attributes.  We could drop and re-create the GSI with 
selective projection of just attributes Product and Price.  This way the GSI will include 
only these attributes plus the original Partition Key and Sort Key of the base table.
This could reduce the size of the GSI by a lot.  

10. Change the GSI Replication Percentage value from 100 to 50.  

If we know the Status column is updated very often, and we are now not tracking that in 
the GSI, we can assume the GSI Replication Percentage is even less.  GSI Replication
Percentage here represents a rough gauge of storage and write costs as compared to the base
table. 

11. Change the GSI Replication Percentage value from 50 to 25.  

Finally, if we know the customer only wants the daily Car-Price listing, and they will not
need a similar listing for Trucks or Bikes, we could stop storing the values Truck and Bike 
in the table going forward.  When a GSI indexes on values that are sometimes empty or NULL
causes the GSI size and write consumption to shrink even more.  This is the Sparse Index pattern.


While it might seem extreme to stop recording the product type here for Trucks and Bikes, 
we could instead leave that attribute as-is, and begin writing a new attribute called 
CarsOnly that will be null for everything except for Cars.  We are essentially duplicating 
the Product attribute for a special set of items.  We don't worry much about the small 
extra cost of storage in this case.  Scans and Filters will take the total size of the 
result set before dividing by the 4k RCU size to calculate read consumption.  We can
drop and re-create GSIs at any time, so we will create a GSI on the sparse CarsOnly attribute.

12. Change the GSI Replication Percentage value from 25 to 1.  

### Summary

Congratulations! You have solved the customer's requirement to generate a list of Car items
and prices, and tuned the GSI to be sparse. The total monthly cost for the GSI and query is
now just $1.27!  It is time to celebrate. Send a :thumbsup: emoji in the chat if you are doing
this workshop at a live event.  👍


[Return Home](../README.md)
