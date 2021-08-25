var AWS = require("aws-sdk");

// instantiate a document client for DynamoDB access
var docClient = new AWS.DynamoDB.DocumentClient({maxRetries: 20, httpOptions: {connectTimeout: 500}}),
    appRegion = "us-west-2",
    sk = "A",
    table = "data_s";
    
// instantiate a Kinesis Firehose client for sending data to S3
var firehose = new AWS.Firehose();

AWS.config.update({
    region: appRegion,
    endpoint: "http://localhost:8000"
});

// handle zero padding for dates
function addZeros(n) {
    "use strict";
    if (n <= 9) {
        return "0" + n;
    }
    return n;
}

// do the work
exports.handler = async function (event, context, callback) {
    "use strict";
    // declare misc vars
    var vals = {},
        date = new Date(),
        curDay = date.getFullYear() + "" + addZeros(date.getMonth() + 1) + "" + addZeros(date.getDate()),
        nextDay,
        exp,
        query = {},
        types = ["buy", "sell"],
        processor = "StreamProcessor" + Math.floor(Math.random() * 10);
        
    var params = { DeliveryStreamName: processor, Records: [] };
    
    // sum items in this batch by security
    event.Records.forEach(function (record) {
        
        // skip anything that is not an insert
        if (record.dynamodb.hasOwnProperty("OldImage") != true && record.dynamodb.NewImage.region.S == appRegion) {
            
            // skip anything that is not a transaction
            if (types.includes(record.dynamodb.NewImage.type.S)) {
                // get the values from the trade record
                var price = parseFloat(record.dynamodb.NewImage.price.N), 
                    shares = parseInt(record.dynamodb.NewImage.shares.N), 
                    type = record.dynamodb.NewImage.type.S,
                    region = record.dynamodb.NewImage.region.S,
                    security = record.dynamodb.NewImage.PK.S.split("#")[0],
                    timestamp = record.dynamodb.NewImage.timestamp.S;
                    
                
                // add a summary item if this is the first trade for this security
                if (vals[security] == undefined) {
                    vals[security] = {"buyShares": 0, "sellShares": 0, "buyTotal": 0, "sellTotal": 0, "buyOrders": 0, "sellOrders": 0};
                }
        
                // add this trade to the price/volume summary by type for this security.
                switch(type) {
                    case "buy": 
                        vals[security].buyShares += shares;
                        vals[security].buyTotal += price * shares;
                        vals[security].buyOrders += 1;
                        break;
                        
                    case "sell":
                        vals[security].sellShares += shares;
                        vals[security].sellTotal += price * shares;
                        vals[security].sellOrders += 1;
                        break;   
                }       
                
                // push the record onto the Records array for a firehose write
                var trade = {"security": security, "timestamp": timestamp, "type": type, "shares": shares, "price": price, "region": region};
                params.Records.push({Data: JSON.stringify(trade)});
            }
        }
    });
    
    // if there are any records processed send the copies to firehose
    var promises = [];
    
    if (params.Records != null && params.Records.length > 0) {
        const putRecords = (config) => {
            return new Promise((resolve, reject) => {
                firehose.putRecordBatch(config, function(err, data) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(data);
                });
            });
        };
    
        promises.push(putRecords(params));
    }
    
    // build the update expression to increment totals
    exp = "set dailyTotals.d" + curDay + ".buyShares = dailyTotals.d" + curDay + ".buyShares + :val1";
    exp = exp + ", dailyTotals.d" + curDay + ".buyTotal = dailyTotals.d" + curDay + ".buyTotal + :val2";
    exp = exp + ", dailyTotals.d" + curDay + ".buyOrders = dailyTotals.d" + curDay + ".buyOrders + :val3";
    exp = exp + ", dailyTotals.d" + curDay + ".sellShares = dailyTotals.d" + curDay + ".sellShares + :val4";
    exp = exp + ", dailyTotals.d" + curDay + ".sellTotal = dailyTotals.d" + curDay + ".sellTotal + :val5";
    exp = exp + ", dailyTotals.d" + curDay + ".sellOrders = dailyTotals.d" + curDay + ".sellOrders + :val6";
    
    // initialize counters for next day
    date.setDate(date.getDate() + 1);
    curDay = date.getFullYear() + "" + addZeros(date.getMonth() + 1) + "" + addZeros(date.getDate()),
    exp += ", dailyTotals.d" + curDay + " = :val7";
    
    // remove stale summary
    date.setDate(date.getDate() - 32);
    curDay = date.getFullYear() + "" + addZeros(date.getMonth() + 1) + "" + addZeros(date.getDate()),
    exp += " remove dailyTotals.d" + curDay; 
    
    // set the query parameters
    query = {
        TableName: table,
        UpdateExpression: exp,
        ReturnValues: "UPDATED_NEW"
    };
    
    // get a promise for each update
    for (var key in vals) {
        var attrVals = {":val7": {"buyShares":0, "sellShares":0, "buyTotal":0, "sellTotal":0, "buyOrders":0, "sellOrders":0}};
        
        query.Key = {"PK": key, "SK": sk};
        attrVals[":val1"] = vals[key]["buyShares"];
        attrVals[":val2"] = vals[key]["buyTotal"];
        attrVals[":val3"] = vals[key]["buyOrders"];
        attrVals[":val4"] = vals[key]["sellShares"];
        attrVals[":val5"] = vals[key]["sellTotal"];
        attrVals[":val6"] = vals[key]["sellOrders"];
        
        query.ExpressionAttributeValues = attrVals;
        
        // update the security counter 
        promises.push(docClient.update(query).promise());
    };
        
    // wait on the promises
    try {
        await Promise.all(promises);
        callback(null, "true");
    } catch(error) {
        console.log(error);
        console.log(exp);
        callback(null, "false");
    }
};