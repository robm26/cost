package com.amazonaws.ItemGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateTableSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalSecondaryIndexAction;

public class Main {
	// misc globals
	public static DynamoDB db;
	public static volatile AtomicInteger numThreads = new AtomicInteger(0);
	public static volatile Object sync = new Object();
	public static Table table;
	public static volatile Map<Integer, ArrayList<Item>> results = new HashMap<Integer, ArrayList<Item>>();
	public static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);

	private static long elapsed;
	private static int count = 0, numSecurities, numTransactions;
	private static Calendar cal = Calendar.getInstance();
	private static final DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private static final DateFormat iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
	private static Random random = new Random();
	private static TableWriteItems twi;
	private static String tableName, target;
	private static String region = Regions.getCurrentRegion().getName();
	private static boolean listener = false, clearTable = false;

	// main function
	public static void main(String[] args) {
		disableWarning();
		// using GMT time zone
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

		// configure the client
		ClientConfiguration config = new ClientConfiguration().withConnectionTimeout(500)
				.withClientExecutionTimeout(20000).withRequestTimeout(1000).withSocketTimeout(1000)
				.withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(20));

		db = new DynamoDB(AmazonDynamoDBClientBuilder.standard().withClientConfiguration(config)
				.withCredentials(new ProfileCredentialsProvider("default")).build());

		// set globals
		parseArgs(args);

		// check if we are clearing the tables
		if (clearTable) {
			clearTable(tableName);
			clearTable(String.format("%s_s", tableName));
			System.exit(0);
		} else
			table = db.getTable(tableName);

		if (listener) {
			// setup up the listener
			listen();

			// sleep for a second to make sure a listener gets stood up then cycle the test
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}

			// load the counters and transaction items for this region
			System.out.println("Cycling test...");
			if (loadSecurities()) {
				loadTransactions(1);
			}

			// let the source region know to start scanning
			send(target, 2);
		} else {
			// this is the source region, load the data
			if (loadSecurities()) {
				loadTransactions(0);

				if (target != null) {
					// let the target region know to start scanning
					send(target, 1);

					// Cycle the test and start listening
					System.out.println("\nCycling test...");
					listen();
				}
			}
		}

		Scanner scanner = new Scanner(System.in);
		if (!listener && target != null) {
			System.out.print("Enter TransactionID: ");
			getTransactionById(scanner.nextLine());

			System.out.println();
			System.out.print("Enter SecurityID: ");
			getAllTransactionsForSecurity(scanner.nextLine());
			System.out.println();
		}

		// shutdown the thread pool and exit
		System.out.println("Shutting down....");
		tpe.shutdown();
		scanner.close();
		System.out.println("Done.\n");
	}

	// blow away all the items from previous test cycle
	private static void clearTable(String name) {
		System.out.print(String.format("Clearing items from table [%s]...", name));
		elapsed = System.currentTimeMillis();

		for (int i = 0; i < tpe.getMaximumPoolSize(); i++) {
			tpe.execute(new RunScan(i, name));
		}

		waitForWorkers();

		twi = new TableWriteItems(name);
		count = 0;
		for (ArrayList<Item> resultItems : results.values()) {
			for (Item item : resultItems)
				removeItem(item);

			count += resultItems.size();
		}
		removeItem(null);

		waitForWorkers();
		System.out.println(String.format("\nDeleted %d items in %dms.", count, System.currentTimeMillis() - elapsed));
		results.clear();
	}

	// load items for deletion and execute BatchwWrite requests
	private static void removeItem(Item item) {
		if (item != null)
			twi.addHashAndRangePrimaryKeysToDelete("PK", "SK", item.get("PK"), item.get("SK"));

		// check if we need to send a batch write
		if (twi.getPrimaryKeysToDelete() != null && (item == null || twi.getPrimaryKeysToDelete().size() == 25)) {
			tpe.execute(new BatchLoad(twi));
			twi = new TableWriteItems(twi.getTableName());
		}
	}

	// standup a listener and wait for the source region to complete the table load
	private static void listen() {
		boolean run = true;

		System.out.println("Waiting for loader to complete...");
		try (ServerSocket serverSocket = new ServerSocket(8080);) {
			while (run) {
				Socket clientSocket = serverSocket.accept();
				PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

				String inputLine, outputLine;

				// wait for the loader to tell us we are ready to scan
				inputLine = in.readLine();
				int iteration = Integer.valueOf(inputLine.split("\\|")[1]);

				if (inputLine.startsWith("execute")) {
					System.out.print("\nLoader completed, scanning table...");

					int total = 0;
					long time = 0;

					elapsed = System.currentTimeMillis();

					// scan until we see the correct number of items
					while (total < numTransactions * iteration) {
						time = System.currentTimeMillis();
						for (int i = 0; i < tpe.getMaximumPoolSize(); i++) {
							tpe.execute(new RunScan(i, tableName));
						}

						waitForWorkers();

						for (Integer key : results.keySet()) {
							total += results.get(key).size();
						}

						// log how many items we collected if the total is not what we expect to see
						if (total < numTransactions * iteration) {
							System.out.print(String.format("\n%d of %d items replicated...",
									total - (numTransactions * (iteration - 1)), numTransactions));
							total = 0;
							results = new HashMap<Integer, ArrayList<Item>>();
						}
					}

					time = System.currentTimeMillis() - time;
					elapsed = System.currentTimeMillis() - elapsed;
					outputLine = String.format("%d of %d items scanned in %dms.  Replication lag: %dms",
							total - (numTransactions * (iteration - 1)), numTransactions, time, elapsed - time);

					// send the result to the source region and log to console.
					out.println(outputLine);
					System.out.println();
					System.out.println(outputLine);
					System.out.println();
					break;
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	// let the other region know table loading is complete
	private static void send(String hostName, int iteration) {
		System.out.println("Sending notification to scanner...");
		try (Socket socket = new Socket(hostName, 8080);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
			String fromServer;

			// tell the listener to start scanning the local table
			out.println(String.format("execute|%d", iteration));

			// wait for the listener and log the response to the console
			fromServer = in.readLine();
			System.out.println(fromServer);
		} catch (UnknownHostException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	// load the summary counters for the securities
	private static boolean loadSecurities() {
		elapsed = System.currentTimeMillis();
		System.out.print("Loading security counter items...");
		for (count = 1; count <= numSecurities; count++) {
			saveItem(
					new Item().withString("PK", String.format("S%d", count))
							.withString("SK", (region.equals("us-west-2") ? "A" : "B")).withString("type", "summary")
							.withMap("dailyTotals", getMap()).withString("region", region),
					String.format("%s_s", tableName));
		}

		// run the last batchWrite
		saveItem(null, String.format("%s_s", tableName));
		waitForWorkers();

		// log elapsed time and wait on console input
		System.out.println(String.format("\nLoaded %d security counters in %dms\n", numSecurities,
				System.currentTimeMillis() - elapsed));
		System.out.println();

		return true;
	}

	// hash the transactions by ID across 100 shards
	private static boolean loadTransactions(int iteration) {
		// reset the count and load order items
		count = 0;
		elapsed = System.currentTimeMillis();
		System.out.print(String.format("Loading %d transactions...", numTransactions));
		while (count++ < numTransactions) {
			// create an order and add it to the container
			Integer id = count + iteration * numTransactions;

			String security, pk, transId, type;
			// generate a security ID
			security = String.format("S%d", random.nextInt(numSecurities) + 1);
			// generate the transaction ID
			transId = String.format("%s#T%d", security, id);

			// build the transaction item key, hash the transaction ID, and modulo the hash
			// by 100 for the shard ID
			pk = String.format("%s#%d", security, Math.abs(transId.hashCode() % 100));
			type = (random.nextInt(100) < 50 ? "buy" : "sell");

			DecimalFormat df = new DecimalFormat("#.00");
			cal.setTime(new Date());
			cal.add(Calendar.DAY_OF_YEAR, random.nextInt(365));
			cal.set(Calendar.HOUR_OF_DAY, random.nextInt(24));
			cal.set(Calendar.MINUTE, random.nextInt(60));
			cal.set(Calendar.SECOND, random.nextInt(60));
			Item item = new Item().withString("PK", pk).withString("SK", transId).withString("security", security)
					.withString("timestamp", iso.format(cal.getTime()))
					.withNumber("price", 15.5 + Double.parseDouble(df.format(random.nextDouble())))
					.withString("type", type).withNumber("shares", random.nextInt(100) + 1)
					.withString("region", region);

			saveItem(item, tableName);
		}

		// if any items are left run the batchWrite
		saveItem(null, tableName);
		waitForWorkers();

		// log elapsed time and exit
		System.out.println(
				String.format("\nLoaded %d items in %dms\n", numTransactions, System.currentTimeMillis() - elapsed));

		return true;
	}

	// query for a trade by transactionId
	private static void getTransactionById(String transId) {
		// split out the securityId and modulo transId by 100 to find the shard the
		// trade was written to.
		String pk = String.format("%s#%d", transId.split("#")[0], Math.abs(transId.hashCode() % 100));

		// build the query
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("PK = :pk and SK = :sk")
				.withValueMap(new ValueMap().withString(":pk", pk).withString(":sk", transId));

		// run the query and print the result
		System.out.print(String.format("Running query for transaction ID '%s'...", transId));
		tpe.execute(new RunQuery(spec, table, 0));
		waitForWorkers();
		System.out.println(String.format("\n%s", results.get(0).get(0).toString()));

		results.clear();
	}

	// query all trades for a securityId
	private static void getAllTransactionsForSecurity(String securityId) {
		QuerySpec spec;
		System.out.print(String.format("Retrieving all transactions for securityId '%s'...", securityId));

		elapsed = System.currentTimeMillis();

		// send a query to each shard
		for (int shardId = 0; shardId < 100; shardId++) {
			spec = new QuerySpec().withKeyConditionExpression("PK = :pk")
					.withValueMap(new ValueMap().withString(":pk", String.format("%s#%d", securityId, shardId)));

			tpe.execute(new RunQuery(spec, table, shardId));
		}
		waitForWorkers();
		elapsed = System.currentTimeMillis() - elapsed;

		// count the items returned and report the results
		count = 0;
		for (ArrayList<Item> items : results.values())
			count += items.size();

		System.out.println(String.format("\nRetrieved %d items in %dms.", count, elapsed));

		results.clear();
	}

	private static void saveItem(Item item, String name) {
		// add the item to the batch
		if (item != null) {
			if (twi == null)
				twi = new TableWriteItems(name);

			twi.addItemToPut(item);

			// if the container has 25 items run the batchWrite on a new thread
			if (twi.getItemsToPut().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(name);
			}
		} else {
			if (twi.getItemsToPut() != null)
				tpe.execute(new BatchLoad(twi));
			twi = null;
		}
	}

	private static void waitForWorkers() {
		// sleep until all updates are done
		while (numThreads.get() > 0)
			try {
				System.out.print(".");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
	}

	private static Map<String, Map<String, Number>> getMap() {
		Map<String, Map<String, Number>> map = new HashMap<String, Map<String, Number>>();
		Map<String, Number> props = new HashMap<String, Number>();
		props.put("buyShares", 0);
		props.put("sellShares", 0);
		props.put("sellTotal", 0);
		props.put("buyTotal", 0);
		props.put("sellOrders", 0);
		props.put("buyOrders", 0);
		map.put(String.format("d%s", sdf.format(cal.getTime())), props);

		return map;
	}

	private static void createTable() {
		try {
			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Creating table '%s' at 100K WCU...", tableName));
			table = db.createTable(tableName,
					Arrays.asList(new KeySchemaElement("PK", KeyType.HASH), new KeySchemaElement("SK", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("PK", ScalarAttributeType.S),
							new AttributeDefinition("SK", ScalarAttributeType.S)),
					new ProvisionedThroughput(10L, 100000L));
			table.waitForActive();
			System.out.println(String.format("Table created in %dms", System.currentTimeMillis() - elapsed));

			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Updating table to 10K WCU..."));
			table.updateTable(new ProvisionedThroughput(10L, 10000L));
			table.waitForActive();
			System.out.println(String.format("Table updated in %dms", System.currentTimeMillis() - elapsed));

			createIndex("GSI1PK");
			createIndex("GSI2PK");
			createIndex("GSI3PK");

			System.out.println("[Press ENTER to continue]");
			Scanner scanner = new Scanner(System.in);
			scanner.nextLine();
			scanner.close();
			System.out.println();
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	private static void createIndex(String pk) {
		try {
			try {
				ArrayList<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();
				attrDefs.add(new AttributeDefinition().withAttributeName(pk).withAttributeType("S"));
				attrDefs.add(new AttributeDefinition().withAttributeName("GSISK").withAttributeType("S"));

				GlobalSecondaryIndexUpdate update = new GlobalSecondaryIndexUpdate()
						.withCreate(new CreateGlobalSecondaryIndexAction().withIndexName(pk.replaceAll("PK", ""))
								.withProvisionedThroughput(new ProvisionedThroughput(10L, 100000L))
								.withKeySchema(new KeySchemaElement().withAttributeName(pk).withKeyType(KeyType.HASH),
										new KeySchemaElement().withAttributeName("GSISK").withKeyType(KeyType.RANGE))
								.withProjection(new Projection().withProjectionType("ALL")));

				UpdateTableSpec uts = new UpdateTableSpec().withAttributeDefinitions(attrDefs)
						.withGlobalSecondaryIndexUpdates(update);
				table.updateTable(uts);

				elapsed = System.currentTimeMillis();
				System.out.println(String.format("Creating %s at 100K WCU...", pk.replaceAll("PK", "")));
				table.waitForActive();
				System.out.println(String.format("Index created in %dms", System.currentTimeMillis() - elapsed));

				update = new GlobalSecondaryIndexUpdate()
						.withUpdate(new UpdateGlobalSecondaryIndexAction().withIndexName(pk.replaceAll("PK", ""))
								.withProvisionedThroughput(new ProvisionedThroughput(10L, 10000L)));
				uts = new UpdateTableSpec().withGlobalSecondaryIndexUpdates(update);

				elapsed = System.currentTimeMillis();
				System.out.println(String.format("Updating %s to 10K WCU...", pk.replaceAll("PK", "")));
				table.updateTable(uts);
				table.waitForActive();
			} catch (LimitExceededException ex) {
				Thread.sleep(5000);
				createIndex(pk);
			}
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	private static void parseArgs(String[] args) {
		Map<String, String> argVals = new HashMap<String, String>();
		String lastArg = "";

		for (String arg : args) {
			if (arg.startsWith("-")) {
				lastArg = arg;
				argVals.put(lastArg, null);
			} else
				argVals.put(lastArg, arg);
		}

		for (String key : argVals.keySet()) {
			switch (key) {
			case "-t":
				tableName = argVals.get(key);
				break;

			case "-s":
				numSecurities = Integer.valueOf(argVals.get(key));
				break;

			case "-n":
				numTransactions = Integer.valueOf(argVals.get(key));
				break;

			case "-d":
				numSecurities = Integer.valueOf(argVals.get(key));
				break;

			case "-l":
				listener = true;
				break;

			case "-h":
				target = argVals.get(key);
				break;

			case "-r":
				clearTable = true;
				break;
			}
		}
	}

	private static void disableWarning() {
		try {
			Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			sun.misc.Unsafe u = (sun.misc.Unsafe) theUnsafe.get(null);

			Class cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
			Field logger = cls.getDeclaredField("logger");
			u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
		} catch (Exception e) {
			// ignore
		}
	}
}
