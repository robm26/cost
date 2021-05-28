package com.amazonaws.ItemGenerator;

import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

/**
 * reads all items from a given logical partition on a DynamoDB table
 * @author rickhou
 *
 */
public class RunScan implements Runnable {
	private int shardId;
	private String tableName;
	ArrayList<Item> items = new ArrayList<Item>();

	public RunScan(int shardId, String tableName) {
		this.shardId = shardId;
		this.tableName = tableName;
		
		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	/**
	 * the runnable process that executes the read
	 */
	@Override
	public void run() {
		// create the scan specification
		ScanSpec spec = new ScanSpec().withSegment(shardId).withTotalSegments(Main.tpe.getMaximumPoolSize());
		ItemCollection<ScanOutcome> results = Main.db.getTable(tableName).scan(spec);
		
		// collect the scanned items 
		for (Page<Item, ScanOutcome> page : results.pages()) {
			Iterator<Item> it = page.iterator();
			
			while (it.hasNext()) {
				items.add(it.next());
			}
		}

		synchronized (Main.sync) {
			// put these results in the result map
			Main.results.put(Integer.valueOf(shardId), items);
			Main.numThreads.decrementAndGet();
		}
	}
}
