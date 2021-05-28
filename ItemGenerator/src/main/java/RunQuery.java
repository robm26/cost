package com.amazonaws.ItemGenerator;

import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class RunQuery implements Runnable {
	private QuerySpec spec;
	private Table table;
	private Integer key;

	public RunQuery(QuerySpec spec, Table table, Integer key) {
		synchronized (Main.numThreads) {
			Main.numThreads.incrementAndGet();
		}
		
		this.spec = spec;
		this.table = table;
		this.key = key;
	}

	/**
	 * the runnable process that executes the read
	 */
	@Override
	public void run() {
		ItemCollection<QueryOutcome> results = table.query(spec);

		ArrayList<Item> items = new ArrayList<Item>();
		for (Page<Item, QueryOutcome> page : results.pages()) {
			Iterator<Item> it = page.iterator();
			while (it.hasNext()) {
				items.add(it.next());
			}
		}

		synchronized (Main.numThreads) {
			// put these results in the result map
			Main.results.put(key, items);
			Main.numThreads.decrementAndGet();
		}
	}
}
