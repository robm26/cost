package com.amazonaws.TableLoader;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

public class ItemWriter implements Runnable {
	String leadingChar;
	String partitionKey;
	String key;
	TableWriteItems twi;
	public static Map<String, Integer> counters = new HashMap<String, Integer>();

	public ItemWriter(String partitionKey, String leadingChar, String table) {
		this.partitionKey = partitionKey;
		this.leadingChar = leadingChar;
		this.twi = new TableWriteItems(table);

		for (int i = 0; i < 25; i++) {
			Item item = new Item().withString("PK", partitionKey).withString("SK",
					String.format("%s#%d", leadingChar, i));
			twi.addItemToPut(item);
		}

		key = String.format("%s#%s", partitionKey, leadingChar);
		counters.put(key, Integer.valueOf(0));
	}

	@Override
	public void run() {
		System.out.println(
				String.format("Loading '%s' partition with '%s' sort key prefix...", partitionKey, leadingChar));

		while (Main.runFlag) {
			synchronized (counters.get(key)) {
				counters.put(key, Integer.valueOf(counters.get(key) + 1));
			}

			Main.tpe.execute(new BatchLoad(twi, key));

			try {
				Thread.sleep(Integer.valueOf(partitionKey.split("#")[1]) * 50 + 10);
				
				while (counters.get(key) > 20)
					Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}
}
