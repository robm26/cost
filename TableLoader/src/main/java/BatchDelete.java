package com.amazonaws.TableLoader;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

public class BatchDelete  implements Runnable {
	private TableWriteItems items;

	/**
	 * Constructor
	 * 
	 * @param items - the collection of items to be written
	 */
	public BatchDelete(TableWriteItems items) {
		this.items = items;
	}

	/**
	 * the runnable process to execute the batch write
	 */
	@Override
	public void run() {
		BatchWriteItemOutcome outcome = Main.db.batchWriteItem(items);
		
		while (outcome.getUnprocessedItems().size() > 0) {
			outcome = Main.db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
		}
	}	
}
