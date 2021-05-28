package com.amazonaws.TableLoader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;

/**
 * reads all items from a given logical partition on a DynamoDB table
 * 
 * @author rickhou
 *
 */
public class RunQuery implements Runnable {
	private String pKey, table;
	private boolean partiql = false;

	public RunQuery(String table, String pKey, boolean partiql) {
		synchronized (Main.numThreads) {
			Main.numThreads.incrementAndGet();
		}

		this.table = table;
		this.pKey = pKey;
		this.partiql = partiql;
	}

	/**
	 * the runnable process that executes the read
	 */
	@Override
	public void run() {
		ArrayList<Object> items = new ArrayList<Object>();

		if (partiql) {
			ExecuteStatementRequest request = new ExecuteStatementRequest()
					.withStatement(String.format("SELECT * FROM %s WHERE \"PK\" = '%s'", table, pKey));
			do {
				ExecuteStatementResult result = Main.adb.executeStatement(request);
				items.addAll(result.getItems());
			} while (request.getNextToken() != null);
			
		} else {
			QuerySpec spec = new QuerySpec().withKeyConditionExpression("PK = :pKey")
					.withValueMap(new ValueMap().withString(":pKey", pKey));

			ItemCollection<QueryOutcome> results = Main.db.getTable(table).query(spec);

			for (Page<Item, QueryOutcome> page : results.pages()) {
				items.addAll(page.getLowLevelResult().getItems());
			}
		}

		synchronized (Main.sItems) {
			// put these results in the result map
			if (Main.sItems.containsKey(pKey))
				Main.sItems.get(pKey).addAll(items);
			else
				Main.sItems.put(pKey, items);

			Main.numThreads.decrementAndGet();
		}
	}
}
