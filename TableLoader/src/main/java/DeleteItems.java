package com.amazonaws.TableLoader;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

public class DeleteItems {
	private TableWriteItems twi;
	private static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);
	
	private void clearTable(String table, List<String> keys ) {
		// delete all the items returned from scan
		twi = new TableWriteItems(table );
		
			for (String key : keys)
				removeItem(key);

		removeItem(null);
	}

	private void removeItem(String key) {
		if (key != null) {
			twi.addHashAndRangePrimaryKeysToDelete("PK", "SK", key, "A");

			// check if we need to send a batch write
			if (twi.getPrimaryKeysToDelete().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(twi.getTableName());
			}
		} else if (twi.getPrimaryKeysToDelete() != null)
			tpe.execute(new BatchLoad(twi));
	}

}
