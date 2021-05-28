package com.amazonaws.TableLoader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.http.timers.client.ClientExecutionTimeoutException;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.BatchExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.BatchExecuteStatementResult;
import com.amazonaws.services.dynamodbv2.model.BatchStatementRequest;
import com.amazonaws.services.dynamodbv2.model.BatchStatementResponse;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;

/**
 * Executes an asynchromous batch write to a DynamoDB table
 * 
 * @author rickhou
 *
 */
public class BatchLoad implements Runnable {
	private TableWriteItems items;
	private String key;
	private static volatile Boolean error = false;
	private boolean partiql = false;
	private BatchExecuteStatementRequest br;

	/**
	 * Constructor
	 * 
	 * @param items - the collection of items to be written
	 */
	public BatchLoad(TableWriteItems items) {
		this.items = items;

		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	public BatchLoad(BatchExecuteStatementRequest br) {
		this.partiql = true;
		this.br = br;

		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	public BatchLoad(TableWriteItems items, String key) {
		this.items = items;
		this.key = key;

		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	/**
	 * the runnable process to execute the batch write
	 */
	@Override
	public void run() {
		// execute the write and iterate if there are unprocessed items
		try {
			try {
				if (partiql) {
					List<BatchStatementRequest> statements;
					do {
						BatchExecuteStatementResult result = Main.adb.batchExecuteStatement(br);

						statements = new ArrayList<BatchStatementRequest>();
						int pos = 0;
						for (BatchStatementResponse response : result.getResponses()) {
							if (response.getError() != null)
								statements.add(br.getStatements().get(pos));
							pos++;
						}
						br.setStatements(statements);
					} while (statements.size() > 0);
				} else {
					BatchWriteItemOutcome outcome = Main.db.batchWriteItem(items);
					while (outcome.getUnprocessedItems().size() > 0) {
						outcome = Main.db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
					}
				}

				if (error) {
					error = false;
				}
				synchronized (Main.sync) {
					Main.numThreads.decrementAndGet();
				}

				if (key != null)
					synchronized (ItemWriter.counters.get(key)) {
						ItemWriter.counters.put(key, Integer.valueOf(ItemWriter.counters.get(key) - 1));
					}
			} catch (ClientExecutionTimeoutException ex) {
				run();
			} catch (InternalServerErrorException ex) {
				synchronized (error) {
					if (!error)
						System.out.println(String.format("Partition split detected...", key));

					error = true;
				}
				Thread.sleep(10000);
				run();
			} catch (SdkClientException ex) {
				run();
			}
		} catch (InterruptedException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}
}
