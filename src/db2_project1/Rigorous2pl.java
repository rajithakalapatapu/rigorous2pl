package db2_project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import db2_project1.Transaction.State;

public class Rigorous2pl {
	static Map<Integer, Transaction> transactionTable = new HashMap<>();
	static Map<String, Lock> lockTable = new HashMap<>();
	static int timestamp = 0;

	enum OperationType {
		UNKNOWN, READ, WRITE, COMMIT;
	}

	public static void main(String[] args) {
//		runAllFiles();
		processFile("src/1.txt");
	}

	private static void runAllFiles() {
		for (int i = 1; i <= 7; i++) {
			String file = "src/" + i + ".txt";
			processFile(file);
			transactionTable.clear();
			lockTable.clear();
		}
	}

	private static void processFile(String filePath) {
		System.out.println("Starting processing " + filePath);
		try {
			FileReader fr = new FileReader(filePath);
			BufferedReader br = new BufferedReader(fr);
			String line;
			while ((line = br.readLine()) != null) {
				processLine(line);
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void processLine(String line) {

		String token = line.replace(" ", "");
		int transactionID = extractTransactionID(token);

//		System.out.println("\n" + token + transactionTable + lockTable);
		System.out.println("\n" + token);

		switch (token.charAt(0)) {
		case 'b':
			beginTransaction(transactionID);
			break;
		case 'r':
			String dataItem = extractDataItem(token);
			readTransaction(transactionID, dataItem);
			break;
		case 'w':
			dataItem = extractDataItem(token);
			writeTransaction(transactionID, dataItem);
			break;
		case 'e':
			endTransaction(transactionID);
			break;
		default:
			System.out.println("Unrecognized Token");

		}
	}

	private static int extractTransactionID(String token) {
		if (token.charAt(0) == 'b' || token.charAt(0) == 'e') {
			int firstColonIndex = token.indexOf(";");
			if (firstColonIndex == -1) {
				return 0;
			}
			String tid = token.substring(1, firstColonIndex);
			return Integer.parseInt(tid);

		} else if (token.charAt(0) == 'r' || token.charAt(0) == 'w') {
			int firstParanIndex = token.indexOf("(");
			if (firstParanIndex == -1) {
				return 0;
			}
			String tid = token.substring(1, firstParanIndex);
			return Integer.parseInt(tid);

		}

		return 0;
	}

	private static String extractDataItem(String token) {
		String dataItem = "";
		int openParanIndex = token.indexOf("(");
		if (openParanIndex == -1) {
			return dataItem;
		}
		int closeParanIndex = token.indexOf(")");
		if (closeParanIndex == -1) {
			return dataItem;
		}

		dataItem = token.substring(openParanIndex + 1, closeParanIndex);

		return dataItem;
	}

	private static void beginTransaction(int transactionID) {
		timestamp = timestamp + 1;
		State state = Transaction.State.ACTIVE;
		transactionTable.put(transactionID, new Transaction(transactionID, timestamp, state));
		System.out.println("Starting active transaction with id " + transactionID + " and timestamp as " + timestamp);

	}

	private static void readTransaction(int transactionID, String dataItem) {
		executeOperation(transactionID, dataItem, OperationType.READ);

	}

	private static void writeTransaction(int transactionID, String dataItem) {
		executeOperation(transactionID, dataItem, OperationType.WRITE);

	}

	private static void endTransaction(int transactionID) {
		commitTransaction(transactionTable.get(transactionID));
	}

	private static void executeOperation(int transactionID, String dataItem, OperationType operationType) {
		Transaction t = transactionTable.get(transactionID);
		switch (t.state) {
		case ACTIVE:
			activeTransaction(t, dataItem, operationType);
			break;
		case BLOCKED:
			blockedTransaction(t, dataItem, operationType);
			break;
		case COMMITED:
			System.out.println("Transaction " + transactionID + " is committed. Ignoring...");
			break;
		case ABORTED:
			System.out.println("Transaction " + transactionID + " is aborted. Ignoring...");
			break;
		case UNKNOWN:
		default:
			System.out.println("Transaction " + transactionID + " has unknown state");
			break;
		}

	}

	private static void activeTransaction(Transaction t, String dataItem, OperationType operationType) {
		if (!lockTable.containsKey(dataItem)) {
			Lock lock = new Lock(dataItem, operationType);
			switch (operationType) {
			case READ:
				System.out.println("Transaction " + t.id + " has acquired Read lock on " + dataItem);
				lock.transactionIdsWithReadLock.add(t.id);
				break;
			case WRITE:
				System.out.println("Transaction " + t.id + " has acquired Write lock on " + dataItem);
				lock.transactionIdWithWriteLock = t.id;
				break;
			default:
				break;

			}
			if (!t.itemsLocked.contains(dataItem)) {
				t.itemsLocked.add(dataItem);
			}
			transactionTable.put(t.id, t);
			lockTable.put(dataItem, lock);

			return;
		}

		Lock l = lockTable.get(dataItem);
		if (l.operationType == OperationType.READ && operationType == OperationType.READ) {
			l = readRead(t, dataItem, l);
		} else if (l.operationType == OperationType.READ && operationType == OperationType.WRITE) {
			l = readWrite(t, dataItem, l);
		} else if (l.operationType == OperationType.WRITE && operationType == OperationType.READ) {
			l = writeRead(t, dataItem, l);
		} else if (l.operationType == OperationType.WRITE && operationType == OperationType.WRITE) {
			l = writeWrite(t, dataItem, l);
		} else if (l.operationType == OperationType.UNKNOWN && operationType == OperationType.READ) {
			l.operationType = OperationType.READ;
			System.out.println("Transaction " + t.id + " has acquired readlock on " + dataItem);
			l.transactionIdsWithReadLock.add(t.id);
		} else if (l.operationType == OperationType.UNKNOWN && operationType == OperationType.WRITE) {
			l.operationType = OperationType.WRITE;
			System.out.println("Transaction " + t.id + " has acquired write lock on " + dataItem);
			l.transactionIdWithWriteLock = t.id;
		}
		lockTable.put(dataItem, l);

	}

	private static Lock readRead(Transaction t, String dataItem, Lock l) {
		l.transactionIdsWithReadLock.add(t.id);
		System.out.println("Transaction " + t.id + " has also acquired read lock on dataitem " + dataItem);

		if (!t.itemsLocked.contains(dataItem)) {
			t.itemsLocked.add(dataItem);
		}
		transactionTable.put(t.id, t);
		return l;
	}

	private static Lock readWrite(Transaction t, String dataItem, Lock l) {
		if (l.transactionIdsWithReadLock.size() == 1) {
			// Only one transaction has acquired lock on dataItem
			if (l.transactionIdsWithReadLock.get(0) == t.id) {
				// upgrade the lock if it is the same transactions
				l.operationType = OperationType.WRITE;
				l.transactionIdsWithReadLock.clear();
				l.transactionIdWithWriteLock = t.id;
				System.out.println("Transaction " + t.id + " has upgraded Read lock to Write lock on " + dataItem);
			} else {
				// conflict between two different transactions hence apply wound-wait
				Transaction old = transactionTable.get(l.transactionIdsWithReadLock.get(0));
				applyWoundWait(old, t, new Operation(OperationType.WRITE, dataItem), l);
//				if (old.timestamp < t.timestamp) {
//					// old will Wound t, set t to blocked and restart t with same timestamp
//					t.state = State.BLOCKED;
//					Operation pendingOp = new Operation(OperationType.WRITE, dataItem);
//					t.pendingOperations.add(pendingOp);
//					transactionTable.put(t.id, t);
//					l.waitingList.add(t.id);
//					System.out.println("Transaction " + t.id + " was wounded by " + old.id);
//					System.out.println("Current write operation is added as pending for " + t.id);
//					System.out.println(
//							"Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
//				} else {
//					// old will wait (in aborted state?)
//					old.state = State.ABORTED;
//					transactionTable.put(old.id, old);
//					l.operationType = OperationType.WRITE;
//					l.transactionIdsWithReadLock.clear();
//					l.transactionIdWithWriteLock = t.id;
//					System.out.println("Transaction " + old.id + " aborted since it was younger than " + t.id);
//					System.out.println(t.id + " has now acquired write lock on " + dataItem);
//					releaseLocks(old, dataItem, l);
//				}
			}
		} else {
			// multiple items have read lock apply wound-wait accordingly
			List<Integer> readlist = l.transactionIdsWithReadLock;
			// find the oldest transactions of these - sort in ascending order and find the
			// least element
			Collections.sort(readlist);
			int oldestTransaction = readlist.get(0);

			if (t.id == oldestTransaction) {
				// Upgrade lock
				System.out.println("Transaction " + t.id + " has upgraded its lock on " + dataItem
						+ " from read lock to write lock");
				for (int i = 1; i < readlist.size(); i++) {
					Transaction tOld = transactionTable.get(readlist.get(i));
					abortTransaction(tOld);
					System.out.println("Aborting transaction " + tOld.id);
				}
				l.transactionIdsWithReadLock.clear();
				l.transactionIdWithWriteLock = t.id;
			} else if (t.id < oldestTransaction) {
				// the transaction we're processing is older than the oldest we have. So all the
				// transactions in readlist will get wounded by t
				System.out.println("Transaction " + t.id + " has acquired write lock since it is older than "
						+ oldestTransaction + " for " + dataItem);
				for (int i = 0; i < readlist.size(); i++) {
					Transaction tOld = transactionTable.get(readlist.get(i));
					abortTransaction(tOld);
					System.out.println("Aborting transaction " + tOld.id);
				}
				l.transactionIdsWithReadLock.clear();
				l.transactionIdWithWriteLock = t.id;
			} else {
				// the transaction we're processing is newer than the oldest we have - so it
				// gets blocked
				t.state = State.BLOCKED;
				t.pendingOperations.add(new Operation(OperationType.WRITE, dataItem));
				System.out.println("Transaction " + t.id + " has been blocked because it is younger");

				// an older transaction having read lock on dataitem can wound a younger
				// transaction asking for write on the same dataitem
				for (int i = 0; i < readlist.size(); i++) {
					Transaction tOld = transactionTable.get(readlist.get(i));
					if (tOld.itemsLocked.contains(dataItem)) {
						// tOld will wound t
						abortTransaction(t);
//						t.state = State.BLOCKED;
//						Operation pendingOp = new Operation(OperationType.WRITE, dataItem);
//						t.pendingOperations.add(pendingOp);
//						transactionTable.put(t.id, t);
//						l.waitingList.add(t.id);
//						System.out.println("Transaction " + t.id + " was wounded by " + tOld.id);
//						System.out.println("Current write operation is added as pending for " + t.id);
//						System.out.println(
//								"Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");

//						readlist.remove(readlist.indexOf(t.id));
						readlist.remove(i);
						break;
					}
				}

//				System.out.println(t);
//				System.out.println(transactionTable);
//				System.out.println(lockTable);
//
//				int i = 0;
//				while (i < readlist.size()) {
//					if (t.id > readlist.get(i)) {
//						// do not wound transactions that are older than t
//						i++;
//					}
//					if (t.id <= readlist.get(i)) {
//						// only wound transactions that are younger than t
//						Transaction tOld = transactionTable.get(readlist.get(i));
//						abortTransaction(tOld);
//						System.out.println(i);
//						System.out.println(readlist);
//						readlist.remove(i);
//						System.out.println(readlist);
//						System.out.println("Aborting transaction " + tOld.id);
//					}
//				}

				l.transactionIdsWithReadLock = readlist;
			}
		}

		if (!t.itemsLocked.contains(dataItem)) {
			t.itemsLocked.add(dataItem);
		}
		transactionTable.put(t.id, t);
		return l;
	}

	private static Lock writeRead(Transaction t, String dataItem, Lock l) {
		if (l.transactionIdWithWriteLock == t.id) {
			// downgrade write lock to read lock
			l.transactionIdWithWriteLock = 0;
			l.transactionIdsWithReadLock.add(t.id);
			l.operationType = OperationType.READ;
			System.out.println("Transaction " + t.id + " has downgraded its write lock to read lock for " + dataItem);
		} else {
			Transaction tWithWriteLock = transactionTable.get(l.transactionIdWithWriteLock);
			applyWoundWait(tWithWriteLock, t, new Operation(OperationType.WRITE, dataItem), l);

//			if (tWithWriteLock.timestamp < t.timestamp) {
//				// tWithWriteLock will wound t. Put t in blocked state and restart transaction
//				t.state = State.BLOCKED;
//				Operation pendingOp = new Operation(OperationType.READ, dataItem);
//				t.pendingOperations.add(pendingOp);
//				transactionTable.put(t.id, t);
//				l.waitingList.add(t.id);
//				System.out.println("Transaction " + t.id + " was wounded by " + tWithWriteLock.id);
//				System.out.println("Current read operation is added as pending for " + t.id);
//				System.out
//						.println("Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
//			} else {
//				// tWithWriteLock will be put to wait and the older process (t) will get the
//				// read lock
//				tWithWriteLock.state = State.ABORTED;
//				transactionTable.put(tWithWriteLock.id, tWithWriteLock);
//
//				l.transactionIdWithWriteLock = 0;
//				l.operationType = OperationType.READ;
//				l.transactionIdsWithReadLock.add(t.id);
//
//				System.out.println("Transaction " + tWithWriteLock.id + " aborted because it is younger than " + t.id);
//				System.out.println(t.id + " has acquired read lock on " + dataItem);
//				releaseLocks(tWithWriteLock, dataItem, l);
//			}

		}
		if (!t.itemsLocked.contains(dataItem)) {
			t.itemsLocked.add(dataItem);
		}
		transactionTable.put(t.id, t);
		return l;
	}

	private static Lock writeWrite(Transaction t, String dataItem, Lock l) {
		Transaction tWithWriteLock = transactionTable.get(l.transactionIdWithWriteLock);
		applyWoundWait(tWithWriteLock, t, new Operation(OperationType.WRITE, dataItem), l);

//		if (tWithWriteLock.timestamp < t.timestamp) {
//			// tWithWriteLock will wound t. Put t in blocked state and restart transaction
//			t.state = State.BLOCKED;
//			Operation pendingOp = new Operation(OperationType.WRITE, dataItem);
//			t.pendingOperations.add(pendingOp);
//			transactionTable.put(t.id, t);
//			l.waitingList.add(t.id);
//			System.out.println("Transaction " + t.id + " was wounded by " + tWithWriteLock.id);
//			System.out.println("Current write operation is added as pending for " + t.id);
//			System.out.println("Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
//		} else {
//			// tWithWriteLock will be put to wait and the older process (t) will get the
//			// write lock
//			tWithWriteLock.state = State.ABORTED;
//			transactionTable.put(tWithWriteLock.id, tWithWriteLock);
//			l.transactionIdWithWriteLock = t.id;
//			System.out.println("Transaction " + tWithWriteLock.id + " has been blocked since " + t.id
//					+ " has higher timestamp and thus acquired the write lock.");
//			releaseLocks(tWithWriteLock, dataItem, l);
//		}

		return l;
	}

	private static void applyWoundWait(Transaction t1, Transaction t2, Operation pendingOperation, Lock l) {
		if (t1.timestamp < t2.timestamp) {
			// t1 will abort t2
			System.out.println("Transaction " + t1.id + " has aborted transaction " + t2.id);
			abortTransaction(t2);
		} else {
			// t1 will wait in blocked state
			t1.state = State.BLOCKED;
			t1.pendingOperations.add(pendingOperation);
			transactionTable.put(t1.id, t1);
			l.waitingList.add(t1.id);
			System.out.println(
					"Transaction " + t1.id + " is now waiting for " + pendingOperation + " on Transaction " + t2.id);
		}
	}

	private static void blockedTransaction(Transaction t, String dataItem, OperationType operationType) {
		// In blocked transaction we have to first check the lock table and if the lock
		// table contains data item then add the transactionId to the lock's waiting
		// list and insert into lock table
		// map.

		if (lockTable.containsKey(dataItem)) {
			Lock l = lockTable.get(dataItem);
			l.waitingList.add(t.id);
			lockTable.put(dataItem, l);
		}
		// If transaction's itemsLocked doesnt contain dataitem add it, add a pending
		// operation and insert in transaction tablemap.
		if (!t.itemsLocked.contains(dataItem)) {
			t.itemsLocked.add(dataItem);
		}
		t.pendingOperations.add(new Operation(operationType, dataItem));
		transactionTable.put(t.id, t);
		System.out.println("Transaction " + t.id + " is currently Blocked. Operation " + operationType + " on dataitem "
				+ dataItem + " is added to waiting list.");
		System.out.println("And transaction is added to lock table's waiting list");
	}

	private static void abortTransaction(Transaction t) {
		// set state to Aborted
		t.state = State.ABORTED;
		List<String> itemsLocked = t.itemsLocked;
		for (String item : itemsLocked) {
			// releaseLocks on all items locked by t
			System.out.println("Releasing item " + item + " locked by transaction " + t.id);
			releaseLocks(t, item, lockTable.get(item));
		}
		// update t in transactionTable
		transactionTable.put(t.id, t);
	}

	private static void releaseLocks(Transaction old, String dataItem, Lock l) {
		if (l == null) {
			return;
		}
		if (l.operationType == OperationType.WRITE || l.transactionIdsWithReadLock.size() == 1) {
			// only one other transaction with read or write lock on dataitem
			// need to trigger operations for transactions in the lock's waiting list
			PriorityQueue<Integer> waitingTransactions = l.waitingList;
			l.operationType = OperationType.UNKNOWN;
			if (l.transactionIdsWithReadLock.size() == 1) {
				System.out.println("Transaction " + old.id + " has released read lock on " + dataItem);
				l.transactionIdsWithReadLock.clear();
			} else {
				System.out.println("Transaction " + old.id + " has released write lock on " + dataItem);
			}
			lockTable.put(dataItem, l);
			if (waitingTransactions.isEmpty()) {
				lockTable.remove(dataItem);
			} else {
				while (!l.waitingList.isEmpty()) {
					int waitingTransactionId = l.waitingList.remove();
					Transaction t = transactionTable.get(waitingTransactionId);
					t = acquireLocks(t, dataItem, l);
					transactionTable.put(waitingTransactionId, t);
					if (t.state != Transaction.State.COMMITED) {
						return;
					}
				}
			}
			lockTable.remove(dataItem);
		} else if (l.operationType == OperationType.READ) {
			List<Integer> readers = l.transactionIdsWithReadLock;
			for (int i = 0; i < readers.size(); i++) {
				if (old.id == readers.get(i)) {
					readers.remove(i);
				}
			}
			System.out.println("Transaction " + old.id + " has release read lock on " + dataItem);
			lockTable.put(dataItem, l);
		}
	}

	private static Transaction acquireLocks(Transaction t, String dataItem, Lock l) {
		Queue<Operation> pendingOperations = t.pendingOperations;
		t.state = State.ACTIVE;
		transactionTable.put(t.id, t);

		if (!pendingOperations.isEmpty()) {
			System.out.println(
					"Transaction " + t.id + " has changed from blocked to active. Running its waiting operations...");
			while (!pendingOperations.isEmpty()) {
				Operation o = pendingOperations.remove();
				switch (o.operationType) {
				case READ:
				case WRITE:
					executeOperation(t.id, o.dataItem, o.operationType);
					break;
				case COMMIT:
					commitTransaction(t);
				case UNKNOWN:
				default:
					break;
				}
			}
		}

		lockTable.put(dataItem, l);

		return t;
	}

	private static void commitTransaction(Transaction t) {
		switch (t.state) {
		case ACTIVE:
			System.out.println("Commiting transaction and releasing its locks");
			t.state = State.COMMITED;
			List<String> itemsLocked = t.itemsLocked;
			for (String item : itemsLocked) {
				releaseLocks(t, item, lockTable.get(item));
			}
			System.out.println("Committed transaction and released its locks");
			break;
		case BLOCKED:
			t.pendingOperations.add(new Operation(OperationType.COMMIT, ""));
			transactionTable.put(t.id, t);
			System.out.println("Commit operation is added as pending for transaction " + t.id);
			break;
		case ABORTED:
		case COMMITED:
			System.out.println("Cannot commit since transaction is already " + t.state);
			break;
		case UNKNOWN:
		default:
			break;
		}
	}

}
