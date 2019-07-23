package db2_project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db2_project1.Transaction.State;

public class Rigorous2pl {
	static Map<Integer, Transaction> transactionTable = new HashMap<>();
	static Map<String, Lock> lockTable = new HashMap<>();
	static int timestamp = 0;

	enum OperationType {
		READ, WRITE;
	}

	public static void main(String[] args) {
		processFile();
	}

	private static void processFile() {
		try {
			FileReader fr = new FileReader("src/input.txt");
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

		System.out.println("Executing operation " + token + "....");
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
		System.out.println("Transaction with id " + transactionID + " has timestamp " + timestamp
				+ ". It's current state is " + state);

	}

	private static void readTransaction(int transactionID, String dataItem) {
		executeOperation(transactionID, dataItem, OperationType.READ);

	}

	private static void writeTransaction(int transactionID, String dataItem) {
		executeOperation(transactionID, dataItem, OperationType.WRITE);

	}

	private static void endTransaction(int transactionID) {
		// TODO: Complete this....
		System.out.println("end " + transactionID);

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
			System.out.println("Transaction " + transactionID + " is committed");
			break;
		case ABORTED:
			System.out.println("Transaction " + transactionID + " is aborted");
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
				System.out.println("The transaction " + t.id + " is active.");
				System.out.println("Transaction with id " + t.id + " has acquired Read lock on  " + dataItem);
				lock.transactionIdsWithReadLock.add(t.id);
				break;
			case WRITE:
				System.out.println("The transaction " + t.id + " is active.");
				System.out.println("Transaction with id " + t.id + " has acquired Write lock on  " + dataItem);
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
		} else {
			// this is not possible...
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
				if (old.timestamp < t.timestamp) {
					// old will Wound t, set t to blocked and restart t with same timestamp
					t.state = State.BLOCKED;
					Operation pendingOp = new Operation(OperationType.WRITE, dataItem);
					t.pendingOperations.add(pendingOp);
					transactionTable.put(t.id, t);
					l.waitingList.add(t.id);
					System.out.println("Transaction " + t.id + " was wounded by " + old.id);
					System.out.println("Current write operation is added as pending for " + t.id);
					System.out.println(
							"Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
				} else {
					// old will wait (in aborted state?)
					old.state = State.ABORTED;
					transactionTable.put(old.id, old);
					l.operationType = OperationType.WRITE;
					l.transactionIdsWithReadLock.clear();
					l.transactionIdWithWriteLock = t.id;
					System.out.println("Transaction " + old.id + " aborted since it was younger than " + t.id);
					System.out.println(t.id + " has now acquired write lock on " + dataItem);
					releaseLocks(old, dataItem);
				}
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
				for (int i = 0; i < readlist.size(); i++) {
					if (t.id >= readlist.get(i)) {
						// do not wound transactions that are older than t
						i++;
					}
					if (t.id < readlist.get(i)) {
						// only wound transactions that are younger than t
						readlist.remove(i);
						Transaction tOld = transactionTable.get(readlist.get(i));
						abortTransaction(tOld);
						System.out.println("Aborting transaction " + tOld.id);
					}
				}

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
			if (tWithWriteLock.timestamp < t.timestamp) {
				// tWithWriteLock will wound t. Put t in blocked state and restart transaction
				t.state = State.BLOCKED;
				Operation pendingOp = new Operation(OperationType.READ, dataItem);
				t.pendingOperations.add(pendingOp);
				transactionTable.put(t.id, t);
				l.waitingList.add(t.id);
				System.out.println("Transaction " + t.id + " was wounded by " + tWithWriteLock.id);
				System.out.println("Current read operation is added as pending for " + t.id);
				System.out
						.println("Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
			} else {
				// tWithWriteLock will be put to wait and the older process (t) will get the
				// read lock
				tWithWriteLock.state = State.ABORTED;
				transactionTable.put(tWithWriteLock.id, tWithWriteLock);

				l.transactionIdWithWriteLock = 0;
				l.operationType = OperationType.READ;
				l.transactionIdsWithReadLock.add(t.id);

				System.out.println("Transaction " + tWithWriteLock.id + " aborted because it is younger than " + t.id);
				System.out.println(t.id + " has acquired read lock on " + dataItem);
				releaseLocks(tWithWriteLock, dataItem);
			}

		}
		if (!t.itemsLocked.contains(dataItem)) {
			t.itemsLocked.add(dataItem);
		}
		transactionTable.put(t.id, t);
		return l;
	}

	private static Lock writeWrite(Transaction t, String dataItem, Lock l) {
		Transaction tWithWriteLock = transactionTable.get(l.transactionIdWithWriteLock);
		if (tWithWriteLock.timestamp < t.timestamp) {
			// tWithWriteLock will wound t. Put t in blocked state and restart transaction
			t.state = State.BLOCKED;
			Operation pendingOp = new Operation(OperationType.WRITE, dataItem);
			t.pendingOperations.add(pendingOp);
			transactionTable.put(t.id, t);
			l.waitingList.add(t.id);
			System.out.println("Transaction " + t.id + " was wounded by " + tWithWriteLock.id);
			System.out.println("Current write operation is added as pending for " + t.id);
			System.out.println("Lock for item " + dataItem + " was updated to add " + t.id + " to its waiting list");
		} else {
			// tWithWriteLock will be put to wait and the older process (t) will get the
			// write lock
			tWithWriteLock.state = State.ABORTED;
			transactionTable.put(tWithWriteLock.id, tWithWriteLock);
			l.transactionIdWithWriteLock = t.id;
			System.out.println("Transaction " + tWithWriteLock.id + " has been blocked since " + t.id
					+ " has higher timestamp and thus acquired the write lock.");
			releaseLocks(tWithWriteLock, dataItem);
		}
		return l;
	}

	private static void blockedTransaction(Transaction t, String dataItem, OperationType operationType) {
		// TODO: Complete this....
		// In blocked transaction we have to first check the lock table and if the lock
		// table contains data item then
		// add the transactionId to the lock's waiting list and insert into lock table
		// map.
		// If transaction's itemsLocked doesnt contain dataitem add it, add a pending
		// operation and insert in transaction tablemap.
	}

	private static void abortTransaction(Transaction t) {
		// TODO: Complete this....
		// set state to Aborted
		// releaseLocks on all items locked by t
		// update t in transactionTable
	}

	private static void releaseLocks(Transaction old, String dataItem) {
		// TODO: Complete this....
	}

}
