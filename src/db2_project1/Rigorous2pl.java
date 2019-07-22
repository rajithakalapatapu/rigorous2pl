package db2_project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
		System.out.println("read " + transactionID);
		System.out.println(dataItem);

		executeOperation(transactionID, dataItem, OperationType.READ);

	}

	private static void writeTransaction(int transactionID, String dataItem) {
		System.out.println("write " + transactionID);
		System.out.println(dataItem);

		executeOperation(transactionID, dataItem, OperationType.WRITE);

	}

	private static void endTransaction(int transactionID) {
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
		case COMMIT:
			System.out.println("Transaction " + transactionID + " is committed");
			break;
		case ABORT:
			System.out.println("Transaction " + transactionID + " is aborted");
			break;
		case UNKNOWN:
		default:
			System.out.println("Transaction " + transactionID + " has unknown state");
			break;
		}
	}

	private static void activeTransaction(Transaction t, String dataItem, OperationType operationType) {

	}

	private static void blockedTransaction(Transaction t, String dataItem, OperationType operationType) {
		// In blocked transaction we have to first check the lock table and if the lock
		// table contains data item then
		// add the transactionId to the lock's waiting list and insert into lock table
		// map.
		// If transaction's itemsLocked doesnt contain dataitem add it, add a pending
		// operation and insert in transaction tablemap.
	}

}
