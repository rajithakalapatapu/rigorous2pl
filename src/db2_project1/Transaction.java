package db2_project1;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Transaction {

	public enum State {
		UNKNOWN, ACTIVE, BLOCKED, COMMITED, ABORTED;
	}

	int id;
	int timestamp;
	State state;
	List<String> itemsLocked;
	Queue<Operation> pendingOperations;

	public Transaction(int id, int timestamp, State state) {
		super();
		this.id = id;
		this.timestamp = timestamp;
		this.state = state;
		this.itemsLocked = new ArrayList<>();
		this.pendingOperations = new LinkedList<Operation>();
	}

	@Override
	public String toString() {
		return "Transaction [id=" + id + ", timestamp=" + timestamp + ", state=" + state + ", items=" + itemsLocked
				+ ", twait=" + pendingOperations + "]";
	}

}
