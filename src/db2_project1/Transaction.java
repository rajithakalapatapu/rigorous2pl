package db2_project1;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

	public class Operation {
		String action;
		String dataItem;
		
		public Operation(String action, String dataItem) {
			super();
			this.action = action;
			this.dataItem = dataItem;
		}

		@Override
		public String toString() {
			return "Operation [action=" + action + ", dataItem=" + dataItem + "]";
		}
		
		
	}

	public enum State {
		UNKNOWN,
		ACTIVE,
		BLOCKED,
		COMMIT,
		ABORT;
	}
	int id;
	int timestamp;
	State state;
	List<String> itemsLocked;
	List<Operation> pendingOperations;

	public Transaction(int id, int timestamp, State state) {
		super();
		this.id = id;
		this.timestamp = timestamp;
		this.state = state;
		this.itemsLocked=new ArrayList<>();
		this.pendingOperations= new ArrayList<>();
	}
	
	@Override
	public String toString() {
		return "Transaction [id=" + id + ", timestamp=" + timestamp + ", state=" + state + ", items=" + itemsLocked
				+ ", twait=" + pendingOperations + "]";
	}

	
}
