package db2_project1;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class Lock {

	String itemUnderLock;
	String state;
	int transactionIdWithWriteLock;
	List<Integer> transactionIdsWithReadLock;
	PriorityQueue<Integer> waitingList;
	
	public Lock(String itemUnderLock, String state, int transactionIdWithWriteLock) {
		super();
		this.itemUnderLock = itemUnderLock;
		this.state = state;
		this.transactionIdWithWriteLock = transactionIdWithWriteLock;
		this.transactionIdsWithReadLock = new ArrayList<>();
		this.waitingList = new PriorityQueue<>();
	}

	@Override
	public String toString() {
		return "Lock [itemUnderLock=" + itemUnderLock + ", state=" + state + ", transactionIdWithWriteLock="
				+ transactionIdWithWriteLock + ", transactionIdsWithReadLock=" + transactionIdsWithReadLock
				+ ", waitingList=" + waitingList + "]";
	}
	
	
}
