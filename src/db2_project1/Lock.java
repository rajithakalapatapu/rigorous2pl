package db2_project1;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import db2_project1.Rigorous2pl.OperationType;

public class Lock {

	String itemUnderLock;
	OperationType operationType;
	int transactionIdWithWriteLock;
	List<Integer> transactionIdsWithReadLock;
	PriorityQueue<Integer> waitingList;

	public Lock(String itemUnderLock, OperationType operationType) {
		super();
		this.itemUnderLock = itemUnderLock;
		this.operationType = operationType;
		this.transactionIdWithWriteLock = 0;
		this.transactionIdsWithReadLock = new ArrayList<>();
		this.waitingList = new PriorityQueue<>();
	}

	@Override
	public String toString() {
		return "Lock [itemUnderLock=" + itemUnderLock + ", operationType=" + operationType
				+ ", transactionIdWithWriteLock=" + transactionIdWithWriteLock + ", transactionIdsWithReadLock="
				+ transactionIdsWithReadLock + ", waitingList=" + waitingList + "]";
	}

}
