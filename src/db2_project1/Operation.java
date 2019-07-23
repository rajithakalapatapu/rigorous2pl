package db2_project1;

import db2_project1.Rigorous2pl.OperationType;

public class Operation {
	OperationType operationType;
	String dataItem;

	public Operation(OperationType operationType, String dataItem) {
		super();
		this.operationType = operationType;
		this.dataItem = dataItem;
	}

	@Override
	public String toString() {
		return "Operation [OperationType=" + operationType + ", dataItem=" + dataItem + "]";
	}

}
