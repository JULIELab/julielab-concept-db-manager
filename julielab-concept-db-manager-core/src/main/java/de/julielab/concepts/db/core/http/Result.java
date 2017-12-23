package de.julielab.concepts.db.core.http;

import java.util.List;

public class Result {

	private List<String> columns;
	private List<Data> data;
	

 
	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<Data> getData() {
		return data;
	}
	
	public Data getData(int i) {
		return data.get(i);
	}

	@Override
	public String toString() {
		return "Result [columns=" + columns + ", data=" + data + "]";
	}

	public void setData(List<Data> data) {
		this.data = data;
	}



}
