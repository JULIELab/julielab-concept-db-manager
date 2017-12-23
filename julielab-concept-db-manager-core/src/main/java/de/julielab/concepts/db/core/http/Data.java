package de.julielab.concepts.db.core.http;

import java.util.List;

public class Data {
	@Override
	public String toString() {
		return "Data [row=" + row + ", meta=" + meta + "]";
	}
	private List<Object> row;
	private List<Meta> meta;
	public List<Object> getRow() {
		return row;
	}
	
	public Object getRow(int i) {
		return row.get(i);
	}
	public void setRow(List<Object> row) {
		this.row = row;
	}
	public List<Meta> getMeta() {
		return meta;
	}
	
	public Meta getMeta(int i) {
		return meta.get(i);
	}
	public void setMeta(List<Meta> meta) {
		this.meta = meta;
	}
}
