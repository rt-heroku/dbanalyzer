package co.rtapps.processor.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

	public Table(String name, String schema) {
		super();
		this.name = name;
		this.schema = schema;
		columns = new HashMap<String,Column>();
		primaryKey = new PrimaryKey();
	}

	private String name;
	private String schema;

	private PrimaryKey primaryKey;
	private Map<String, Column> columns;
	
	public Table() {
		columns = new HashMap<String,Column>();
		primaryKey = new PrimaryKey();
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public PrimaryKey getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(PrimaryKey primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public List<Column> getColumns() {
		return new ArrayList<Column>(columns.values());
	}
	
	public Column getColumn(String name) {
		return columns.get(name);
	}
	
	public void setColumns(List<Column> cols) {
		for (Column c: cols) 
			addColumn(c);
	}
	
	public void addColumn(Column column) {
		this.columns.put(column.getName(), column);
	}
	
	@Override
	public String toString(){
		return String.format("Schema: %s, Name: %s, Number Of Columns: %s, Columns:[%s] ", 
							 schema, 
							 name, 
							 columns.size(), 
							 getColumnNames()
							);
	}
	private String getColumnNames() {
		String r = "";
		for (String s: columns.keySet()) {
			r = r + "," + s;
		}
		return r;
	}
}
