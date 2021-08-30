package co.rtapps.processor.db;

import java.util.List;

import lombok.Data;

@Data
public class PrimaryKey {

	private String name;
	private List<Column> columns;
	
	public void addPrimaryKeyColumn(Column column) {
		columns.add(column);
	}
	
}
