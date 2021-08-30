package co.rtapps.processor.db;

public class Column {

	private String  name;
	private Integer dataType;
	private String  typeName;
	private Integer size;
	private Boolean nullable;
	private Boolean autoIncrement;
	private Boolean primaryKey;
	
	public Column() {
		super();
	}
	
	public Column(String name, Integer dataType, String typeName, Integer size, Boolean nullable,
			Boolean autoIncrement) {
		super();
		this.name = name;
		this.dataType = dataType;
		this.typeName = typeName;
		this.size = size;
		this.nullable = nullable;
		this.autoIncrement = autoIncrement;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getDataType() {
		return dataType;
	}
	public void setDataType(Integer dataType) {
		this.dataType = dataType;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	public Integer getSize() {
		return size;
	}
	public void setSize(Integer size) {
		this.size = size;
	}
	public Boolean isNullable() {
		return nullable;
	}
	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}
	public Boolean isAutoIncrement() {
		return autoIncrement;
	}
	public void setAutoIncrement(Boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}
	public Boolean isPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(Boolean primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	@Override
	public String toString(){
		return String.format("ColumnName: %s, columnSize: %s, datatype: %s, isColumnNullable: %s, isAutoIncrementEnabled: %s, isPrimaryKey: %s", 
							 name, size, dataType, isNullable(), isAutoIncrement(), isPrimaryKey());
	}
	
}
