package co.rtapps.processor.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.rtapps.processor.db.Analyzer;
import co.rtapps.processor.db.Column;
import co.rtapps.processor.db.Table;
import co.rtapps.processor.message.MessageData;
import co.rtapps.processor.message.MessagePayload;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DbAnalyzerService {

	@Autowired
	private Analyzer analyzer;
	
	public void printDatabaseInfo(){
	
		log.info("Printing Database Information:");
		analyzer.printDatabaseInfo();
	
	}

	public void printTableInfo(String schema, String table){
		
		log.info("Analyzing Table: " + schema + "."+ table);
		Table t = analyzer.getTableInfo(schema, table);
		log.info("Table definition: " + t.toString());
	}
	
	
	public Table getTable(String schema, String table) {
		Table t = analyzer.getTableInfo(schema, table);
		
		return t;
	}
	
	public void analyze(MessagePayload payload) {
		Table t =  analyzer.getTableInfo(payload.getDatabase(), payload.getTable());

		for (MessageData m: payload.getMessageData()) {
			
			log.info("\t column: " + m.getKey() + ",\tType: " + m.getDataType());
			Column c = t.getColumn(m.getKey().toLowerCase());
			
			if (c== null) {
				log.info(String.format("Column %s NOT found!", m.getKey().toLowerCase() + ",\tType: " + m.getDataType()));
				continue;
			}
			log.info("\tDB Name: " + c.getName() + ",\tType: " + c.getTypeName() + ",\tData Type: " + c.getDataType());
			
		}
	}
	
}
