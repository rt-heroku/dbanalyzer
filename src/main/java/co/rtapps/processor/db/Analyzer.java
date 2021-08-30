package co.rtapps.processor.db;

import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import co.rtapps.processor.repositories.GenericTableRepository;
import co.rtapps.processor.util.MetadataExtractor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Analyzer {

	@Autowired
	GenericTableRepository genericTableRepository;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	public void printTableInfo(String db, String table) {
		try {
			MetadataExtractor metadataExtractor = new MetadataExtractor(jdbcTemplate);
			log.info("\nSCHEMA: " + db + ", TABLE: " + table);
			metadataExtractor.extractPrimaryKeys(db, table);
			metadataExtractor.extractColumnInfo(db, table);
			log.info("\n\n\nSCHEMA: " + db + ", TABLE: " + table);
			metadataExtractor.extractAllTablesWithSameNameInfo(table);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void printDatabaseInfo(){
		
		try {
			MetadataExtractor metadataExtractor = new MetadataExtractor(jdbcTemplate);
			metadataExtractor.extractDatabaseInfo();
			metadataExtractor.extractSupportedFeatures();
			metadataExtractor.extractUserName();
			metadataExtractor.extractSystemTables();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public Table getTableInfo(String db, String table) {
		try {
			MetadataExtractor metadataExtractor = new MetadataExtractor(jdbcTemplate);
			Table t = metadataExtractor.extractTableFromSchema(db, table);
			return t;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void analyzeTable() {
		// 1. Find table in that schema
		// 2. Should I create the new schema if table is not found there?
		// 
	}
	
}
