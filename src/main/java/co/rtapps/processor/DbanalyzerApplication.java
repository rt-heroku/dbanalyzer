package co.rtapps.processor;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.rtapps.processor.message.MessagePayload;
import co.rtapps.processor.service.DbAnalyzerService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class DbanalyzerApplication implements CommandLineRunner{

	private String MESSAGE = "{\"database\":\"av3\",\"table\":\"testsamples\",\"type\":\"insert\",\"ts\":1630016099,\"xid\":24770,\"xoffset\":997,\"server_id\":1,\"data\":{\"Id2\":1790036,\"Id\":1790036,\"RunId\":4,\"LaneId\":\"*\",\"SampNo\":980,\"SampTime\":\"2020-11-22 01:08:11\",\"nDetected\":0,\"nPassed\":0,\"nMarginal\":0,\"nRejected\":0,\"WidthAverage\":0.0,\"WidthFirst\":0.0,\"WidthLast\":0.0,\"WidthMin\":0.0,\"WidthMax\":0.0,\"HeightAverage\":0.0,\"HeightFirst\":0.0,\"HeightLast\":0.0,\"HeightMin\":0.0,\"HeightMax\":0.0,\"DMajorAverage\":0.0,\"DMajorFirst\":0.0,\"DMajorLast\":0.0,\"DMajorMin\":0.0,\"DMajorMax\":0.0,\"DMinorAverage\":0.0,\"DMinorFirst\":0.0,\"DMinorLast\":0.0,\"DMinorMin\":0.0,\"DMinorMax\":0.0,\"DAvgAverage\":0.0,\"DAvgFirst\":0.0,\"DAvgLast\":0.0,\"DAvgMin\":0.0,\"DAvgMax\":0.0,\"EFAverage\":0.0,\"EFFirst\":0,\"EFLast\":0,\"EFMin\":0,\"EFMax\":0,\"EDAverage\":0.0,\"EDFirst\":0,\"EDLast\":0,\"EDMin\":0,\"EDMax\":0,\"HAAverage\":0.0,\"HAFirst\":0.0,\"HALast\":0.0,\"HAMin\":0.0,\"HAMax\":0.0,\"ShapeAverage\":0.0,\"ShapeFirst\":0.0,\"ShapeLast\":0.0,\"ShapeMin\":0.0,\"ShapeMax\":0.0,\"ToastAverage\":0.0,\"ToastFirst\":0,\"ToastLast\":0,\"ToastMin\":0,\"ToastMax\":0,\"RawAverage\":0.0,\"RawFirst\":0,\"RawLast\":0,\"RawMin\":0,\"RawMax\":0,\"TransAverage\":0.0,\"TransFirst\":0,\"TransLast\":0,\"TransMin\":0,\"TransMax\":0,\"LanePosAverage\":0.0,\"LanePosFirst\":0.0,\"LanePosLast\":0.0,\"LanePosMin\":0.0,\"LanePosMax\":0.0,\"nDMajorMin\":0,\"nDMajorMax\":0,\"nDMinorMin\":0,\"nDMinorMax\":0,\"nDAvgMin\":0,\"nDAvgMax\":0,\"nEFMax\":0,\"nEDMax\":0,\"nHAMax\":0,\"nShapeMax\":0,\"nRawMax\":0,\"nTransMax\":0,\"nLaneMin\":0,\"nLaneMax\":0,\"SyncUp\":null}}";
	
	@Autowired
	DbAnalyzerService dbAnalyzerService;
	
	public static void main(String[] args) {
		SpringApplication.run(DbanalyzerApplication.class, args);
	}

	
	@Override
    public void run(String... args) {
        log.info("EXECUTING : DbAnalyzer!");
 
        for (int i = 0; i < args.length; ++i) {
            log.info("args[{}]: {}", i, args[i]);
        }
        
        dbAnalyzerService.printTableInfo("av3", "testsamples");
       
        try {
	        ObjectMapper mapper = new ObjectMapper();
	        MessagePayload payload = mapper.readValue(MESSAGE, new TypeReference<MessagePayload>() {});
	        log.info(payload.toString());
	        dbAnalyzerService.analyze(payload);
        }catch (IOException e) {
        	e.printStackTrace();
        }
        
    }

}
