package org.oversea.db.cassandra.json.sorter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
/**
 * 
 * @author hungnt.it@gmail.com
 *
 * 
 */
public class SSTableJsonRowSerializer<T>  implements ExternalJsonMergeSort.Serializer<T>{
	private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    private final TypeReference<T> type;
    private final ObjectMapper mapper;

    public SSTableJsonRowSerializer(TypeReference<T> type) {
        this(type, DEFAULT_MAPPER);
    }

    public SSTableJsonRowSerializer(TypeReference<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    
    public void writeValues(Iterator<T> values, OutputStream out) throws IOException{
        long st = System.currentTimeMillis();
        JsonFactory jsonFactory = mapper.getJsonFactory();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(out);
        jsonGenerator.writeRaw("[\n");
        boolean writeSeperate = false;
        while (values.hasNext()) {
        	if(writeSeperate){
        		jsonGenerator.writeRaw(",\n");
        	}
            T next = values.next();
            jsonGenerator.writeObject(next);
            writeSeperate = true;
        }
        jsonGenerator.writeRaw("\n]");
        jsonGenerator.close();
        if (ExternalJsonMergeSort.debug) {
            System.out.println("W: " + (System.currentTimeMillis() - st) + "ms");
        }
    }

    
    public JsonParser readValues(InputStream input) throws IOException {
        JsonFactory jsonFactory = mapper.getJsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(input);
        return jsonParser;
    }
    
    public T readValue(JsonParser jsonParser){
    	try{
        JsonToken token = jsonParser.nextToken();
        if(token == JsonToken.START_ARRAY || token == JsonToken.END_ARRAY){
			token = jsonParser.nextToken();
		}
		if(token !=null  ){
			return jsonParser.readValueAs(type);
		}
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}
		return null;
    }
	
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
