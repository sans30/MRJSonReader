package io.saagie.example.json;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.*;


/**
 * Created by santhoshpc on 10-03-2018.
 */
public class JsonToAvroMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Logger logger = Logger.getLogger(JsonToAvroMapper.class);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String json = value.toString();
        //Schema.Parser parser = new Schema.Parser();
        //Schema mSchema = parser.parse(this.getClass().getResourceAsStream("/kindlereview.avsc"));

        //byte[] avroByteArray = fromJasonToAvro(json,mSchema);


        //logger.info("BEGIN JSON==> "+value+" <== END JSON");
        //System.out.println("JSON String==> "+value);

        //DatumReader<GenericRecord> reader1 = new GenericDatumReader<GenericRecord>(mSchema);

        //Decoder decoder1 = DecoderFactory.get().binaryDecoder(avroByteArray, null);
        //GenericRecord result = reader1.read(null, decoder1);
        //logger.info("Json String: "+json);
        //logger.info("Prod Id: "+result.get("asin").toString());
/*        if(result.get("reviewerName")==null)
            logger.info("Reviewer name is null for :"+result.get("reviewerName").toString());*/
        //logger.info(result.get("asin").toString());
        if (json.contains("reviewerID") &&
                json.contains("reviewerName") &&
                json.contains("asin") &&
                json.contains("reviewerName") &&
                json.contains("helpful") &&
                json.contains("reviewText") &&
                json.contains("overall") &&
                json.contains("summary")&&
                json.contains("unixReviewTime") &&
                json.contains("reviewTime")) {
            logger.info("Valid Json: "+json);
            //context.write(key, new Text(json));
            context.write(NullWritable.get(), new Text(json));
        }
        //context.write(NullWritable.get(), value);
        //logger.info("Written "+result.get("asin").toString()+" to buffer.");
    }

    private byte[] fromJasonToAvro(String json, Schema schemastr) throws IOException {

        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);

        Schema schema = schemastr;

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        Object datum = reader.read(null, decoder);


        GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }
}
