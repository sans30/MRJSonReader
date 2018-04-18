package io.saagie.example.json;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by santhoshpc on 10-03-2018.
 */
public class JsonToAvroReducer
        extends Reducer<NullWritable, Text, NullWritable, Text> {

    private Logger logger = Logger.getLogger(JsonToAvroReducer.class);

    protected void reduce(NullWritable key, Text values, Context context)
            throws IOException, InterruptedException {
        logger.info("Value in Reducer ==>"+values.toString());
        context.write(key, values);
/*
        for (Text value : values) {
            Schema.Parser parser = new Schema.Parser();
            Schema mSchema = parser.parse(this.getClass().getResourceAsStream("/kindlereview.avsc"));

            byte[] avroByteArray = fromJasonToAvro(value.toString(),mSchema);


            //logger.info("BEGIN JSON==> "+value+" <== END JSON");
            //System.out.println("JSON String==> "+value);

            DatumReader<GenericRecord> reader1 = new GenericDatumReader<GenericRecord>(mSchema);

            Decoder decoder1 = DecoderFactory.get().binaryDecoder(avroByteArray, null);
            GenericRecord result = reader1.read(null, decoder1);
            context.write(new AvroKey<String>(key.toString()), new AvroValue<GenericRecord>(result));
        }
*/
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
