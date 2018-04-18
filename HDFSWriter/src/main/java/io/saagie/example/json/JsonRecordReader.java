package io.saagie.example.json;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by santhoshpc on 08-03-2018.
 */
public class JsonRecordReader extends RecordReader<LongWritable, Text> {

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream fsin = null;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    public static final String START_TAG_KEY = "json.start";
    public static final String END_TAG_KEY = "json.end";
    private LongWritable key;
    private Text value;

    public JsonRecordReader(String startTag, String endTag){
        this.startTag =   startTag.getBytes(Charsets.UTF_8);
        this.endTag =   endTag.getBytes(Charsets.UTF_8);
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration job = taskAttemptContext.getConfiguration();
/*
        startTag = job.get(START_TAG_KEY).getBytes("utf-8");
        endTag = job.get(END_TAG_KEY).getBytes("utf-8");
*/
        FileSplit split = (FileSplit)inputSplit;
        // open the file and seek to the start of the split
        start = split.getStart();
        end = start + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        fsin = fs.open(split.getPath());
        fsin.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(this.key == null) {
            this.key = new LongWritable();
        }
        if(this.value == null) {
            this.value = new Text();
        }

        if (fsin.getPos() < end) {
            if (readUntilMatch(startTag, false)) {
                try {
                    buffer.write(startTag);
                    if (readUntilMatch(endTag, true)) {
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;

    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fsin.read();
            // end of file:
            if (b == -1) return false;
            // save to buffer:
            if (withinBlock) buffer.write(b);

            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length) return true;
            } else i = 0;
            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (fsin.getPos() - start) / (float) (end - start);
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }
}
