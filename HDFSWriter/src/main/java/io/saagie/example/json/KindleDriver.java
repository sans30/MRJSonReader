package io.saagie.example.json;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by santhoshpc on 08-03-2018.
 */


public class KindleDriver extends Configured implements Tool {

    public int run(String[] args){
        try {
            Job job = new Job(getConf(),"Kindle Review Analysis");
            job.setJarByClass(KindleDriver.class);
            job.setMapperClass(null);
            job.setReducerClass(null);
            job.setInputFormatClass(JsonInputFormat.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;


/*        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}
