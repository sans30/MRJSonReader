package io.saagie.example.json;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
 * MapReduce jobs are typically implemented by using a driver class,
 * which sets up the configuration and then submits the job to the
 * Hadoop cluster for execution.  Typical tasks performed in the
 * driver class include configuring the input and output data formats,
 * configuring the map and reduce classes, and specifying the types
 * of intermediate data produced by the job.
 */
public class ExampleDriver {

    public void run(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //Schema.Parser parser = new Schema.Parser();
        //Schema mSchema = parser.parse(this.getClass().getResourceAsStream("/kindlereview.avsc"));

        Job job = Job.getInstance();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(ExampleDriver.class);
        job.setJobName("Kindle Review Write Text only in Map");

        job.setInputFormatClass(JsonInputFormat.class);
        //job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(JsonToAvroMapper.class);
        //job.setNumReduceTasks(0);
        job.setReducerClass(JsonToAvroReducer.class);

        //AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        job.setMapOutputKeyClass(NullWritable.class);
        //AvroJob.setMapOutputValueSchema(job, mSchema);
        job.setMapOutputValueClass(Text.class);

        //job.setMapOutputValueClass(Text.class);

        //AvroJob.setOutputKeySchema(job,Schema.create(Schema.Type.STRING));
        //AvroJob.setOutputKeySchema(job, mSchema);

      /*
       * Specify the job's output key and value classes.
       */


      /*
       * Start the MapReduce job and wait for it to finish.
       * If it finishes successfully, return 0. If not, return 1.
       */

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        ExampleDriver driver = new ExampleDriver();
        driver.run(args);
    }

    private static Job addJarToDistributedCache(
            String jarName, Job job)
            throws IOException, URISyntaxException {

        // Retrieve jar file for class2Add
/*
        String jar = classToAdd.getProtectionDomain().
                getCodeSource().getLocation().
                getPath();
        File jarFile = new File(jar);
*/

        job.addCacheFile(new URI("/user/hdfs/lib/"
                + jarName));
        return job;
    }

}