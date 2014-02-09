package org.choa.hadoop.HL7Parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;


/**
 * Created with IntelliJ IDEA.
 * User: jholoman
 * Date: 2/9/14
 * Time: 10:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class HL7Parser {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("HL7Parser <inputPath> <outputPath>");
            System.out.println();
            System.out
                    .println("Example: HL7Parser ./input ./output ");
            return;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        //conf.set("xmlinput.start", "MSH");
        //conf.set("xmlinput.end", Character.toString((char) 11));
        // Create job

        Job job = new Job(conf);
        job.setJobName("HL7Parser");

        job.setJarByClass(HL7Parser.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);

        //Set the input format
        job.setInputFormatClass(XMLInputFormat.class);
        job.setMapperClass(HL7Mapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

    }
}














