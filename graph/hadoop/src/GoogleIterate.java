import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GoogleIterate
{
    static enum GoogleCounters
    {
        ReachableNodesAtMap, ReachableNodesAtReduce
    };
    public static boolean runJob(String input, String output, int reducers) throws Exception {

        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        conf.set("mapreduce.child.java.opts","-Xmx2048m");

        job.setJarByClass(GoogleIterate.class);

        job.setMapperClass(GoogleIterateMapper.class);
        job.setReducerClass(GoogleIterateReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GoogleNode.class);

        job.setNumReduceTasks(reducers);

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);

        long c1 = job.getCounters().findCounter(GoogleCounters.ReachableNodesAtMap).getValue();
        long c2 = job.getCounters().findCounter(GoogleCounters.ReachableNodesAtReduce).getValue();

        return c1!=c2;
    }
    public static void main(String[] args) throws Exception
    {
        for (int j=2; j<=10; j++)
        {
            Boolean iterate = runJob("GoogleBFS/iter-00","GoogleBFS/iter-01",j);

            int i = 2;
            String dir = "GoogleBFS/iter-";

            while(iterate)
            {
                iterate = runJob(dir+numberString(i-1),dir+numberString(i),j);
                i++;
            }
        }
    }
    public static String numberString(int i)
    {
        if (i<10)
        {
            return "0"+i;
        }
        return ""+i;
    }
}
