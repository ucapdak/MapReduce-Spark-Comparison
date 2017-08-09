import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ControversyJoin
{
   public static void runJob(String[] input, String output, int reducers) throws Exception
   {
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(ControversyJoin.class);
        job.setMapperClass(ControversyJoinMapper.class);
        job.setReducerClass(ControversyJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(reducers);

        job.addCacheFile(new Path("/data/movie-ratings/movies.dat").toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
   }
   public static void main(String[] args) throws Exception
   {
        for (int i=2; i<11; i++)
        {
            runJob(Arrays.copyOfRange(args,0,args.length-1),args[args.length-1],i);
        }
   }
}