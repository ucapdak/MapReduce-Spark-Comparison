import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GoogleExtractor
{
    static enum Counters
    {
        FaultyEntries
    };
    public static void runJob(String[] input, String output) throws Exception
    {
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        conf.set("SOURCENODE","1234");

        job.setJarByClass(GoogleExtractor.class);
        job.setMapperClass(GoogleMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job,SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GoogleNode.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws Exception
    {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1),args[args.length - 1]);
    }
}
