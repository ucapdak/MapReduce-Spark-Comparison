import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoogleMapper extends Mapper<LongWritable, Text, Text, GoogleNode>
{

    private GoogleNode node = new GoogleNode();
    private String sourceId;
    private Text nodeId = new Text();

    protected void setup(Context context) throws IOException ,InterruptedException
    {
        Configuration conf = context.getConfiguration();
        sourceId = conf.get("SOURCENODE");
    };
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try
        {
            String[] parts = value.toString().split("[^0-9]");
            String id = parts[0];

            String dest;

            if (parts.length == 2)
            {
                dest  = parts[1];
                int distance = Integer.MAX_VALUE;

                if (id.equals(sourceId))
                {
                    distance = 0;
                }

                nodeId.set(id);
                node.set(id,new String[]{dest},distance);
                context.write(nodeId,node);
            }
        }
        catch (NumberFormatException e)
        {
            context.getCounter(GoogleExtractor.Counters.FaultyEntries).increment(1);
        }
        catch (NullPointerException e)
        {
            context.getCounter(GoogleExtractor.Counters.FaultyEntries).increment(1);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            context.getCounter(GoogleExtractor.Counters.FaultyEntries).increment(1);
        }
    }
}
