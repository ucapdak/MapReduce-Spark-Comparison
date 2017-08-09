import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GoogleIterateReducer extends Reducer<Text, GoogleNode, Text, GoogleNode>
{
    private GoogleNode resultNode = null;

    public void reduce(Text nid, Iterable<GoogleNode> values, Context context) throws IOException, InterruptedException
    {
        int min = Integer.MAX_VALUE;

        for (GoogleNode node : values)
        {
            if (min>node.getDistance())
            {
                min = node.getDistance();
            }
            if(!node.getId().equals(GoogleNode.DISTANCE_INFO))
            {
                resultNode = node;
            }
        }
        if(min<Integer.MAX_VALUE)
        {
            context.getCounter(GoogleIterate.GoogleCounters.ReachableNodesAtReduce).increment(1);
        }

        resultNode.setDistance(min);
        context.write(nid, resultNode);
    }
}
