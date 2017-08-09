import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoogleIterateMapper extends Mapper<Text, GoogleNode, Text, GoogleNode>
{
    private final GoogleNode distanceNode = new GoogleNode();
    private Text key = new Text();

    protected void setup(Context context) throws IOException ,InterruptedException
    {
        distanceNode.setId(GoogleNode.DISTANCE_INFO);
    };
    public void map(Text nid, GoogleNode node,  Context context) throws IOException, InterruptedException
    {
        context.write(nid, node);

        if (node.getDistance()<Integer.MAX_VALUE)
        {
            context.getCounter(GoogleIterate.GoogleCounters.ReachableNodesAtMap).increment(1);

            String[] dest = node.getDest();
            int dist = node.getDistance()+1;

            for (int i = 0; i < dest.length; i++)
            {
                String neighbor = dest[i];
                distanceNode.setDistance(dist);

                key.set(neighbor);
                context.write(key,distanceNode);
            }
        }
    }

}