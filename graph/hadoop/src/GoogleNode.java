import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class GoogleNode implements WritableComparable<GoogleNode>
{
    public static String DISTANCE_INFO = "DISTANCE_INFORMATION_ONLY";

    private Text id;
    private ArrayWritable dest;
    private IntWritable distance;

    public GoogleNode()
    {
        ArrayWritable aw = new ArrayWritable(Text.class);
        aw.set(new Writable[0]);
        set(new Text(), aw, new IntWritable());
    }
    public GoogleNode(Text id, ArrayWritable dest, IntWritable distance)
    {
        set(id, dest, distance);
    }
    public void set(Text id, ArrayWritable dest, IntWritable distance)
    {
        this.id = id;
        this.dest = dest;
        this.distance = distance;
    }
    public void set(String id, String[] dest, int distance)
    {
        this.id.set(id);
        Text[] values = new Text[dest.length];

        for (int i = 0; i < dest.length; i++)
        {
            values[i] = new Text(dest[i]);
        }

        this.dest.set(values);
        this.distance.set(distance);
    }
    public void setId(String id)
    {
        this.id.set(id);
    }
    public void setDistance(int distance)
    {
        this.distance.set(distance);
    }
    public String getId()
    {
        return id.toString();
    }
    public String[] getDest()
    {
        Writable[] arr = (Writable[]) dest.get();
        String[] ldest = new String[arr.length];

        for (int i = 0; i < arr.length; i++)
        {
            ldest[i] = ((Text) arr[i]).toString();
        }

        return ldest;
    }
    public int getDistance()
    {
        return distance.get();
    }
    @Override
    public void write(DataOutput out) throws IOException
    {
        id.write(out);
        dest.write(out);
        distance.write(out);

    }
    @Override
    public void readFields(DataInput in) throws IOException
    {
        id.readFields(in);
        dest.readFields(in);
        distance.readFields(in);
    }
    @Override
    public int compareTo(GoogleNode node)
    {
        String self = this.getId();
        return self.compareTo(node.getId());
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dest == null) ? 0 : dest.hashCode());
        result = prime * result + ((distance == null) ? 0 : distance.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GoogleNode)
        {
            GoogleNode node = (GoogleNode) obj;
            return this.getId().equals(node.getId());
        }
        return false;
    }
}