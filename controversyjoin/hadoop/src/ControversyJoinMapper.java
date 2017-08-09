import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ControversyJoinMapper extends Mapper<Object, Text, Text, DoubleWritable>
{

    private Hashtable<String, String> movieInfo;
    private Text movieTitle = new Text();
    private DoubleWritable movieRating = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] parts = line.split("::");

        String name = movieInfo.get(parts[1]);

        movieRating.set(Double.parseDouble(parts[2]));

        if (name != null)
        {
            movieTitle.set(name);
            context.write(movieTitle,movieRating);
        }
        else
        {
            movieTitle.set("No Matching Title: "+parts[1]);
            context.write(movieTitle,movieRating);
        }
    }
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        movieInfo = new Hashtable<String,String>();

        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;

        try
        {
            br.readLine();

            while ((line = br.readLine()) != null)
            {
                String[] fields = line.split("::");
                movieInfo.put(fields[0],fields[1]);
            }

            br.close();
        }
        catch (IOException e1) {}

        super.setup(context);
    }

}
