import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;


public class ControversyJoinReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    DoubleWritable controversy = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> ratings, Context context) throws IOException, InterruptedException
    {
        int positive = 0;
        int negative = 0;

        for (DoubleWritable rating : ratings)
        {
            if (rating.get()>=2.5)
            {
                positive++;
            }
            else
            {
                negative++;
            }
        }

        if (negative+positive>=10)
        {
            Double controversyRatio;

            if ((positive==0)||(negative==0))
            {
                return;
            }
            else if (positive>negative)
            {
                controversyRatio = positive*1.0 / negative;
            }
            else
            {
                controversyRatio = negative*1.0 / positive;
            }

            Double controversyAbs = 1 - controversyRatio;
            controversyAbs = Math.abs(controversyAbs);
            controversy.set(controversyAbs);

            context.write(key,controversy);
        }

    }
}
