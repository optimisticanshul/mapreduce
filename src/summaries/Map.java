package summaries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InterruptedIOException;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedIOException {

        String line = value.toString();
        String[] data = line.split(",");

        try {
            String maritalStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);
            
            context.write(new Text(maritalStatus), new DoubleWritable(hrs));
        } catch (Exception e){

        }
    }

}