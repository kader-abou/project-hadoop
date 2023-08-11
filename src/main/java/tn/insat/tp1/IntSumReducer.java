package tn.insat.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException

    {
        
        Iterator<IntWritable> i = values.iterator();
        int count = 0; 
        while (i.hasNext()) 
            count += i.next().get(); 
    
        context.write(key, new IntWritable(count));

    }

}