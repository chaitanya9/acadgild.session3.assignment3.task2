import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

//Reducer to calculate total units sold for each company
public class rmduplicaterecordreducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        //sum total products for each company
        Iterator<IntWritable> valuesIt = values.iterator();
        while(valuesIt.hasNext()){
            sum = sum + valuesIt.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}