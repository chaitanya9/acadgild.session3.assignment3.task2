import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class rmduplicaterecordmapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        //loop through each line of the television text file
        String[] linesArray = value.toString().split("  ");

        //print each line of the file to output -- for debugging
       /* for(String s: linesArray){
            System.out.println(s);}*/
       //loop through each column in the line to find the invalid company and product names
        for(String line : linesArray){
            String[] lineArray = line.split("\\|");
            /*for(String s: lineArray){
                System.out.println(s);} -- for debuggin purpose */
            Text company = new Text(lineArray[0]);
            Text product = new Text(lineArray[1]);
            Text state = new Text(lineArray[3]);
            // Remove lines which have company or product as "NA"
            if(!company.equals(new Text("NA"))){
                if(!product.equals(new Text("NA"))){
                    if(company.equals(new Text("Onida"))) {
                        context.write(state, one);
                    }
            }}
        }
        }

    }

