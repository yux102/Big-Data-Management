package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// this is the second version of utilizing an in-mapper combining approach

public class WordAvgLen2
{
    // Create a pair, copy from ppt.
    // 
    public static class IntPair implements Writable
    {
        private int first, second;
        
        public IntPair() {}
        
        public IntPair(int first, int second)
        {
            set(first, second);
        }
        
        public void set(int left, int right)
        {
            first = left;
            second = right;
        }
        
        public int getFirst()
        {
            return first;
        }
        
        public int getSecond()
        {
            return second;
        }
        
        public void write(DataOutput out) throws IOException
        {
            out.writeInt(first);
            out.writeInt(second);
        }
        
        public void readFields(DataInput in) throws IOException
        {
            first = in.readInt();
            second = in.readInt();
        }
    }
    
    // mapper got a hashmap, 
    // store the first letter as key, 
    // occur times and sum of length as IntPair as value
    // 
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntPair>
    {
        private Text word = new Text();
        private HashMap<String, IntPair> map = new HashMap<String, IntPair>();
        
        // in setup, need to init the hashmap, however it is done.
        // 
        public void setup(Context context) {}
        
        // add first letter and occur times and length
        // we don't write it to the context in this step
        // we write it as hashmap into memory
        // 
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
            
            while (itr.hasMoreTokens()) 
            {
                String term = itr.nextToken().toLowerCase();
                char first_char = term.charAt(0);
                
                if ('a' <= first_char && first_char <= 'z')
                {
                    String first_letter = String.valueOf(first_char);
                    
                    if (map.containsKey(first_letter) == true)
                    {
                        IntPair temp = map.get(first_letter);
                        temp.set(temp.getFirst()+1, temp.getSecond()+term.length());
                        map.put(first_letter, temp);
                    }
                    else
                    {
                        IntPair temp = new IntPair();
                        temp.set(1, term.length());
                        map.put(first_letter, temp);
                    }
                }
            }
        }
        
        // finally, we write (word, IntPair) into the context
        // finally, we write (word, (total count, total length)) into the context
        //
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            for (String first_letter : map.keySet())
            {
                word.set(first_letter);
                context.write(word, map.get(first_letter));
            }
        }
    }
    
    // reducer get each summed length divide the summed counts
    // then we get the avg length
    // 
    public static class DoubleSumReducer extends Reducer<Text,IntPair,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntPair> values, Context context) 
                throws IOException, InterruptedException 
        {
            int sum_length = 0;
            int count = 0;
            for (IntPair value : values) 
            {
                sum_length += value.getSecond();
                count += value.getFirst();
            }
            
            double avg = (sum_length * 1.0) / (count * 1.0);
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordAvgLen2");
        
        job.setJarByClass(WordAvgLen2.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(Combiner.class);
        job.setReducerClass(DoubleSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPair.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
