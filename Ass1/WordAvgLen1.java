package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

// this is the first version of utilizing a combiner approach
// 

public class WordAvgLen1
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
    
    // mapper set the first lowercase letter as key, pair of (1, length) as value.
    // 1 means this word counts once.
    // this can be sum up latter.
    // 
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntPair>
    {
        private IntPair pair = new IntPair();
        private Text word = new Text();
    
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
                    word.set(first_letter);
                    pair.set(1, term.length());
                    context.write(word, pair);
                }
            }
        }
    }
    
    // reducer sum each value length for each key first_char
    // and the count number
    // then put back
    // 
    public static class Combiner extends Reducer<Text, IntPair, Text, IntPair>
    {
        public void combiner(Text key, Iterable<IntPair> values, Context context)
            throws IOException, InterruptedException
        {
            int sum_length = 0;
            int count = 0;
            IntPair pair = new IntPair();
            for (IntPair value : values)
            {
                sum_length += value.getSecond();
                count += value.getFirst();
            }
            pair.set(count, sum_length);
            context.write(key, pair);
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
        Job job = Job.getInstance(conf, "WordAvgLen1");
        
        job.setJarByClass(WordAvgLen1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(DoubleSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPair.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
