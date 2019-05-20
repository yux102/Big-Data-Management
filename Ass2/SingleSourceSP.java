package comp9313.ass2;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleSourceSP
{
    // use enum as counter
    static enum counter
    {
        i;
    }

    // 1. format from the input file need to convert
    // EdgeId | FromNodeId | ToNodeId | DIstance
    // NodeId | Distance(from Souce to Node) | AdjacentNodeId:Distance(from Node to Adj)
    //
    public static class ConvertMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text node_id = new Text();
        private Text adj_dist = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens())
            {
                String line = itr.nextToken();

                String[] token = line.split(" ");

                String nid = token[1];
                String adj_id = token[2];
                String dist = token[3];

                node_id.set(nid);
                adj_dist.set(adj_id + ":" + dist);
                context.write(node_id, adj_dist);
            }
        }
    }


    // for iter 0, distance is max if it is not the source node;
    // add distance to the list, split by '\t'
    // NodeId | Distance(from Souce to Node) | AdjacentNodeId:Distance(from Node to Adj)
    // 0 \t 0.0 \t 1:10,2:5 \t 0
    // 1 \t MAX \t 3:1,2:2 \t 0
    // ...
    public static class ConvertReducer extends Reducer<Text, Text, Text, Text>
    {
        private Text list = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            String source_node = conf.get("source_node");

            String temp_list = "";

            String distance = Double.toString(Double.MAX_VALUE);
            if (Integer.parseInt(key.toString()) == Integer.parseInt(source_node))
            {
                distance = Double.toString(0);
            }

            temp_list += distance;
            temp_list += "\t";

            for (Text value : values)
            {
                temp_list += value.toString();
                temp_list += "|";
            }

            // add path
            // for iter 0, path is source node
            //
            temp_list += "\t";
            temp_list += source_node;

            list.set(temp_list);
            context.write(key, list);
        }
    }

    // 2. Shortest Path

    public static String OUT = "output";
    public static String IN = "input";


    // split the line by \t
    // sp[0] is the node id
    // sp[1] is the distance from source to node
    // sp[2] is the distance list from node to adjust node
    // sp[3] is the previous path
    // transform to     VALUE   added_distance      path
    // transform to     NODE    current_distance    adj-list
    // which would be   s[0]    sp[1]               sp[2]
    // copy from PPT
    //
    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text>
    {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            // YOUR JOB: map function
            // ... ...
            Text word = new Text();
            String line = value.toString();

            String[] sp = line.split("\t");
            int nid = Integer.parseInt(sp[0]);

            double distance = Double.parseDouble(sp[1]);

            String list = sp[2].toString();

            String[] pairs = list.split("\\|");
            for (String pair : pairs)
            {
                if (pair.equalsIgnoreCase("UNMODED"))
                {
                    // if node is unmoded, pass.
                    continue;
                }
                String[] adj_dist = pair.split(":");
                int adj = Integer.parseInt(adj_dist[0]);
                double dist = Double.parseDouble(adj_dist[1]);

                double dist_add;
                if (distance != Double.MAX_VALUE)
                {
                    // althought lecture said, we could ignore this double addtion percious error
                    // i feel it is a good thing to add them correct
                    BigDecimal big_distance = new BigDecimal(Double.toString(distance));
                    BigDecimal big_dist = new BigDecimal(Double.toString(dist));
                    dist_add = big_distance.add(big_dist).doubleValue();

                    // VALUE add_adj_distance path
                    word.set("VALUE" + " " + dist_add + " " + sp[3] + "->" + adj);

                    context.write(new LongWritable(adj), word);
//                    System.out.println("map: " + adj + " " + word.toString());

                    word.clear();
                }
            }

            // VALUE current_distance path
            word.set("VALUE" + " " + sp[1] + " " + sp[3]);
            context.write(new LongWritable(nid), word);
//            System.out.println("map: " + nid + " " + word.toString());
            word.clear();

            // NODES current_distance adj_list
            word.set("NODES" + " " + sp[1] + " " + sp[2]);
            context.write(new LongWritable(nid), word);
//            System.out.println("map: " + nid + " " + word.toString());
            word.clear();

        }

    }

    // pass     VALUE   added_distance      path
    // pass     NODE    current_distance    adj_list
    // pass     NODE    UNMODED             adj_list
    // which    s[0]    sp[1]               sp[2]
    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Text result = new Text();
            String adj_list = "UNMODED";
            String path = "";

            double last_distance = Double.MAX_VALUE;
            double shortest_distance = Double.MAX_VALUE;

            for (Text value : values)
            {
                String[] sp = value.toString().split(" ");

                if (sp[0].equalsIgnoreCase("NODES"))
                {
                    // remember this last distance to see if it is done using counter
                    last_distance = Double.parseDouble(sp[1]);
                    adj_list = sp[2];
                }
                else if (sp[0].equalsIgnoreCase("VALUE"))
                {
                    double dist = Double.parseDouble(sp[1]);
                    if (dist <= shortest_distance)
                    {
                        shortest_distance = dist;
                        path = sp[2];
                    }
                }
            }

            // looks like: key  shortest_distance   adj_list    path
            // looks like: key  shortest_distance   UNMODED     path
            result.set(shortest_distance + "\t" + adj_list + "\t" + path);
            context.write(key, result);
//            System.out.println("reduce: " + key + " " + result.toString());
            result.clear();

            if (shortest_distance < last_distance)
            {
                context.getCounter(counter.i).setValue(1);
            }
        }
    }

    // 3. finally convert it to the final format

    public static class FinalMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        private IntWritable node = new IntWritable();
        private Text dist_path = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreElements())
            {
                String line = itr.nextToken();
                String[] token = line.split("\t");
                int nid = Integer.parseInt(token[0]);
                String dist = token[1];

                String path = token[3];

                node.set(nid);
                dist_path.set(dist + "\t" + path);

                if (Double.parseDouble(dist) != Double.MAX_VALUE)
                {
                    context.write(node, dist_path);
                }
            }
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, Text, Text, Text>
    {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text value : values)
            {
                result.set(key.toString() + "\t" + value);
                context.write(null, result);
            }
        }
    }


    public static void main(String[] args) throws Exception
    {

        IN = args[0];

        OUT = args[1];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration;

        // and the query source node ID.
        String source_node = args[2];

        // YOUR JOB: Convert the input file to the desired format for iteration,
        // i.e.,
        // create the adjacency list and initialize the distances
        // ... ...
        Configuration convert_conf = new Configuration();
        convert_conf.set("source_node", source_node);

        Job job = Job.getInstance(convert_conf, "convert");
        job.setJarByClass(SingleSourceSP.class);
        job.setMapperClass(ConvertMapper.class);
        job.setReducerClass(ConvertReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);

        // update the input and output folder
        input = output;
        iteration += 1;
        output = OUT + iteration;

        long last_counter = -1;
        boolean isdone = false;

        while (isdone == false)
        {
            // YOUR JOB: Configure and run the MapReduce job
            // ... ...
            Configuration shortest_conf = new Configuration();

            Job job2 = Job.getInstance(shortest_conf, "shortest path");
            job2.setJarByClass(SingleSourceSP.class);
            job2.setMapperClass(SSSPMapper.class);
            job2.setReducerClass(SSSPReducer.class);
            job2.setNumReduceTasks(1);

            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(input));
            FileOutputFormat.setOutputPath(job2, new Path(output));
            job2.waitForCompletion(true);

            // You can consider to delete the output folder in the previous
            // iteration to save disk space.
            FileSystem hdfs = FileSystem.get(new URI(OUT), shortest_conf);
            Path tmp_path = new Path(input);

            if (hdfs.exists(tmp_path))
            {
                hdfs.delete(tmp_path, true);
            }

            // YOUR JOB: Check the termination criterion by utilizing the
            // counter
            // ... ...
            long current_counter = job2.getCounters().findCounter(counter.i).getValue();

            if (0 == current_counter)
            {
                isdone = true;
            }
            last_counter = current_counter;
            job2.getCounters().findCounter(counter.i).setValue(0);

            input = output;
            iteration += 1;
            output = OUT + iteration;
        }

        // YOUR JOB: Extract the final result using another MapReduce job with
        // only 1 reducer, and store the results in HDFS
        // ... ...
        Configuration result_conf = new Configuration();

        Job job3 = Job.getInstance(result_conf, "result");
        job3.setMapperClass(FinalMapper.class);
        job3.setReducerClass(FinalReducer.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.setJarByClass(SingleSourceSP.class);
        FileInputFormat.addInputPath(job3, new Path(input));
        FileOutputFormat.setOutputPath(job3, new Path(OUT));
        job3.waitForCompletion(true);

        // delete the last temporary folder
        FileSystem hdfs = FileSystem.get(new URI(OUT), result_conf);
        Path tmp_path = new Path(input);

        if (hdfs.exists(tmp_path))
        {
            hdfs.delete(tmp_path, true);
        }
    }

}
