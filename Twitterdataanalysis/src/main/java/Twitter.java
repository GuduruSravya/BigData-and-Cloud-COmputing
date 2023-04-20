import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




public class Twitter extends Configured implements Tool {

public static class FollowerCountMapper extends Mapper<Object,Text,IntWritable,IntWritable>
{
   @Override
   public void map(Object key, Text value, Context context) throws IOException,InterruptedException
   {
        String str = value.toString();
        Scanner scanner = new Scanner(str).useDelimiter(",");
        int id = scanner.nextInt();
        int followerId = scanner.nextInt();
        context.write(new IntWritable(followerId),new IntWritable(id));
   }

}

public static class FollowerCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
{
    @Override
    public void reduce(IntWritable followerId, Iterable<IntWritable> ids, Context context) throws IOException,InterruptedException
    {
        int count = 0;
        for(IntWritable id:ids){
            count+=1;
        }
        context.write(followerId,new IntWritable(count));
    }
}

public static class EqualCountMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
    @Override
    public void map(Object key,Text value, Context context) throws IOException,InterruptedException
    {
        Scanner scanner = new Scanner(value.toString()).useDelimiter("\t");
        int followerid = scanner.nextInt();
        int count = scanner.nextInt();
        context.write(new IntWritable(count), new IntWritable(1));
    }
}

public static class EqualCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
{
    @Override
    public void reduce(IntWritable count, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
    {
        int sum = 0;
        for(IntWritable val:values){
            sum+=1;
        }
        context.write(count,new IntWritable(sum));
    }
}

    @Override
    public int run( String args[]) throws Exception{
        Configuration conf = getConf();
        Job job1 = Job.getInstance(conf,"job1");
        job1.setJarByClass(Twitter.class);
        job1.setMapperClass(FollowerCountMapper.class);
        job1.setReducerClass(FollowerCountReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf,"job2");
        job2.setJarByClass(Twitter.class);
        job2.setMapperClass(EqualCountMapper.class);
        job2.setReducerClass(EqualCountReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Twitter(),args);
    }
}
