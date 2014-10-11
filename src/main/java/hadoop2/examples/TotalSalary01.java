package hadoop2.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TotalSalary01 extends Configured implements Tool {
        
    /**  
     * counter
     * Used for counting any abnormal data
     */  
    enum Counter 
    {
        LINESKIP,   // line with error
    }

    /**
     * MAP
     */  
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
        {
            String line = value.toString();             // read source data
            
            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String dept = lineSplit[7].trim();
                String salary = lineSplit[5].trim();
                
                if (dept.isEmpty() || salary.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    context.write( new Text(dept), new IntWritable(Integer.parseInt(salary)) );    // output
                }
            }
            catch ( Exception e )
            {
                System.out.println("Get an Exception: " + e);
                context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                return;
            }
        }
    }

    
    /**
     * REDUCE
     */  
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                        throws IOException, InterruptedException {

            int totalSalary = 0;
            for (IntWritable value : values) {
                totalSalary += value.get();
            }
            context.write(key, new IntWritable(totalSalary));
        }
    }

    
    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new TotalSalary01(), args);
        System.exit(res);

    }
        
    @Override
    public int run(String[] args) throws Exception 
    {
        Configuration conf = getConf();
        

        Job job = new Job(conf, "TotalSalary01");          // Job name
        job.setJarByClass(TotalSalary01.class);            // Job Class
        
        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o1
        FileInputFormat.addInputPath( job, new Path(args[0]) );     // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );   // Output Path
        
        job.setMapperClass(Map.class);                              // Set Mapper class
        job.setReducerClass(Reduce.class);
        job.setOutputFormatClass( TextOutputFormat.class );
        job.setOutputKeyClass( Text.class );                        // set Output Key type
        job.setOutputValueClass( IntWritable.class );               // set Output value type
        
        job.waitForCompletion(true);
        
        // Print out Job finishing status
        System.out.println( "Job Name: " + job.getJobName() );
        System.out.println( "Job Successful: " + ( job.isSuccessful() ? "Yes" : "No" ) );

        // org.apache.hadoop.mapred.Task$Counter is deprecated
        System.out.println( "Lines of Mapper Input: " + job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue() );
        System.out.println( "Lines of Reducer Output: " + job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue() );
        System.out.println( "Lines skipped: " + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

        return job.isSuccessful() ? 0 : 1;
    }

}


/*
yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar
         hadoop2.examples.TotalSalary01
         hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
         hdfs://quickstart.cloudera:8020/user/cloudera/data/yarn-result/o1

hdfs dfs -rm -r -f /user/cloudera/data/emp/yarn-result
hdfs dfs -rm -r -f /user/cloudera/data/emp/spark-result

yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.TotalSalary01 hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/01


*/