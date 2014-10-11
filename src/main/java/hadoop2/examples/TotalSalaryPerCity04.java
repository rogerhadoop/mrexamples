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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class TotalSalaryPerCity04 extends Configured implements Tool {
        
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
    public static class EmployeeFileMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        String        fileTag="EMPLOYEE~";
            
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
                }
                else
                {
                    context.write( new Text(dept), new Text(fileTag + salary) );    // output
                }
            }
            catch ( Exception e )
            {
                System.out.println("Get an Exception: " + e);
                context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
            }
        }
    }

    
    /**
     * MAP
     */  
    public static class DeptFileMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        String        fileTag="DEPT~";
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
        {
            String line = value.toString();             // read source data
            
            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String dept = lineSplit[0].trim();
                String city = lineSplit[2].trim();
                
                if (dept.isEmpty() || city.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                }
                else
                {
                    System.out.printf("Write Dept:City =  [%s : %s]\n", dept, city);
                    context.write( new Text(dept), new Text(fileTag + city));    // output
                }
            }
            catch ( Exception e )
            {
                System.out.println("Get an Exception: " + e);
                context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
            }
        }
    }
    
    /**
     * REDUCE
     */  
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException
        {
            /*
            for example, key="10", values=["EMPLOYEE~2450", "EMPLOYEE~5000", "EMPLOYEE~1300", "DEPT~NEW YORK"]
             */
            int     totalSalary = 0;
            String  city = null;

            for (Text value : values) {
                String [] lineSplit = value.toString().split("~");
                String fileTag = lineSplit[0].trim();
                String fileValue = lineSplit[1].trim();

                if (fileTag.equals("EMPLOYEE"))
                {
                    totalSalary += Integer.parseInt(fileValue);
                }
                else if (fileTag.equals("DEPT"))
                {
                    city = fileValue;
                }
            }
            context.write(new Text(city), new IntWritable(totalSalary));
        }
    }

    
    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new TotalSalaryPerCity04(), args);
        System.exit(res);

    }
        
    @Override
    public int run(String[] args) throws Exception 
    {
        Configuration conf = getConf();
        

        Job job = new Job(conf, "TotalSalaryPerCity04");    // Job name
        job.setJarByClass(TotalSalaryPerCity04.class);      // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt
        // args[2] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o4

        MultipleInputs.addInputPath( job, new Path(args[0]), TextInputFormat.class,  EmployeeFileMapper.class);     // Input Path
        MultipleInputs.addInputPath( job, new Path(args[1]), TextInputFormat.class,  DeptFileMapper.class);         // Input Path

        FileOutputFormat.setOutputPath( job, new Path(args[2]) );       // Output Path
        
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass( Text.class );     // set Map Output Key type, if it is different from Map Input Key class
        job.setMapOutputValueClass( Text.class );   // set Map Output value type, not necessary, since it is same as input

        job.setOutputFormatClass( TextOutputFormat.class );
        job.setOutputKeyClass( IntWritable.class ); // set Output Key type, if it is different from Map Input Key class

        job.waitForCompletion(true);
        
        // Print out Job finishing status
        System.out.println( "Job Name: " + job.getJobName() );
        System.out.println( "Job Successful: " + ( job.isSuccessful() ? "Yes" : "No" ) );
        System.out.println( "Lines of Mapper Input: "   + job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue() );
        System.out.println( "Lines of Reducer Output: " + job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue() );
        System.out.println( "Lines skipped: " + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

        return job.isSuccessful() ? 0 : 1;
    }
}

