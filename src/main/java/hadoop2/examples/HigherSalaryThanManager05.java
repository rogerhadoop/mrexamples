package hadoop2.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class HigherSalaryThanManager05 extends Configured implements Tool {
        
    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
          return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
        
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
    public static class Map extends Mapper<LongWritable, Text, TextPair, Text> 
    {
        String        managerTag="MANAGER~";
        String        employeeTag="EMPLOYEE~";
            
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
        {
            String line = value.toString();             // read source data
            
            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String employeeNo   = lineSplit[0].trim();
                String mgrId        = lineSplit[3].trim();
                String salary       = lineSplit[5].trim();
                
                if (employeeNo.isEmpty() || salary.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    // (1)
                    String        outputValueAsManager = managerTag + salary;

                    System.out.println("ManagerSalaryMapper: [" + employeeNo + "], [" + outputValueAsManager + "]");
                    context.write( new TextPair(employeeNo, "0"), new Text(outputValueAsManager) );    // output


                    // (2)
                    String        outputValueAsEmployee = employeeTag + employeeNo + "~" + salary;
                    System.out.println("EmployeeSalaryMapper: [Manager is " + mgrId + "], [" + outputValueAsEmployee + "]");
                    if (mgrId.isEmpty())
                    {
                        System.out.println("Employee " + employeeNo + " does not have manager, ignore.");
                    }
                    else
                    {
                        context.write( new TextPair(mgrId, "1"), new Text(outputValueAsEmployee));    // output
                    }
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
    public static class Reduce extends Reducer<TextPair, Text, NullWritable, Text> {

        int                count = 0;
        @Override
        public void reduce(TextPair key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {

            int         managerSalaryValue = 0;
            String      header = null;
            boolean     headerPrinted = false;

            Iterator<Text> iter = values.iterator();
            Text        managerSalary = new Text(iter.next());
            System.out.println("Reducer: count = " + count + ", Manager ID = " + key.getFirst().toString() + ", Manager Salary = " + managerSalary.toString());
            String [] lineSplit = managerSalary.toString().split("~");
            String filter = lineSplit[0].trim();
            if (filter.equals("MANAGER"))
            {
                managerSalaryValue = Integer.parseInt(lineSplit[1].trim());
                header = "Manager[ManagerId = " + key.getFirst().toString() + "][Salary = " + managerSalaryValue + "] have following employee with higher salary:";
            }
            else
            {
                System.err.println("Partitioning and grouping error.");
            }

            count++;

            while (iter.hasNext()) {
                Text record = iter.next();
                String [] lineSplit2 = record.toString().split("~");
                String filter2 = lineSplit2[0].trim();
                if (filter2.equals("EMPLOYEE"))
                {
                    String employeeId = lineSplit2[1].trim();
                    int employeeSalaryValue = Integer.parseInt(lineSplit2[2].trim());

                    if (employeeSalaryValue > managerSalaryValue)
                    {
                        if (false == headerPrinted)
                        {
                            context.write(NullWritable.get(), new Text(header));
                            headerPrinted = true;
                        }
                        String        outputValue = "\tEmployee Id: " + employeeId + "; Salary: " + employeeSalaryValue;
                        context.write(NullWritable.get(), new Text(outputValue));
                    }
                }
                else
                {
                    System.err.println("Partitioning and grouping error.");
                }
            }
        }
    }

    
    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new HigherSalaryThanManager05(), args);
        System.exit(res);

    }
        
    @Override
    public int run(String[] args) throws Exception 
    {
        Configuration conf = getConf();
        

        Job job = new Job(conf, "HigherSalaryThanManager05");       // Job name
        job.setJarByClass(HigherSalaryThanManager05.class);         // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o5

        FileInputFormat.addInputPath( job, new Path(args[0]) );     // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );   // Output Path
        
        job.setMapperClass(Map.class);                              // Set Mapper class

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);
        
        job.setReducerClass(Reduce.class);

        job.setOutputFormatClass( TextOutputFormat.class );
        job.setMapOutputKeyClass( TextPair.class );                // set Output Key type
        job.setMapOutputValueClass( Text.class );                  // set Output value type
        
        job.setOutputKeyClass( NullWritable.class );               // set Reducer Output Key type

        
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

