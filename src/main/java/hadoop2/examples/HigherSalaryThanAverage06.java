package hadoop2.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class HigherSalaryThanAverage06 extends Configured implements Tool {


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
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        String  averageTag="AVERAGE~";
        String  employeeTag="EMPLOYEE~";

        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();             // read source data

            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String employeeName = lineSplit[1].trim();
                String salary       = lineSplit[5].trim();

                if (employeeName.isEmpty() || salary.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    // (1) output for calculating average
                    context.write( new IntWritable(0), new Text(salary) );    //


                    // (2) output for compare individual employee's salary to average
                    String  outputValueForEmployee = employeeTag + employeeName + "~" + salary;
                    System.out.println("Map: [employeeName is " + employeeName + "], [salary is " + salary + "]");
                    context.write( new IntWritable(1), new Text(outputValueForEmployee));    // output
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
    public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

        int     count = 0;
        float   avgSalary = 0;
        int     totalSalary = 0;
        int     callIndex = 0;



        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                if (key.get() == 0)
                {
                    System.out.println("CallIndex = " + callIndex);
                    totalSalary += Integer.parseInt(value.toString());
                    count++;
                    callIndex++;
                }
                else if (key.get() == 1)
                {
                    if (avgSalary == 0)
                    {
                        avgSalary = (float)1.0 * totalSalary / count;
                        context.write(NullWritable.get(), new Text("Average Salary = " + avgSalary));
                        context.write(NullWritable.get(), new Text("Following employees have salarys higher than Average:"));
                    }

                    System.out.println("Employee salary = " + value.toString());
                    String [] lineSplit = value.toString().split("~");
                    String name = lineSplit[1].trim();
                    int    salary = Integer.parseInt(lineSplit[2].trim());

                    if (salary > avgSalary)
                    {
                        context.write(NullWritable.get(), new Text("\t" + name + "\t" + salary));

                    }

                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new HigherSalaryThanAverage06(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = new Job(conf, "HigherSalaryThanAverage06");             // Job name
        job.setJarByClass(HigherSalaryThanAverage06.class);               // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o6

        FileInputFormat.addInputPath( job, new Path(args[0]) );         // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );       // Output Path

        job.setMapperClass(Map.class);                                  // Set Mapper class
        job.setReducerClass(Reduce.class);

        // Must set number of reducer to 1 # -D mapred.reduce.tasks = 1
        job.setNumReduceTasks(1);

        job.setOutputFormatClass( TextOutputFormat.class );
        job.setMapOutputKeyClass( IntWritable.class );             // set Output Key type
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


