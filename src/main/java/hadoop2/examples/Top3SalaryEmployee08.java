package hadoop2.examples;


import java.io.IOException;
import java.util.LinkedList;

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



public class Top3SalaryEmployee08 extends Configured implements Tool {

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
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();             // read source data

            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String name = lineSplit[1].trim();
                String salary = lineSplit[5].trim();

                if (name.isEmpty() || salary.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    String outputValue = name + "~" + salary;
                    context.write( new IntWritable(0), new Text(outputValue) );    // output
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
        int numOfOutput = 3;

        int[] salaryArray  = new int[numOfOutput];
        LinkedList<String>  nameList = new LinkedList<String>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            PrintForDebug();
            for (Text value : values) {
                String[] lineSplit = value.toString().split("~");

                String employeeName = lineSplit[0].trim();
                int salary = Integer.parseInt(lineSplit[1].trim());

                System.out.println("name = " + employeeName + ", salary = " + salary);
                updateArray(employeeName, salary);
            }

            for (int i = 0; i < numOfOutput; i++)
            {
                context.write(NullWritable.get(), new Text(nameList.get(i) + "\t" + salaryArray[i]));
            }
        }

        // salaryArray[0] >= salaryArray[1] >= salaryArray[2]
        private void updateArray(String name, int salary)
        {
            for (int i = 0; i < numOfOutput; i++)
            {
                if (salary > salaryArray[i])
                {
                    for (int j = numOfOutput - 1; j > i; j--)
                    {
                        salaryArray[j] = salaryArray[j - 1];
                    }
                    salaryArray[i] = salary;

                    // update nameList
                    if (nameList.size() >= numOfOutput)
                    {
                        nameList.removeLast();
                    }
                    nameList.add(i, name);

                    PrintForDebug();

                    break;
                }
            }
        }

         private void PrintForDebug()
         {
             System.out.println("nameList.size() = " + nameList.size());

             System.out.println("salaryArray = ");
             for (int i = 0; i < numOfOutput; i++)
             {
                 System.out.println("\t" + salaryArray[i]);
             }
         }
    }


    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new Top3SalaryEmployee08(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = new Job(conf, "Top3SalaryEmployee08");            // Job name
        job.setJarByClass(Top3SalaryEmployee08.class);              // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o8
        
        FileInputFormat.addInputPath( job, new Path(args[0]) );     // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );   // Output Path

        job.setMapperClass(Map.class);                              // Set Mapper class
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass( IntWritable.class );              // set map Output Key type
        job.setMapOutputValueClass( Text.class );                   // set map Output value type

        job.setOutputFormatClass( TextOutputFormat.class );
        job.setOutputKeyClass( NullWritable.class );                // set Output Key type
        job.setOutputValueClass( Text.class );                      // set Output value type

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



