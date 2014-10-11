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



public class EmployeeListByIncome09 extends Configured implements Tool {

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
                String comm = lineSplit[6].trim();
                int totalIncome = 0;

                if (!salary.isEmpty())
                {
                    totalIncome += Integer.parseInt(salary);
                }
                if (!comm.isEmpty())
                {
                    totalIncome += Integer.parseInt(comm);
                }

                if (name.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    String outputValue = name + "~" + totalIncome;
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
        LinkedList<Integer> incomeList = new LinkedList<Integer>();
        LinkedList<String>  nameList = new LinkedList<String>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                String[] lineSplit = value.toString().split("~");

                String employeeName = lineSplit[0].trim();
                int income = Integer.parseInt(lineSplit[1].trim());

                System.out.println("name = " + employeeName + ", income = " + income);
                updateList(employeeName, income);
            }

            for (int i = 0; i < incomeList.size(); i++)
            {
                context.write(NullWritable.get(), new Text(nameList.get(i) + "\t" + incomeList.get(i)));
            }
        }

        private void updateList(String name, int income)
        {
            boolean done = false;

            for (int i = 0; i < incomeList.size(); i++)
            {
                if (income > incomeList.get(i))
                {
                    incomeList.add(i, income);
                    nameList.add(i, name);

                    done = true;
                    break;
                }
            }

            if (false == done)
            {
                incomeList.add(income);
                nameList.add(name);
            }
        }

    }


    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new EmployeeListByIncome09(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = new Job(conf, "EmployeeListByIncome09");          // Job name
        job.setJarByClass(EmployeeListByIncome09.class);            // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o9
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


