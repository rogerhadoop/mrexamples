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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class JEmployeeAndDept07 extends Configured implements Tool {
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
    public static class EmployeeFileMapper extends Mapper<LongWritable, Text, TextPair, Text>
    {
        String  fileTag="EMPLOYEENAME~";

        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();             // read source data

            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String dept = lineSplit[7].trim();
                String name = lineSplit[1].trim();

                if (dept.isEmpty() || name.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    if (name.startsWith("J"))
                    {
                        context.write( new TextPair(dept, "1"), new Text(fileTag + name) );    // output
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
     * MAP
     */
    public static class DeptFileMapper extends Mapper<LongWritable, Text, TextPair, Text>
    {
        String  fileTag="DEPT~";
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();             // read source data

            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String dept = lineSplit[0].trim();
                String deptName = lineSplit[1].trim();

                if (dept.isEmpty() || deptName.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    context.write( new TextPair(dept, "0"), new Text(deptName));    // output
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

        @Override
        public void reduce(TextPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            Iterator<Text> iter = values.iterator();
            Text    deptName = new Text(iter.next());


            while (iter.hasNext()) {
                Text record = iter.next();
                String [] lineSplit = record.toString().split("~");
                String filter = lineSplit[0].trim();
                if (filter.equals("EMPLOYEENAME"))
                {
                    String employeeName = lineSplit[1].trim();

                    String  outputValue = employeeName + "\t" + deptName.toString();
                    context.write(NullWritable.get(), new Text(outputValue));
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
        int res = ToolRunner.run(new Configuration(), new JEmployeeAndDept07(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = new Job(conf, "JEmployeeAndDept07");                  // Job name
        job.setJarByClass(JEmployeeAndDept07.class);                      // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt
        // args[2] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o7

        MultipleInputs.addInputPath( job, new Path(args[0]), TextInputFormat.class,  EmployeeFileMapper.class);     // Input Path
        MultipleInputs.addInputPath( job, new Path(args[1]), TextInputFormat.class,  DeptFileMapper.class);         // Input Path

        FileOutputFormat.setOutputPath( job, new Path(args[2]) );       // Output Path

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);


        job.setReducerClass(Reduce.class);
        job.setOutputFormatClass( TextOutputFormat.class );
        job.setMapOutputKeyClass( TextPair.class );                 // set Map Output Key type
        job.setMapOutputValueClass( Text.class );               // set Map Output value type

        job.setOutputKeyClass( NullWritable.class );            // set Output Key type
        job.setOutputValueClass( Text.class );                  // set Output value type

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



