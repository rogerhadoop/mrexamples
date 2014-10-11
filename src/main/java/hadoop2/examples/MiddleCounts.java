package hadoop2.examples;


import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MiddleCounts extends Configured implements Tool {
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
    public static class Map10 extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();             // read source data

            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String employeeId = lineSplit[0].trim();
                String managerId = lineSplit[3].trim();

                if (employeeId.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    if (managerId.isEmpty())
                    {
                        managerId = " ";
                    }
                    String out =  employeeId + "~" + managerId + "~";
                    context.write( new IntWritable(0), new Text(out) );    // output
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
        List<String>        employeeList = new ArrayList<String>();
        Map<String, String> employeeToManagerMap = new HashMap<String, String>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                System.out.println("reduce(): " + value);

                String[] lineSplit = value.toString().split("~");

                String employeeId = lineSplit[0].trim();
                String managerId = lineSplit[1].trim();

                employeeList.add(employeeId);
                employeeToManagerMap.put(employeeId, managerId);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            int     totalEmployee = employeeList.size();
            int     i, j;
            int     distance;
            System.out.println(employeeList);
            System.out.println(employeeToManagerMap);

            for (i = 0; i < (totalEmployee - 1); i++)
            {
                for (j = (i + 1); j < totalEmployee; j++)
                {
                    distance = calculateDistance(i, j);
                    String  value = employeeList.get(i) + " and " + employeeList.get(j) + " = " + distance;
                    context.write( NullWritable.get(), new Text(value) );    // output
                }
            }
        }


        private int calculateDistance(int i, int j) {
            String  employeeA = employeeList.get(i);
            String  employeeB = employeeList.get(j);
            int     distance = 0;


            // if A is B's manager or vice verse
            if ( employeeToManagerMap.get(employeeA).equals(employeeB)  ||  employeeToManagerMap.get(employeeB).equals(employeeA) )
            {
                distance = 0;
            }
            // else if A and B under same manager
            else if ( employeeToManagerMap.get(employeeA).equals(employeeToManagerMap.get(employeeB)) )
            {
                distance = 0;
            }
            // else calculate Least common Ancestor
            else
            {
                // build A, B's manager list, start from self
                List<String>        employeeA_ManagerList = new ArrayList<String>();
                List<String>        employeeB_ManagerList = new ArrayList<String>();


                employeeA_ManagerList.add(employeeA);
                String  current = employeeA;
                while (false == employeeToManagerMap.get(current).isEmpty())
                {
                    current = employeeToManagerMap.get(current);
                    employeeA_ManagerList.add(current);
                }
                //employeeA_ManagerList.add(employeeA);

                employeeB_ManagerList.add(employeeB);
                current = employeeB;
                while (false == employeeToManagerMap.get(current).isEmpty())
                {
                    current = employeeToManagerMap.get(current);
                    employeeB_ManagerList.add(current);
                }

                // find the common manager of A and B
                int ii = 0, jj = 0;
                String  currentA_manager, currentB_manager;
                boolean found = false;

                for (ii = 0; ii < employeeA_ManagerList.size(); ii++ )
                {
                    currentA_manager = employeeA_ManagerList.get(ii);
                    for (jj = 0; jj < employeeB_ManagerList.size(); jj++ )
                    {
                        currentB_manager = employeeB_ManagerList.get(jj);
                        if (currentA_manager.equals(currentB_manager))
                        {
                            found = true;
                            break;
                        }
                    }

                    if (found)
                    {
                        break;
                    }
                }

                // now get the answer
                distance = ii + jj - 1;
            }

            return distance;
        }
    }


    public static void main(String[] args) throws Exception {

        // record starting time
        DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        Date start = new Date();

        // Run job
        int res = ToolRunner.run(new Configuration(), new MiddleCounts(), args);

        // Print out job running time
        Date end = new Date();
        float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
        System.out.println( "Job start: " + formatter.format(start) );
        System.out.println( "Job end: " + formatter.format(end) );
        System.out.println( "Job interval: " + String.valueOf( time ) + " minutes." );

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = new Job(conf, "MiddleCounts");    // Job name
        job.setJarByClass(MiddleCounts.class);      // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/10

        FileInputFormat.addInputPath( job, new Path(args[0]) );     // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );   // Output Path

        job.setMapperClass(Map10.class);                    // Set Mapper class
        job.setReducerClass(Reduce.class);                  // Set Reducer class

        job.setOutputFormatClass( TextOutputFormat.class );
        job.setMapOutputKeyClass( IntWritable.class );      // set map Output Key type
        job.setMapOutputValueClass( Text.class );           // set map Output value type

        job.setOutputKeyClass( NullWritable.class );        // set Output Key type
        job.setOutputValueClass( Text.class );              // set Output value type

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
