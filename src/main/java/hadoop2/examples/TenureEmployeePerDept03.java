package hadoop2.examples;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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



public class TenureEmployeePerDept03 extends Configured implements Tool {
        
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
    public static class Map extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
        {
            String line = value.toString();             // read source data
            
            try
            {
                // data processing
                String [] lineSplit = line.split(",");
                String dept = lineSplit[7].trim();
                String name = lineSplit[1].trim();
                String hiredate = lineSplit[4].trim();
                
                if (dept.isEmpty() || name.isEmpty() || hiredate.isEmpty())
                {
                    System.out.println("Get an error");
                    context.getCounter(Counter.LINESKIP).increment(1);  // exception, error counter + 1
                    return;
                }
                else
                {
                    String        outputvalue = hiredate + "," + name;
                    context.write( new Text(dept), new Text(outputvalue) );    // output
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
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        int year = Integer.MAX_VALUE;
        int month = Integer.MAX_VALUE;
        int day = Integer.MAX_VALUE;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException
        {
            year  = Integer.MAX_VALUE;
            month = Integer.MAX_VALUE;
            day   = Integer.MAX_VALUE;

            String name_result = null;
            for (Text value : values) {
                String [] lineSplit = value.toString().split(",");
                String hiredate = lineSplit[0].trim();
                String name = lineSplit[1].trim();

                if (IsEarlier(hiredate))
                {
                    System.out.println("Name = " + name);
                    name_result = name;
                }
                else
                {
                    System.out.println("Ignore this name");
                }
            }

            System.out.println("Key = " + key.toString());
            System.out.println("name_result = " + name_result);

            context.write(key, new Text(name_result));
        }


        private boolean IsEarlier(String hiredate) {
            boolean        result = false;

            String[]    lineSplit = hiredate.trim().split("-");
            int         cur_year = Integer.parseInt(lineSplit[0]);
            int         cur_month = Integer.parseInt(lineSplit[1]);
            int         cur_day = Integer.parseInt(lineSplit[2]);
            System.out.println(cur_year + "-" + cur_month + "-" + cur_day);

            if (cur_year < year)
            {
                result = true;
            }
            else if (cur_year == year)
            {
                if (cur_month < month)
                {
                    result = true;
                }
                else if (cur_month == month)
                {
                    if (cur_day < day)
                    {
                        result = true;
                    }
                }
            }


            if (true == result)
            {
                year = cur_year;
                month = cur_month;
                day = cur_day;
            }

            return result;
        }

    }

    
    public static void main(String[] args) throws Exception {
        // Run job
        int res = ToolRunner.run(new Configuration(), new TenureEmployeePerDept03(), args);
        System.exit(res);

    }
        
    @Override
    public int run(String[] args) throws Exception 
    {
        Configuration conf = getConf();
        

        Job job = new Job(conf, "TenureEmployeePerDept03");     // Job name
        job.setJarByClass(TenureEmployeePerDept03.class);       // Job Class

        // args[0] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
        // args[1] = hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o3

        FileInputFormat.addInputPath( job, new Path(args[0]) );     // Input Path
        FileOutputFormat.setOutputPath( job, new Path(args[1]) );   // Output Path
        
        job.setMapperClass(Map.class);                              // Set Mapper class
        job.setReducerClass(Reduce.class);
        job.setOutputFormatClass( TextOutputFormat.class );
        job.setOutputKeyClass( Text.class );                    // set Output Key type
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

/*

yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar
    hadoop2.examples.TenureEmployeePerDept03
    hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
    hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o3

*/
