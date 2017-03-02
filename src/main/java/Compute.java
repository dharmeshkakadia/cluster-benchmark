import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.BenchmarkConfig;
import util.Util;

import java.io.IOException;
import java.util.Random;

public class Compute extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Compute(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Util.checkArgs(args, 2, "Compute.jar numberOfMappers busyTime(s) [optional arguments]");
        Configuration conf = new Configuration();
        Util.setConfParameters(conf, args, 2);
        String tmp = conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT) + "compute";
        int numMappers = Integer.parseInt(args[0]);
        long busyTime = Long.parseLong(args[1]);
        Path inPath = new Path(tmp, numMappers + "_compute_"+busyTime);
        Path outPath = new Path(tmp, numMappers + "_compute_out_"+busyTime);
        Util.writeNLineHDFSFile(conf, inPath, numMappers);
        conf.set("mapred.task.timeout", "0");
        conf.setLong(BenchmarkConfig.BUSY_TIME_CONF, busyTime*1000);
        Job job = Job.getInstance(conf,"Compute " + numMappers + " : " + busyTime);
        job.setJarByClass(Compute.class);
        job.setMapperClass(BusyMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(NLineInputFormat.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.submit();
        boolean isSuccessful = job.waitForCompletion(true);
        Util.cleanup(conf,inPath,outPath);
        return isSuccessful?0:1;
    }

    private static class BusyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + context.getConfiguration().getLong(
                BenchmarkConfig.BUSY_TIME_CONF, BenchmarkConfig.DEFAULT_SLEEP_VALUE);
            int count = 0;
            while (true) {
                count += new Random().nextInt();
                if (System.currentTimeMillis() >= endTime) {
                    break;
                }
            }
        }
    }
}
