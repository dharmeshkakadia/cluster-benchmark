import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Sleep extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Sleep(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Util.checkArgs(args, 2, "Sleep.jar numberOfMappers sleepTime(s) [optional arguments]");
        Configuration conf = new Configuration();
        int numMappers = Integer.parseInt(args[0]);
        long sleepTime = Long.parseLong(args[1]);
        String tmp = conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT) + "sleep";
        Path inPath = new Path(tmp,numMappers+"_sleep_"+sleepTime);
        Path outPath = new Path(tmp,numMappers+"_sleep_out_"+sleepTime);
        Util.writeNLineHDFSFile(conf, inPath, numMappers);
        Util.setConfParameters(conf, args, 2);
        conf.setLong(BenchmarkConfig.SLEEP_CONF, sleepTime*1000);
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        Job job = Job.getInstance(conf, "Sleep " + numMappers + " : " + sleepTime);
        job.setJarByClass(Sleep.class);
        job.setMapperClass(SleepMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(NLineInputFormat.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        Util.cleanup(conf,inPath, outPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SleepMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Thread.sleep(context.getConfiguration().getLong(
                BenchmarkConfig.SLEEP_CONF, BenchmarkConfig.DEFAULT_SLEEP_VALUE));
        }
    }
}
