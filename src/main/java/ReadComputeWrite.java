import com.google.common.collect.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.BenchmarkConfig;
import util.Util;

import java.io.IOException;
import java.util.Random;

public class ReadComputeWrite extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReadComputeWrite(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Util.checkArgs(args, 6, "ReadComputeWrite.jar input numReducers mapComputeTime(s) mapWriteSize (bytes) reduceComputeTime reduceWriteSize (bytes) [optional arguments]");
    Configuration conf = new Configuration();
    Path inPath = new Path(args[0]);
    int numOfReducers = Integer.parseInt(args[1]);
    long mapComputeTime = Long.parseLong(args[2])*1000;
    long mapWriteSize = Long.parseLong(args[3]);
    long reduceComputeTime = Long.parseLong(args[4])*1000;
    long reduceWriteSize = Long.parseLong(args[5]);
    Path outPath = new Path(conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT)+ "ReadComputeWrite", inPath.getName() + "_"+Math.abs(new Random().nextInt())+"_read_out");

    Util.setConfParameters(conf, args, 6);
    conf.setInt(BenchmarkConfig.NUM_REDUCERS_CONF, numOfReducers);
    conf.setLong(BenchmarkConfig.MAP_COMPUTE_TIME_CONF, mapComputeTime);
    conf.setLong(BenchmarkConfig.MAP_WRITE_SIZE_CONF, mapWriteSize);
    conf.setLong(BenchmarkConfig.REDUCE_COMPUTE_TIME_CONF, reduceComputeTime);
    conf.setLong(BenchmarkConfig.REDUCE_WRITE_SIZE_CONF, reduceWriteSize);

    Job job = Job.getInstance(conf, "ReadComputeWrite " + inPath);
    job.setJarByClass(ReadComputeWrite.class);
    job.setMapperClass(MapClass.class);
    job.setNumReduceTasks(numOfReducers);
    job.setReducerClass(MixReducer.class);

    FileInputFormat.addInputPaths(job,args[0]);
    FileOutputFormat.setOutputPath(job, outPath);
    boolean isSuccessful = job.waitForCompletion(true);
    String historyURI = Util.getHistoryURI(conf);

    Table<String, String, Double> times = Util
        .getTaskStats(job.getJobID().toString(), historyURI, Util.TASK_TYPE.MAP);
    System.out.println(job.getJobID());
    System.out.println("Mean : " + Util.getMean(times.values())/1000);
    System.out.println("Variance :" + Util.getVariance(times.values())/1000000);
    System.out.println("Num of Maps :" + times.values().size());

    Util.cleanup(conf,inPath, outPath);
    return isSuccessful?0:1;
  }

  public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      long startTime = System.currentTimeMillis();
      long endTime = startTime + context.getConfiguration().getLong(
          BenchmarkConfig.MAP_COMPUTE_TIME_CONF, BenchmarkConfig.MAP_COMPUTE_TIME_DEFAULT);
      int count = 0;
      while (true) {
        count += new Random().nextInt();
        if (System.currentTimeMillis() >= endTime) {
          break;
        }
      }
      context.progress();
      context.setStatus("write+shuffle");
      long size = context.getConfiguration().getLong(
          BenchmarkConfig.MAP_WRITE_SIZE_CONF, BenchmarkConfig.MAP_WRITE_SIZE_DEFAULT);
      for (int i = 0; i < context.getConfiguration().getInt(
          BenchmarkConfig.NUM_REDUCERS_CONF, BenchmarkConfig.NUM_REDUCERS_DEFAULT); i++) {
        for(long j=0;j<size/1000;j++){
          context.write(new IntWritable(i),new Text(new byte[996]));
        }
      }
    }
  }

  public static class MixReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long startTime = System.currentTimeMillis();
      long endTime = startTime + context.getConfiguration().getLong(
          BenchmarkConfig.REDUCE_COMPUTE_TIME_CONF, BenchmarkConfig.REDUCE_COMPUTE_TIME_DEFAULT);
      int count = 0;
      context.setStatus("compute");
      while (true) {
        count += new Random().nextInt();
        if (System.currentTimeMillis() >= endTime) {
          break;
        }
      }
      context.setStatus("write");
      context.progress();
      long size = context.getConfiguration().getLong(
          BenchmarkConfig.REDUCE_WRITE_SIZE_CONF, BenchmarkConfig.REDUCE_WRITE_SIZE_DEFAULT);
      for (int i = 0; i < size / 1000; i++) {
        context.write(key, new Text(new byte[996]));
      }
    }
  }
}
