import com.google.common.collect.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.*;

import java.io.IOException;
import java.util.Map;

public class DFSWrite extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DFSWrite(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Util.checkArgs(args, 2, "DFSWrite.jar numberOfReducers size [optional arguments]");
    Configuration conf = new Configuration();
    int numReducers = Integer.parseInt(args[0]);
    long size = Long.parseLong(args[1]);
    Util.setConfParameters(conf, args, 2);
    conf.setLong(BenchmarkConfig.BENCHMARK_WRITESPERNODE, numReducers);
    conf.setLong(BenchmarkConfig.SIZE_CONF, size);
    String tmp = conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT)+"DFSWrite";
    Path inPath = new Path(tmp, numReducers + "_DFSWrite_" + size);
    Path outPath = new Path(tmp, numReducers + "_DFSWrite_out_" + size);
    Util.writeNLineHDFSFile(conf, inPath, 1);

    Job job = Job.getInstance(conf, "DFSWrite " + numReducers + " -> " + size);
    job.setJarByClass(DFSWrite.class);
    job.setMapperClass(WriteBenchMapper.class);
    job.setReducerClass(WriteBenchReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(numReducers);

    FileInputFormat.addInputPath(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);
    job.submit();
    System.out.println(job.getJobID());
    System.out.println(job.getHistoryUrl());
    boolean status = job.waitForCompletion(true);
    String historyURI = Util.getHistoryURI(conf);
    Table<String, String, Double> taskStats = Util
        .getTaskStats(job.getJobID().toString(), historyURI, Util.TASK_TYPE.REDUCE);

    // Print the analysis
    System.out.println("Node\tNumTasks\tMean\tVariance");
    System.out.println("===========================================");
    for (Map.Entry<String, Map<String, Double>> e : taskStats.rowMap().entrySet()) {
      System.out.println(e.getKey()+"\t"+e.getValue().size()+"\t"+
          Util.getMean(e.getValue().values())/1000+"\t"+
          Util.getVariance(e.getValue().values())/1000000);
    }
    System.out.println("Mean Reduce Time : "+ Util.getMean(taskStats.values())/1000);
    System.out.println("Variance Reduce Time: "+ Util.getVariance(taskStats.values())/1000000);

    Util.cleanup(conf,inPath, outPath);

    return status?0:1;
  }

  public static class WriteBenchMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      IntWritable zero = new IntWritable(1);
      for (int i = 0; i < context.getConfiguration().getInt(
          BenchmarkConfig.BENCHMARK_WRITESPERNODE, BenchmarkConfig.REDUCE_DEFAULT); i++) {
        context.write(new IntWritable(i), zero);
      }
    }
  }

  public static class WriteBenchReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long size = context.getConfiguration().getLong(
          BenchmarkConfig.SIZE_CONF, BenchmarkConfig.SIZE_DEFAULT);
      for (int i = 0; i < size / 1000; i++) {
        context.write(key, new Text(new byte[996]));
      }
    }
  }
}
