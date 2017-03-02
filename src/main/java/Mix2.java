import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.BenchmarkConfig;
import util.Util;

import java.io.IOException;
import java.util.Random;

public class Mix2 extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Mix2(), args);
    System.exit(res);
  }

  @Override public int run(String[] args) throws Exception {
    Util.checkArgs(args, 6,
        "Mix.jar numberOfMappers numberOfReducers mapComputeTime(s) mapWriteSize reduceComputeTime(s) reduceWriteSize [optional arguments]");
    Configuration conf = new Configuration();
    Util.setConfParameters(conf, args, 6);

    Path inPath = new Path(conf.get(BenchmarkConfig.BENCHMARK_TMPPATH,
        BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT),
        args[0] + "_" + args[1] + "_" + args[2] + "mix");
    Path outPath = new Path(conf.get(BenchmarkConfig.BENCHMARK_TMPPATH,
        BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT),
        args[1] + "_" + args[2] + "_mix_out");
    int numOfMaps = Integer.parseInt(args[0]);
    int numOfReducers = Integer.parseInt(args[1]);
    long mapComputeTime = Integer.parseInt(args[2]) * 1000;
    long mapWriteSize = Long.parseLong(args[3]);
    long reduceComputeTime = Long.parseLong(args[4]) * 1000;
    long reduceWriteSize = Long.parseLong(args[5]);

    Util.writeNLineHDFSFile(conf, inPath, numOfMaps);
    conf.setInt(BenchmarkConfig.NUM_REDUCERS_CONF, numOfReducers);
    conf.setLong(BenchmarkConfig.MAP_COMPUTE_TIME_CONF, mapComputeTime);
    conf.setLong(BenchmarkConfig.MAP_WRITE_SIZE_CONF, mapWriteSize);
    conf.setLong(BenchmarkConfig.REDUCE_COMPUTE_TIME_CONF, reduceComputeTime);
    conf.setLong(BenchmarkConfig.REDUCE_WRITE_SIZE_CONF, reduceWriteSize);

    Job job = Job.getInstance(conf);
    job.setJobName(
        "Mix2 " + numOfMaps + " : " + mapComputeTime + " : " + mapWriteSize
            + "->" + numOfReducers + " : " + reduceComputeTime + " : "
            + reduceWriteSize);
    job.setJarByClass(Mix2.class);
    job.setMapperClass(MixMapper.class);
    job.setReducerClass(MixReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(numOfReducers);
    job.setInputFormatClass(NLineInputFormat.class);

    NLineInputFormat.addInputPath(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);
    boolean isSuccessful = job.waitForCompletion(true);
    Util.cleanup(conf, inPath, outPath);
    return isSuccessful ? 0 : 1;
  }

  public static class MixMapper
      extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      Random random = new Random();
      int choose = random.nextInt() % 10;
      switch (choose) {
      case 1:
      case 2:
      case 3:
        long startTime = System.currentTimeMillis();
        long endTime = startTime + context.getConfiguration()
            .getLong(BenchmarkConfig.MAP_COMPUTE_TIME_CONF,
                BenchmarkConfig.MAP_COMPUTE_TIME_DEFAULT);
        int count = 0;
        while (true) {
          count += new Random().nextInt();
          if (System.currentTimeMillis() >= endTime) {
            break;
          }
        }
        break;
      case 4:
      case 5:
        Thread.sleep(context.getConfiguration()
            .getLong(BenchmarkConfig.MAP_COMPUTE_TIME_CONF,
                BenchmarkConfig.MAP_COMPUTE_TIME_DEFAULT));
        break;

      case 6:
      case 7:
      case 8:
      case 9:
        long size = context.getConfiguration()
            .getLong(BenchmarkConfig.MAP_WRITE_SIZE_CONF,
                BenchmarkConfig.MAP_WRITE_SIZE_DEFAULT);
        for (int i = 0; i < context.getConfiguration()
            .getInt(BenchmarkConfig.NUM_REDUCERS_CONF,
                BenchmarkConfig.NUM_REDUCERS_DEFAULT); i++) {
          for (long j = 0; j < size / 1000; j++) {
            context.write(new IntWritable(i), new Text(new byte[1000]));
          }
        }
        break;
      }
    }
  }

  public static class MixReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      long startTime = System.currentTimeMillis();
      long endTime = startTime + context.getConfiguration()
          .getLong(BenchmarkConfig.REDUCE_COMPUTE_TIME_CONF,
              BenchmarkConfig.REDUCE_COMPUTE_TIME_DEFAULT);
      int count = 0;
      while (true) {
        count += new Random().nextInt();
        if (System.currentTimeMillis() >= endTime) {
          break;
        }
      }

      long size = context.getConfiguration()
          .getLong(BenchmarkConfig.REDUCE_WRITE_SIZE_CONF,
              BenchmarkConfig.REDUCE_WRITE_SIZE_DEFAULT);
      for (int i = 0; i < size / 1000; i++) {
        context.write(key, new Text(new byte[996]));
      }
    }
  }
}
