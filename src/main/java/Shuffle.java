import util.BenchmarkConfig;
import util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Shuffle extends Configured implements Tool {
  private static final Logger logger = Logger.getLogger("cluster-benchmark");

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Shuffle(), args);
    System.exit(res);
  }

  @Override public int run(String[] args) throws Exception {
    Util.checkArgs(args, 3,
        "Shuffle.jar numberOfMappers numberOfReducers sizeInMB [optional arguments]");
    Configuration conf = new Configuration();
    Util.setConfParameters(conf, args, 3);

    String tmp = conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, "/tmp/")
        + "perforator_write";
    Path inPath =
        new Path(tmp, args[0] + "_" + args[1] + "_" + args[2] + "_shuffle");
    Path outPath =
        new Path(tmp, args[0] + "_" + args[1] + "_" + args[2] + "_shuffle_out");
    int numOfMaps = Integer.parseInt(args[0]);
    int numOfReducers = Integer.parseInt(args[1]);
    long sizeInMB = Long.parseLong(args[2]) * BenchmarkConfig.BYTES_TO_MB;

    Util.writeNLineHDFSFile(conf, inPath, numOfMaps);
    conf.setInt(BenchmarkConfig.NUM_REDUCERS_CONF, numOfReducers);
    conf.setLong(BenchmarkConfig.SIZE_CONF, sizeInMB);

    Job job = Job.getInstance(conf);
    job.setJobName(
        "Shuffle " + numOfMaps + "->" + numOfReducers + " : " + sizeInMB);
    job.setJarByClass(Shuffle.class);
    job.setMapperClass(ShuffleBenchMapper.class);
    job.setReducerClass(EmptyReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(numOfReducers);
    job.setInputFormatClass(NLineInputFormat.class);

    NLineInputFormat.addInputPath(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);
    boolean isSuccessful = job.waitForCompletion(true);
    Util.cleanup(conf,inPath,outPath);
    Thread.sleep(conf.getLong(BenchmarkConfig.LOG_WAITTIME, BenchmarkConfig.LOG_WAITTIME_DEFAULT) * 1000);
    System.out.println("======== Collecting Shuffle Statistics =========");
    FileSystem hdfs = FileSystem.get(conf);
    String logPath =
        conf.get("yarn.nodemanager.remote-app-log-dir") + Path.SEPARATOR + job
            .getUser() + Path.SEPARATOR + conf
            .get("yarn.nodemanager.remote-app-log-dir-suffix") + Path.SEPARATOR
            + job.getJobID().toString().replace("job", "application");

    Map<String, Long> startTimes = new HashMap<>();
    Map<String, Long> endTimes = new HashMap<>();
    RemoteIterator<LocatedFileStatus> itr =
        hdfs.listFiles(new Path(logPath), true);
    Pattern pattern = Pattern.compile("(.*) INFO (.*) attempt_([^ ]*)(.*)");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    List<Double> diff = new ArrayList<>();
    while (itr.hasNext()) {
      LocatedFileStatus next = itr.next();
      logger.warning("Reading : " + next.getPath());
      BufferedReader fis =
          new BufferedReader(new InputStreamReader(hdfs.open(next.getPath())));
      startTimes.clear();
      endTimes.clear();
      String line;

      while ((line = fis.readLine()) != null) {
        if (line.contains(" about to ")) {
          Matcher matcher = pattern.matcher(line);
          matcher.find();
          startTimes
              .put(matcher.group(3), format.parse(matcher.group(1)).getTime());
        } else if (line.contains(" Read ")) {
          Matcher matcher = pattern.matcher(line);
          matcher.find();
          endTimes
              .put(matcher.group(3), format.parse(matcher.group(1)).getTime());
        }
      }
      assert startTimes.keySet().equals(endTimes.keySet());
      if (startTimes.size() > 0) {
        System.out.println("======================" + next.getPath());
        double sum = 0;
        for (Map.Entry<String, Long> start : startTimes.entrySet()) {
          System.out.println(
              start.getKey() + "\t" + start.getValue() + "\t" + endTimes
                  .get(start.getKey()) + "\t" + (endTimes.get(start.getKey())
                  - start.getValue()));
          sum += endTimes.get(start.getKey()) - start.getValue();
        }
        diff.add(sum / startTimes.size());
      }
    }
    System.out.println("Reducer averages : " + diff);
    System.out.println("Final Average : " + Util.getMean(diff));
    System.out.println("Variance : " + Util.getVariance(diff));

    Util.cleanup(conf, inPath, outPath);
    hdfs.close();
    return isSuccessful ? 0 : 1;
  }

  public static class ShuffleBenchMapper
      extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      long size = context.getConfiguration().getLong(BenchmarkConfig.SIZE_CONF, BenchmarkConfig.SIZE_DEFAULT);
      Text text = new Text(new byte[1000]);
      IntWritable index = new IntWritable();
      for (int i = 0; i < context.getConfiguration()
          .getInt(
              BenchmarkConfig.NUM_REDUCERS_CONF, BenchmarkConfig.NUM_REDUCERS_DEFAULT); i++) {
        for (int j = 0; j < size / 1000; j++) {
          index.set(i);
          context.write(index, text);
        }
      }
    }
  }

  public static class EmptyReducer
      extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

    }
  }
}
