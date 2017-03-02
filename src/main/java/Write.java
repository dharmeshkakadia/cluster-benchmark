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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Write extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Write(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Util.checkArgs(args, 2, "Write.jar numberOfMapsPerNode writeSizePerMap (MB) [optional arguments]");
        List<Double> mapsPerNode = Util.getDoubleListFromString(args[0]);
        List<Double> writePerMapMB = Util.getDoubleListFromString(args[1]);
        StringBuilder result = new StringBuilder();
        for (Double maps : mapsPerNode) {
            int numOfMaps = maps.intValue();
            for (Double size : writePerMapMB) {
                Configuration conf = new Configuration();
                Util.setConfParameters(conf, args, 2);
                int numOfReducers = conf.getInt(BenchmarkConfig.NUM_REDUCERS_CONF,BenchmarkConfig.NUM_REDUCERS_DEFAULT);
                conf.setLong(BenchmarkConfig.BENCHMARK_WRITESPERNODE, numOfMaps);
                conf.setLong(BenchmarkConfig.SIZE_CONF, (long) Math.floor(size * BenchmarkConfig.BYTES_TO_MB));
                conf.setBoolean("mapreduce.map.speculative", false);
                conf.setBoolean("mapreduce.reduce.speculative",false);
                String tmp = conf.get(BenchmarkConfig.BENCHMARK_TMPPATH, BenchmarkConfig.BENCHMARK_TMPPATH_DEFAULT) + "write";
                Path inPath = new Path(tmp, numOfMaps + "_write_" + size);
                Path outPath = new Path(tmp, numOfMaps + "_write_out_" + size);
                Util.writeNLineHDFSFile(conf, inPath, numOfMaps);

                Job job = Job.getInstance(conf, "Write " + numOfMaps + " x " + numOfReducers + " -> " + size);
                job.setJarByClass(Write.class);
                job.setMapperClass(WriteBenchMapper.class);
                job.setReducerClass(WriteBenchReducer.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setInputFormatClass(NLineInputFormat.class);
                job.setNumReduceTasks(numOfReducers);

                FileInputFormat.addInputPath(job, inPath);
                FileOutputFormat.setOutputPath(job, outPath);
                job.submit();
                System.out.println(job.getJobID());
                job.waitForCompletion(true);
                String historyURI = Util.getHistoryURI(conf);
                Table<String, String, Double> taskStats = Util
                    .getTaskStats(job.getJobID().toString(), historyURI, Util.TASK_TYPE.MAP);

                result.append(String.format("%d\t%.2f\t%.2f\n", numOfMaps, size, Util
                    .getMean(taskStats.values())/1000));

                System.out.println("Mean Map Time : " + Util.getMean(taskStats.values()) / 1000);
                System.out.println("Variance Map Time: " + Util.getVariance(taskStats.values()) / 1000000);
                System.out.println("Node\tNumTasks\tSize\tMean\tVariance");
                System.out.println("==========================================");
                for (Map.Entry<String, Map<String, Double>> e : taskStats.rowMap().entrySet()) {
                    System.out.println(e.getKey() + "\t" + e.getValue().size()+"\t"+ size + "\t" + Util.getMean(e.getValue().values()) / 1000 + "\t" + Util.getVariance(e.getValue().values()) / 1000000);
                }
                Util.cleanup(conf, inPath, outPath);
            }
        }
        return 1;
    }

    public static class WriteBenchMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int count=Integer.MIN_VALUE;
            IntWritable intWritable = new IntWritable();
            long size = context.getConfiguration().getLong(
                BenchmarkConfig.SIZE_CONF, BenchmarkConfig.SIZE_DEFAULT);
            for (int i = 0; i < size / 1000; i++) {
                intWritable.set(count++);
                context.write(intWritable, new Text(new byte[996]));
            }
        }
    }

    public static class WriteBenchReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }
}
