package util;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

public class Util {
    private static final Logger logger = Logger.getLogger("cluster-benchmark");

    public static void checkArgs(String[] args, int numOfArgs, String errorMsg) {
        final String ERR_INCORRECT_ARGS = "Incorrect number of arguments expected " + numOfArgs;
        if (args.length < numOfArgs) {
            logger.warning(ERR_INCORRECT_ARGS+"\n"+errorMsg);
            System.exit(1);
        }
    }

    public static int setConfParameters(Configuration conf, String[] args, int fromIndex) {
        String[] splits;
        for (int i = fromIndex; i < args.length; i++) {
            splits = args[i].split("=");
            conf.set(splits[0], splits[1]);
        }
        return args.length - fromIndex;
    }

    public static List<Double> getDoubleListFromString(String s) {
        List<Double> result = new ArrayList<>();
        if (s==null || s.trim().isEmpty())
            return result;
        for (String d : s.split(",")) {
            result.add(Double.parseDouble(d));
        }
        return result;
    }

    public static double getMean(Collection<Double> numbers) {
        if(numbers.size()<1){
            return 0;
        }
        double sum = 0;
        for (double n : numbers) {
            sum += n;
        }
        return sum / numbers.size();
    }

    public static double getVariance(Collection<Double> numbers) {
          if(numbers.size()<1){
              return 0;
          }
          double mean = getMean(numbers);

          double sum = 0;
          for (double n : numbers) {
              sum += (mean - n) * (mean - n);
          }
          return sum / numbers.size();
      }

    public static void writeNLineHDFSFile(Configuration conf, Path path, int N) throws
        IOException {
        FileSystem hdfs = path.getFileSystem(conf);
        FSDataOutputStream writer = hdfs.create(path);
        for (int i = 1; i <= N; i++) {
            writer.write(String.format("%d\n",i).getBytes());
        }
        writer.close();
    }

    public static String getHistoryURI(Configuration conf) {
        return
            "http://" + conf.get("mapreduce.jobhistory.webapp.address") + BenchmarkConfig.HISTORY_PREFIX;
    }

    public static void cleanup(Configuration conf, Path inPath, Path outPath)
        throws IOException {
        if (conf.getBoolean(BenchmarkConfig.BENCHMARK_DELETE_ON_EXIT, BenchmarkConfig.BENCHMARK_DELETE_ON_EXIT_DEFAULT)) {
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.delete(inPath, true);
            hdfs.delete(outPath, true);
        }
    }

  /**
   * Makes a rest call to the given URI
   *
   * @param urlString pointing to the REST end-point
   * @return Response as a String
   */
  public static String makeRestCall(String urlString) {
      logger.info("calling : " + urlString);
      return makeRestCallInternal(urlString);
  }

  private static String makeRestCallInternal(String urlString){
      HttpClient client = new HttpClient();
      GetMethod method = new GetMethod(urlString);
      try {
          method.getParams().setParameter(
              HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
          int returnCode = client.executeMethod(method);
          if (returnCode != HttpStatus.SC_OK) {
              logger.warning(urlString + " Failed : " + method.getStatusLine());
              method.releaseConnection();
              return null;
          }
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          InputStream responseBodyAsStream = method.getResponseBodyAsStream();
          byte[] byteArray = new byte[1024];
          int count;
          while ((count = responseBodyAsStream.read(byteArray, 0, byteArray.length)) > 0) {
              outputStream.write(byteArray, 0, count);
          }
          return new String(outputStream.toByteArray(), "UTF-8");
      } catch (IOException e) {
          method.releaseConnection();
      } finally {
          method.releaseConnection();
      }
      return null;
  }

    public static Table<String, String, Double> getTaskStats(String jobId, String historyURI, TASK_TYPE type) {
        Table<String, String, Double> times = HashBasedTable.create();
        JsonParser parser = new JsonParser();
        JsonObject
            tree = parser.parse(makeRestCall(historyURI+"/"+jobId+"/tasks")).getAsJsonObject();
        for (JsonElement task : tree.get("tasks").getAsJsonObject().get("task").getAsJsonArray()) {
            String taskId=task.getAsJsonObject().get("id").getAsString();
            String node= getNodeAddressOfAttempt(historyURI+"/"+jobId+"/tasks/"+taskId,parser);
            if (taskId.contains("_m_") && (type== TASK_TYPE.MAP || type== TASK_TYPE.ALL)) {
                times.put(node, taskId, task.getAsJsonObject().get("elapsedTime").getAsDouble());
            }else if (taskId.contains("_r_") && (type== TASK_TYPE.REDUCE || type == TASK_TYPE.ALL)) {
                times.put(node, taskId, task.getAsJsonObject().get("elapsedTime").getAsDouble());
            }
        }
        return times;
    }

    public static String getNodeAddressOfAttempt(String taskURI, JsonParser parser){
        JsonArray attempt = parser.parse(makeRestCall(taskURI+"/attempts"))
                .getAsJsonObject().get("taskAttempts")
                .getAsJsonObject().get("taskAttempt").getAsJsonArray();
            // check for successful
            return attempt.get(0).getAsJsonObject().get("nodeHttpAddress").getAsString();

    }

    public enum TASK_TYPE{MAP, REDUCE, ALL}
}