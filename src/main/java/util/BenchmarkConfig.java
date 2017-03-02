package util;

public interface BenchmarkConfig {
  String BENCHMARK_TMPPATH = "benchmark.tmppath";
  String BENCHMARK_TMPPATH_DEFAULT = "/tmp/";
  String BENCHMARK_WRITESPERNODE ="benchmark.writespernode";
  long BYTES_TO_MB = 1000000;
  String BUSY_TIME_CONF = "benchmark.busy";
  String SLEEP_CONF = "benchmark.sleep";
  long DEFAULT_SLEEP_VALUE = 0;
  int REDUCE_DEFAULT = 1;
  String BENCHMARK_DELETE_ON_EXIT = "benchmark.deleteonExit";
  boolean BENCHMARK_DELETE_ON_EXIT_DEFAULT = true;
  String MAP_WRITE_SIZE_CONF = "benchmark.mix.map.writesize";
  int MAP_WRITE_SIZE_DEFAULT = 10000;
  String REDUCE_WRITE_SIZE_CONF = "benchmark.mix.reduce.writesize";
  int REDUCE_WRITE_SIZE_DEFAULT = 10000;
  String MAP_COMPUTE_TIME_CONF = "benchmark.mix.map.computetime";
  int MAP_COMPUTE_TIME_DEFAULT = 60;
  String REDUCE_COMPUTE_TIME_CONF = "benchmark.mix.reduce.computetime";
  String NUM_REDUCERS_CONF = "benchmark.mix.numreducers";

  String HISTORY_PREFIX = "/ws/v1/history/mapreduce/jobs";

  String LOG_WAITTIME = "benchmark.shuffle.logwaittime";
  int LOG_WAITTIME_DEFAULT = 10;
  int NUM_REDUCERS_DEFAULT = 1;
  int REDUCE_COMPUTE_TIME_DEFAULT = 60;
  String SIZE_CONF = "benchmark.size";
  long SIZE_DEFAULT = 10000;
}
