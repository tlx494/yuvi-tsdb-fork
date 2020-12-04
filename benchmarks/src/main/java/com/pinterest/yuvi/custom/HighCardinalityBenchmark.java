package com.pinterest.yuvi.custom;

import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Query;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class HighCardinalityBenchmark {

    private static final Logger logger = LogManager.getLogger(HighCardinalityBenchmark.class);

    private long startMs = 1600000000;

    private int totalPeriods = 10;
    private int periodIntervalMs = 1000;  // set interval period of 1s

    private long endMs = this.startMs + (this.totalPeriods * this.periodIntervalMs);

    // the cardinality of each tag
    private int numHosts = 10;
    private int numRegions = 10;
    private int numEnvs = 3;
    private int numShards = 70;

    private ChunkManager chunkManager;
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HighCardinalityBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    /*
    adds metrics once a second to the db
     */
    @Setup(Level.Trial)
    public void setupRequests() {
        logger.info("Setting up...");
        chunkManager = new ChunkManager("test", 10000000);

        DecimalFormat df = new DecimalFormat("#");

        // can generate high cardinality without generating so many entries. get a hashmap of previously used values...?

        for (long ms = this.startMs; ms < this.endMs; ms += this.periodIntervalMs) {
            long currentTime = System.currentTimeMillis();
            logger.info(String.format("Adding metrics at time %d...", ms));

            for (int host = 0; host < this.numHosts; host++) {
                for (int shard = 0; shard < this.numShards; shard++) {
                    for (int env = 0; env < this.numEnvs; env++) {
                        for (int region = 0; region < this.numRegions; region++) {
                            String value = df.format(Math.random() * 100);  // set value to a random number between 0 and 100
                            String metric = String.format("put jira.nginx.response.status.count %d %s host=host-%d region=region-%d env=env-%d, shard=shard-%d", ms, value, host, region, env, shard);
                            chunkManager.addMetric(metric);
                        }
                    }
                }
            }
            while (System.currentTimeMillis() < currentTime + 1000) {
                // wait for 1 second
            }
        }

        logger.info("Finished setup");
    }

    @Benchmark
    public void sumRequests() {
        List<TimeSeries> ts = chunkManager.query(Query.parse("jira.nginx.response.status.count host=host-0"), this.startMs, this.endMs, QueryAggregation.ZIMSUM);
        List<Point> points = ts.get(0).getPoints();
        double sum = 0;
        for (Point p : points) {
            sum += p.getVal();
        }

        logger.info(String.format("Total request count: %f", sum));
    }
}
