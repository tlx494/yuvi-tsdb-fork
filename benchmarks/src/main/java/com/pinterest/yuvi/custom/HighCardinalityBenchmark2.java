package com.pinterest.yuvi.custom;

import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.extra.Plot;
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

import java.awt.*;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class HighCardinalityBenchmark2 {

    private static final Logger logger = LogManager.getLogger(HighCardinalityBenchmark2.class);

    private long startMs = 1600000000;

    private int periodIntervalMs = 5_000;  // set interval period of x
    private int totalPeriods = (60/(periodIntervalMs/1000)) * 60;  // 60 to make it an hour
    private int totalMinutes = (periodIntervalMs * totalPeriods) / (60 * 1000);

    private long endMs = this.startMs + (this.totalPeriods * this.periodIntervalMs);

    // the cardinality of each tag
    private int numHosts = 10;
    private int numRegions = 10;
    private int numEnvs = 3;
    private int numShards = 70;
    private DecimalFormat df;

    private boolean change = true;
    private boolean actuallyAdd = false;

    private boolean useSeed = true;
    private long seed = 29;

    private double percentageDeltaDefault = 30;
    private int vertDisp = 70;
    private double smooth = 0.9;

    private int startPosition = 10;
    private int endPosition = 80;


    private Random rand;

    private ChunkManager chunkManager;
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HighCardinalityBenchmark2.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    public Random getRand() {
        if (rand != null) {
            return rand;
        }
        if (useSeed) {
            System.out.println("NEW RAND");
            rand = new Random(seed);
            return rand;
        }
        System.out.println("OLD RAND");
        rand = new Random();
        return rand;
    }

    /*
    adds metrics once a second to the db
     */
//    make the first 5 minutes full resolution!!!!!!!!!!!!!!!
    @Setup(Level.Trial)
    public void setupRequests() {

        logger.info("Setting up...");
//        logger.info(String.format("Total cardinality: %d", numHosts * numRegions * numEnvs * numShards));

        chunkManager = new ChunkManager("test", 10_000_000);
        df = new DecimalFormat("#");
        // can generate high cardinality without generating so many entries. get a hashmap of previously used values...?

//        ArrayList<ArrPoint> points = genPoints();
//        ArrayList<ArrPoint> points = genGradualIncrease();
        ArrayList<ArrPoint> points = genCPUSpike();
        ArrayList<ArrPoint> pointsCompressed = new ArrayList<>();

        int metricsAdded = 0;

        int totalPoints = points.size();
        int a = 0;

        double seconds_passed = 0;

        double lastRecorded = -1;
        double previousSeen = -1;

        double threshold = 80;
        boolean firstMetric = true;

        long startTime = System.currentTimeMillis();

        for (long ms = this.startMs; ms < this.endMs; ms += this.periodIntervalMs) {
            logger.info(String.format("Adding metrics at time %d...", ms));
            ArrPoint p = points.get(a);
            System.out.println(p);
            String value = Double.toString(p.y);
            p.seconds = seconds_passed;
            points.set(a, p);
//            String metric = String.format("put metric1.test %d %s a=a1", ms, value);
            a++;

//            testing change
            if (change) {
                if (firstMetric) {
                    firstMetric = false;
                    lastRecorded = p.y;
                    pointsCompressed.add(p);
                    addMetricIntoChunkManager(ms, value);
//                    chunkManager.addMetric(metric);
                    System.out.println("INITIAL ADDING " + p);
                    metricsAdded++;
                } else {
//                    keep first 5 mins
                    if (ms < startMs + 1000 * 60 * 5) {
                        pointsCompressed.add(p);
                        addMetricIntoChunkManager(ms, value);
                        System.out.println("FIRST 5 MIN ADDING" + p);
                    } else {
                        double percentageDelta = Math.abs((lastRecorded - p.y)/lastRecorded) * 100;
                        //                5% change in the metric (PLAY WITH THIS IN GRAPHS!!!!
                        if (percentageDelta > percentageDeltaDefault) { // 10 %
                            pointsCompressed.add(p);
                            lastRecorded = p.y;
                            System.out.println("ADDING " + p);
                            //                        chunkManager.addMetric(metric);
                            addMetricIntoChunkManager(ms, value);
                            metricsAdded++;
                        } else if (ms == endMs - periodIntervalMs) {
                            lastRecorded = p.y;
                            pointsCompressed.add(p);
                            System.out.println("LAST ADDING " + p);
                            //                        chunkManager.addMetric(metric);
                            addMetricIntoChunkManager(ms, value);
                            metricsAdded++;
                        }
                    }
                }
//                no change
            } else {
//                chunkManager.addMetric(metric);
                addMetricIntoChunkManager(ms, value);
                metricsAdded++;
            }

            seconds_passed += (double)periodIntervalMs/1000;
        }

        System.out.println("metricsAdded: " + Integer.toString(metricsAdded));
        long endTime = System.currentTimeMillis();
        double setupTime = (double)(endTime - startTime)/1000;
        System.out.println("setup time seconds: " + Double.toString(setupTime));


        if (change) {
            createPlot(pointsCompressed, true);
        } else {
            createPlot(points, false);
        }
        /*
        generate graph that stays in place for a while and has a dip  - choose shapes carefully and graph them. create methods that make a bunch of points over time that represent interesting events (cpu spike, gradual increases, etc.)
        do it for a single time-series for 0.5, 1, 2 hours then multiply that by the tags for cardinality
        Do experiment with 1, 2, 4, 7, 10 tags
         */

        logger.info("Finished setup");
    }

    public void addMetricIntoChunkManager(long ms, String value) {
        if (actuallyAdd) {
            for (int a = 0; a < 10; a++) {
                for (int b = 0; b < 10; b++) {
                    for (int c = 0; c < 10; c++) {
                        for (int d = 0; d < 10; d++) {
                            String metric = String.format("put system.cpu.util %d %s a=%d b=%d c=%d, d=%d", ms, value, a, b, c, d);
                            chunkManager.addMetric(metric);
                        }
                    }
                }
            }
        }
    }

    public void createPlot(ArrayList<ArrPoint> points, boolean compressed) {
        ArrayList<Double> xValues = new ArrayList<>();
        ArrayList<Double> yValues = new ArrayList<>();

        for (int i = 0; i < points.size(); i++) {
            if (points.get(i).seconds == -1) {
                // these points are useless now. stop recording
                break;
            }
            double xval = (points.get(i).seconds)/60;
//            System.out.println(xval);
            xValues.add(xval);
            yValues.add(points.get(i).y);
//            System.out.format("point: %f, %f\n", points.get(i).x, points.get(i).y);
        }

        Plot.DataSeriesOptions opts = null;

        if (change) {
            opts = Plot.seriesOpts()
                    .marker(Plot.Marker.DIAMOND)
                    .markerColor(Color.GREEN);
        } else {
            opts = Plot.seriesOpts();
        }



        Plot plot = Plot.plot(Plot.plotOpts())
                .xAxis("time (m)", Plot.axisOpts()) //.range(0, 100))
                .yAxis("% value", Plot.axisOpts().range(0, 100))
                .series(null, Plot.data().xy(xValues, yValues), opts);
        try {
            String comp = "normal";
            if (compressed) {
                comp = "compressed";
            }
            plot.save("plot--vert-" + Integer.toString(vertDisp) + "--smooth-" + Double.toString(smooth) + "--delta-" + Double.toString(percentageDeltaDefault) + "--" + comp, "png");
        } catch(IOException e) {
            logger.error("Couldn't save plot");
        }
    }

    public ArrayList<ArrPoint> concatArr(ArrayList<ArrPoint> a1, ArrayList<ArrPoint> a2, ArrayList<ArrPoint> a3) {
        ArrayList<ArrPoint> result = new ArrayList<>();
        for (ArrPoint a : a1) {
            result.add(a);
        }
        for (ArrPoint a : a2) {
            result.add(a);
        }
        for (ArrPoint a : a3) {
            result.add(a);
        }
        for (ArrPoint a : result) {
            System.out.println(a);
        }
        return result;
    }


    public ArrayList<ArrPoint> genCPUSpike() {
        int stopPoint = totalMinutes/2;
        int leftStop = 70;

        ArrayList<ArrPoint> pointsLeft = midpointDisplacementHalf(new ArrPoint(0, 30), new ArrPoint(stopPoint, leftStop));
        ArrayList<ArrPoint> pointsMid = new ArrayList<>();
        double x = 0;
        double duration = 60;
        double y = pointsLeft.get(pointsLeft.size()-1).y;

        double factorUp = (100 - y)/(duration / 2) + 0.5;
        double factorDown = (100 - y)/(duration / 2);
        while (x < duration) {
            x++;
            if (x < duration / 2) {
                y += factorUp;
//            } else if (x == duration / 2) {
//                for (int i = 0; i < 10; i++) {
//                    pointsMid.add(new ArrPoint(stopPoint + x, y));
//                }
            } else {
                y -= factorDown;
            }
            if (y > 100) {
                y = 100;
            }
//            System.out.println(y);
            pointsMid.add(new ArrPoint(stopPoint + x, y));
        }
        x++;
        ArrayList<ArrPoint> pointsRight = midpointDisplacementHalf(new ArrPoint(stopPoint + x, leftStop), new ArrPoint(totalMinutes, 40));
        return concatArr(pointsLeft, pointsMid, pointsRight);
    }

    public ArrayList<ArrPoint> genPoints() {
        ArrayList<ArrPoint> points = midpointDisplacement(new ArrPoint(0, startPosition), new ArrPoint(totalMinutes, endPosition));
        return points;
    }

    public ArrayList<ArrPoint> genGradualIncrease() {
        ArrayList<ArrPoint> points = midpointDisplacement(new ArrPoint(0, 30), new ArrPoint(totalMinutes, 70));
        return points;
    }

    public ArrayList<ArrPoint> midpointDisplacement(ArrPoint start, ArrPoint end) {
        return midpointDisplacement(start, end, -1, -1, -1, -1, -1, -1);
    }

    public ArrayList<ArrPoint> midpointDisplacementHalf(ArrPoint start, ArrPoint end) {
        return midpointDisplacement(start, end, -1, -1, -1, totalPeriods/2, -1, -1);
    }

//    it won't finish at 65 because the iterations don't stop when it's at 65
    public ArrayList<ArrPoint> midpointDisplacement(ArrPoint start, ArrPoint end, double smoothness, double verticalDisplacement, int iterations, int pointLimit, double minBound, double maxBound) {
        if (smoothness == -1) {
            smoothness = smooth;
        }
        if (minBound == -1 || maxBound == -1) {
            minBound = 0;
            maxBound = 100;
        }
        if (iterations == -1) {
            iterations = Integer.MAX_VALUE;
        }
        if (verticalDisplacement == -1) {
//            verticalDisplacement = (start.y + end.y)/(double)2;
            verticalDisplacement = vertDisp;
        }
        if (pointLimit == -1) {
//            points recorded every 10 seconds for 15 minutes
            pointLimit = totalPeriods;
        }

        ArrayList<ArrPoint> points = new ArrayList<>();
        points.add(start);
        points.add(end);

        int iteration = 1;
        while (iteration <= iterations && points.size() <= pointLimit) {
            ArrayList<ArrPoint> tmp_points = new ArrayList<>();

            for (int i = 0; i < points.size(); i++) {
                tmp_points.add(points.get(i));
            }

            for (int i = 0; i < tmp_points.size() - 1; i++) {
                double midpointX = (tmp_points.get(i).x + tmp_points.get(i + 1).x)/(double)2;
                double midpointY = (tmp_points.get(i).y + tmp_points.get(i + 1).y)/(double)2;

                double randDisplacement = (getRand().nextDouble() * verticalDisplacement) - (verticalDisplacement/(double)2);
                midpointY += randDisplacement;

                if (midpointY > maxBound) {
                    midpointY = maxBound;
                } else if (midpointY < minBound) {
                    midpointY = minBound;
                }

                ArrPoint midpoint = new ArrPoint(midpointX, midpointY);

                points.add(i + 1, midpoint);
                points.sort(new Comparator<ArrPoint>() {
                    @Override
                    public int compare(ArrPoint o1, ArrPoint o2) {
                        if (o1.x < o2.x) {
                            return -1;
                        } else if (o1.x > o2.x) {
                            return 1;
                        }
                        return 0;
                    }
                });
            }

//            System.out.println("begin iteration printout:");
//            for (arrPoint p : points) {
//                System.out.println(p.x);
//            }

            verticalDisplacement *= Math.pow(2, (-smoothness));
            iteration += 1;
        }

        return points;

    }


    class ArrPoint {
        public double x;
        public double y;
        public double seconds;

        public ArrPoint(double x, double y) {
            this.x = x;
            this.y = y;
            this.seconds = -1;
        }

        public double getX() {
            return x;
        }

        public double getY() {
            return y;
        }

        @Override
        public String toString() {
            return "ArrPoint{" +
                    "x=" + x +
                    ", y=" + y +
                    ", seconds=" + seconds +
                    ", mins=" + seconds/60 +
                    '}';
        }
    }

    @Benchmark
    public void sumRequests() {
        if (!actuallyAdd) {
            logger.info("skipped");
            return;
        }
//        String metric = String.format("put system.cpu.util %d %s host=host-%d region=region-%d env=env-%d, shard=shard-%d, deployment=deployment-%d", ms, value, i, i+1, i+2, i+3, i+4);

        List<TimeSeries> ts = chunkManager.query(Query.parse("system.cpu.util"), this.startMs, this.endMs, QueryAggregation.NONE);
//        List<Point> points = ts.get(0).getPoints();
//        double sum = 0;
//        for (Point p : points) {
////            sum += p.getVal();
//        }

        logger.info("Retrieved TS");
    }
}
