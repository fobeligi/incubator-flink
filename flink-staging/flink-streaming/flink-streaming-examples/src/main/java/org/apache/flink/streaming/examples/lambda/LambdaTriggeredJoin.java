package org.apache.flink.streaming.examples.lambda;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction;

import java.util.Random;


public class LambdaTriggeredJoin {

    private static final Logger log = LoggerFactory.getLogger(LambdaTriggeredJoin.class);

    private static final String JAR = "/home/fobeligi/workspace/incubator-flink/flink-staging/flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0.9-SNAPSHOT-LambdaTriggeredJoin.jar";

    public static LocalExecutor exec = new LocalExecutor(false);

    private static Random randomGenerator = new Random();

    public static void main(String[] args) throws Exception {

        exec.setTaskManagerNumSlots(8);
        exec.start();

        ExecutionEnvironment batchEnv = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
                6123, 1, JAR);

        DataSet<Integer> dataSet = batchEnv.fromElements(1000, 2000, 3000, 4000).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + randomGenerator.nextInt(70);
            }
        });
        dataSet.write(new TypeSerializerOutputFormat<Integer>(), "/home/fobeligi/FlinkTmp/temp2",
                FileSystem.WriteMode.OVERWRITE);

        try {
            new Thread(new BatchJob(batchEnv, exec)).start();
            new Thread(new StreamingJob(JAR,dataSet)).start();
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            exec.stop();
        }

    }

}
