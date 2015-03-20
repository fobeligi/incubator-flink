package org.apache.flink.streaming.examples.lambda;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction;


public class StreamingJob implements Runnable {

    StreamExecutionEnvironment streamEnv ;
    DataSet dataSet ;

    public StreamingJob(String JAR,DataSet ds) {
        this.streamEnv = StreamExecutionEnvironment.createRemoteEnvironment(
                "127.0.0.1", 6123, 1, JAR);
        this.dataSet = ds;
    }

    @Override
    public void run() {

        DataStream<Tuple2<String, Integer>> dataSetStream = streamEnv.readFileStream(
                "file:///home/fobeligi/FlinkTmp", dataSet.getType(), 1000,
                FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED);

        dataSetStream.print();
//		dataSetStream.project(1).types(Integer.class).print();
        try {
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
