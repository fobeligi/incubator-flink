package org.apache.flink.streaming.examples.lambda;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.LocalExecutor;


public class BatchJob implements Runnable {

    private Plan plan;
    private LocalExecutor executor;

    public BatchJob(ExecutionEnvironment env,LocalExecutor exec) {
        this.plan = env.createProgramPlan();
        this.executor = exec;
    }

    @Override
    public void run() {
        while (true){
            try {
                executor.executePlan(plan);
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
