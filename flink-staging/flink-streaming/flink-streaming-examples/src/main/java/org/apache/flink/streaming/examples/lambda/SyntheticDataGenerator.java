package org.apache.flink.streaming.examples.lambda;


import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class SyntheticDataGenerator {

    private boolean withDrift;
    private double withError;
    private double gaussianMean;
    private double gaussianVariance;
    private Function PatternFunction;
//    private int numberOfDataPoints;
    NormalDistribution myDataDistribution;
    NormalDistribution myErrorDistribution;

    private List<Tuple2<Double,Integer>> dataSet;

    public SyntheticDataGenerator(){
        this(false, 0.1);
    }

    public SyntheticDataGenerator(boolean drift,double error){
        this.withDrift = drift;
        this.withError = error;
        this.gaussianMean = 0.0;
        this.gaussianVariance = 1.0;

        //TODO::change one-dimensional to multidimensional Gaussian distribution
        myDataDistribution = new NormalDistribution(gaussianMean,gaussianVariance);
        myErrorDistribution = new NormalDistribution(0.0,withError);
    }

    public void generateLabeledData(int dataPoints){
//        this.numberOfDataPoints = dataPoints;
        if (withDrift){
            dataSet = generateLabeledDataWithDrift(dataPoints);
        }
        else {
            dataSet = new ArrayList<Tuple2<Double,Integer>>();
            Double x_temp;
            for (int i=0;i<dataPoints;i++){
                x_temp = myDataDistribution.sample();
                dataSet.add(new Tuple2<Double,Integer>(x_temp,patternFunction(x_temp)));
            }
        }
        System.out.println("-----------------------------------\n" + dataSet.toString());

    }

    private int patternFunction(Double x_temp) {
        double point = Math.sin(x_temp) + 2*x_temp ;
        if (point >= 2*x_temp) return 1;
        else return 0;
    }

    private List<Tuple2<Double,Integer>> generateLabeledDataWithDrift(int dataPoints) {
        return new ArrayList<Tuple2<Double,Integer>>();
    }


    public boolean isWithDrift() {
        return withDrift;
    }

    public void setWithDrift(boolean withDrift) {
        this.withDrift = withDrift;
    }

    public double getWithError() {
        return withError;
    }

    public void setWithError(double withError) {
        this.withError = withError;
    }

    public double getGaussianMean() {
        return gaussianMean;
    }

    public void setGaussianMean(double gaussianMean) {
        this.gaussianMean = gaussianMean;
    }

    public double getGaussianVariance() {
        return gaussianVariance;
    }

    public void setGaussianVariance(double gaussianVariance) {
        this.gaussianVariance = gaussianVariance;
    }

    public List<Tuple2<Double,Integer>> getDataSet() {
        return dataSet;
    }


    public static void main(String[] args){
        SyntheticDataGenerator t = new SyntheticDataGenerator();
        t.generateLabeledData(100);
    }
}
