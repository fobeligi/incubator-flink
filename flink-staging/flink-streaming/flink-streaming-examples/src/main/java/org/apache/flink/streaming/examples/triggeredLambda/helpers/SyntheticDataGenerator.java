/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.examples.triggeredLambda.helpers;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SyntheticDataGenerator {

	private static int intervals_Of_Drift = 4;
	private boolean withDrift;
	private double withError;
	private boolean withNoise;
	private double gaussianMean;
	private double gaussianVariance;
	NormalDistribution myErrorDistribution;
	private String filePath;  // = SyntheticDataGenerator.class.getPackage().toString().replace(".", "/") + "/dataSets-files/";

	private List<Tuple> dataSet;

	public SyntheticDataGenerator() {
		this(false, 0.1);
	}

	public SyntheticDataGenerator(boolean drift, double error) {
		this.withDrift = drift;
		if (error==0.0){
			this.withError = 0.0000000001;
		}else {
			this.withError = error;
		}
		this.withNoise = false;
		this.gaussianMean = 0.0;
		this.gaussianVariance = 1.0;

//TODO::change one-dimensional to multidimensional Gaussian distribution
		myErrorDistribution = new NormalDistribution(0.0,withError);
		filePath = System.getProperty("user.dir") + "/dataSet-files/";
	}

	public SyntheticDataGenerator(boolean drift, double error, boolean noise) {
		this.withDrift = drift;
		if (error==0.0){
			this.withError = 0.0000000001;
		}else {
			this.withError = error;
		}
		this.withNoise = noise;
		this.gaussianMean = 0.0;
		this.gaussianVariance = 1.0;

//TODO::change one-dimensional to multidimensional Gaussian distribution
		myErrorDistribution = new NormalDistribution(0.0,withError);
		filePath = System.getProperty("user.dir") + "/dataSet-files/";
	}

	public void generateLabeledData(int dataPoints) {
		if (withDrift) {
			dataSet = generateLabeledDataWithDrift(dataPoints);
		} else {
			dataSet = generateLabeledDataWithoutDrift(dataPoints);
		}

//		for (int j=0;j<dataSet.size();j++){
//			System.out.println(dataSet.get(j).toString());
//		}
		dataSetToDataFile(dataSet);
	}

	private void dataSetToDataFile(List<Tuple> dataSet) {
		FileParser fp = new FileParser();
		fp.dataSetToTextFile(dataSet,filePath+"syntheticData1.txt");
//		fp.dataSetToCSVFile(dataSet,"exampleCSV.csv");
		fp.writeCSV(dataSet, filePath+"exampleCSV_1.csv");
	}

	private List<Tuple> generateLabeledDataWithoutDrift(int dataPoints){
		NormalDistribution myDataDistribution = new NormalDistribution(gaussianMean, gaussianVariance);
		EnumeratedIntegerDistribution noiseDistribution = null;
		List<Tuple> data = new ArrayList<Tuple>();
		Double x_temp;
		int y_temp;

		if (withNoise){
			int[] numbers = {1, -1};
			double[] probabilities = {0.8,0.2};
			noiseDistribution = new EnumeratedIntegerDistribution(numbers,probabilities);
		}

		for (int i = 0; i < dataPoints; i++) {
			x_temp = myDataDistribution.sample();
			double point = Math.sin(x_temp) + 2 * x_temp + myErrorDistribution.sample();
			if (point >= 2 * x_temp) {
				y_temp = 1;
			} else {
				y_temp = -1;
			}
			if (withNoise){
				data.add(new Tuple2(x_temp,y_temp*noiseDistribution.sample()));
			}else{
				data.add(new Tuple2(x_temp,y_temp));
			}
		}
		return data;
	}

	private List<Tuple> generateLabeledDataWithDrift(int dataPoints) {
		NormalDistribution myDataDistribution = new NormalDistribution(gaussianMean, gaussianVariance);
		EnumeratedIntegerDistribution noiseDistribution = null;

		int[] direction = {1, -1};
		double[] directionProbabilities = {0.9,0.1};
		EnumeratedIntegerDistribution driftDirection = new EnumeratedIntegerDistribution(direction,directionProbabilities);

		List<Tuple> data = new ArrayList<Tuple>();
		Double x_temp;
		int y_temp;
		int w1 = 1;
		int w2 = 2;

		//adding noise by allowing the class value to change or not with a given probability
		if (withNoise){
			int[] numbers = {1, -1};
			double[] probabilities = {0.8,0.2};
			noiseDistribution = new EnumeratedIntegerDistribution(numbers,probabilities);
		}
		//pattern function: y = w1*sin(x) + w2*x + error
		//evolve distribution:
		// 1. p(x) evolves by varying it's distribution center "gaussianMean"
		// 2. p(y/x) evolves, by changes w2 = w2 + 1 (with a probability)
		int numberOfPoints = dataPoints/intervals_Of_Drift;
		for (int k=0;k<intervals_Of_Drift;k++){
			for (int i = 0; i < numberOfPoints; i++) {
				x_temp = Precision.round(myDataDistribution.sample(), 4);
				double point = w1*Math.sin(x_temp) + w2*x_temp + myErrorDistribution.sample();
				if (point >= w2 * x_temp) {
					y_temp = 1;
				} else {
					y_temp = -1;
				}
				if (withNoise){
					data.add(new Tuple2(x_temp,y_temp*noiseDistribution.sample()));
				}else{
					data.add(new Tuple2(x_temp,y_temp));
				}
			}
			// drift of data direction
			double newMean = gaussianMean+(1^driftDirection.sample())*0.1;
			myDataDistribution = new NormalDistribution(newMean,gaussianVariance);
		}
		return data;
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

	public List<Tuple> getDataSet() {
		return dataSet;
	}

	public static void main(String[] args) {
		SyntheticDataGenerator t = new SyntheticDataGenerator(true,0.1);
		t.generateLabeledData(1000);
	}
}
