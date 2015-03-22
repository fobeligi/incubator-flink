package org.apache.flink.streaming.examples.triggeredLambda.helpers;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.flink.api.java.tuple.Tuple;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileParser {

	private static String COMMA_DELIMITER = ",";
	private static String NEW_LINE_SEPARATOR = "\n";
	private static String TEXT_FILE_DELIMITER = "\t";
	public FileParser(){}

	public void dataSetToTextFile(List<Tuple> dataSet, String filename){

		try {
			File file = new File(filename);
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			for (int i=0;i<dataSet.size();i++){
				for (int j=0;j<dataSet.get(i).getArity();j++){
					output.write(dataSet.get(i).getField(j).toString());
					output.write(TEXT_FILE_DELIMITER);
				}
				output.write(NEW_LINE_SEPARATOR);
			}
			output.close();
		} catch ( IOException e ) {
			e.printStackTrace();
		}
	}

	public void dataSetToCSVFile(List<Tuple> ds, String filename){
		FileWriter fw = null;
		CSVPrinter cp = null;
		CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator('\n');

		try{
			fw = new FileWriter(filename);
			cp = new CSVPrinter(fw,format);
			for (Tuple dataPoint : ds){
				List data = new ArrayList();
				for (int j=0;j<dataPoint.getArity();j++){
					data.add(dataPoint.getField(0).toString());
				}
				cp.printRecord(data);
			}
		}catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}finally {
			try{

				fw.close();
				cp.close();
			}catch (IOException e){
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	public void writeCSV(List<Tuple> ds, String filename){
		FileWriter fw = null;

		try{
			fw = new FileWriter(filename);
			for (int j=0;j<ds.size();j++){
				for (int k=0;k<ds.get(j).getArity();k++){
					fw.append(ds.get(j).getField(k).toString());
					//don't place the comma delimiter at the end of each line
					if (!(k==ds.get(j).getArity()-1)){
						fw.append(COMMA_DELIMITER);
					}
				}
				fw.append(NEW_LINE_SEPARATOR);
			}
		}catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}finally {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
