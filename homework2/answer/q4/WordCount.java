/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
	public static class Mapper1 extends
	Mapper<Object, Text, IntWritable, Text> {

		private HashMap<Integer, ArrayList<Double>> centroids = new HashMap<Integer, ArrayList<Double>>();
		private static double cost;

		protected void setup(Context context) throws IOException, InterruptedException {
			cost = 0;
			String folder= context.getConfiguration().get("dir") + context.getConfiguration().get("input_dir");
			File folder_obj = new File(folder);
			File final_file = null;
			for (File file : folder_obj.listFiles()){
				if (file.getName().equals("centroids")) {
					final_file = file;
				}
			}
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(final_file));
				String tempString = null;
				Integer centroid_number = new Integer(0);
				while ((tempString = reader.readLine()) != null){
					String line = tempString;
					StringTokenizer itr = new StringTokenizer(line);
					
					ArrayList<Double> temp_centroid = new ArrayList<Double>();
					while(itr.hasMoreTokens()) {
						temp_centroid.add(new Double(itr.nextToken()));
					}
					centroids.put(centroid_number, temp_centroid);
					centroid_number = centroid_number + 1;
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//This is the user defined map function
		//which is invoked for each line in the input
		//the key is the file offset and the value is line
		//The Context class is used to interact with the Hadoop framework
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			ArrayList<Double> data_point = new ArrayList<Double>();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				data_point.add(new Double(itr.nextToken()));
			}

			int min_i = 0;
			Double min_dist = Double.MAX_VALUE;
			for (Integer i : centroids.keySet()) {
				Double dist = computeDistance(data_point, centroids.get(i));
				if (min_dist > dist) {
					min_i = i;
					min_dist = dist;
				}
			}
			cost = cost + min_dist;
			IntWritable centroid = new IntWritable(min_i);
			context.write(centroid, value);
		}
		
		private Double computeDistance(ArrayList<Double> d1, ArrayList<Double> d2) {
			Double sum = 0.0;
			for (int i = 0; i < d1.size(); i++) {
				sum = sum + (d1.get(i) - d2.get(i))*(d1.get(i) - d2.get(i));
			}
			return Math.sqrt(sum);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			String folder = context.getConfiguration().get("dir");
			String filename = folder + "cost";
			try {
				FileWriter fw = new FileWriter(filename, true);
				fw.write(cost + "\n");
				fw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	public static class Reducer1 extends
			Reducer<IntWritable, Text, Text, Text> {

		//This is the user defined reducer function
		//which is invoked for each unique key
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			ArrayList<ArrayList<Double>> data = new ArrayList<ArrayList<Double>>();
			for (Text t: values){
				StringTokenizer itr = new StringTokenizer(t.toString());
				ArrayList<Double> temp_data = new ArrayList<Double>();
				while (itr.hasMoreTokens()){
					temp_data.add(new Double(itr.nextToken()));
				}
				data.add(temp_data);
			}
			Double[] mean = new Double[58];
			for(int i = 0; i < 58; i++){
				Double sum = 0.0;
				for (int j = 0; j < data.size(); j++){
					sum = sum + data.get(j).get(i);
				}
				mean[i] = sum/data.size();
			}
			
			String line = "";
			for (int i = 0; i < 58; i++){
				line = line +  " " + String.valueOf(mean[i]);
			}
			
			String directory= context.getConfiguration().get("dir") + context.getConfiguration().get("output_dir");
			File folder = new File(directory + "centroids");
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(folder, true));
				bw.write(line + "\n");
				bw.close();
			} catch (IOException e){
				e.printStackTrace();
			}
			context.write(new Text("LOL"), new Text("lolmax"));
		}
	}

	public static void main(String[] args) throws Exception {
		
		//Load the configuration files and
		//add them to the the conf object
		for (int i = 1; i <= 20; i++) {
			Configuration conf = new Configuration();
			int next = i + 1;
			conf.set("dir", "/home/vm4learning/Documents/massive_data_mining/homework2/data/");
			conf.set("input_dir", "c" + i + "/");
			conf.set("output_dir", "c" + next + "/");
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
						
			//Create a job object and give the job a name
			//It allows the user to configure the job, submit it,
			//control its execution, and query the state.
			Job job = new Job(conf, "word count");
			
			//Specify the jar which contains the required classes
			//for the job to run.
			job.setJarByClass(WordCount.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(Mapper1.class);
			
//			job.setCombinerClass(Reducer1.class);
			job.setReducerClass(Reducer1.class);
			
			//Set the output key and the value class for the entire job
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(IntWritable.class);
			
			
			//Set the Input (format and location) and similarly for the output also
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/c" + next + "/"));
//			
//			job.setNumReduceTasks(1);
			
			//Submit the job and wait for it to complete
			job.waitForCompletion(true);
		}
	}
}