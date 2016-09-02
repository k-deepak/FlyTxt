/*Copyright (c) 02-Sep-2016 FlyTxt Corporation and others.
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html

Contributors:
    Deepak  K- initial API and implementation
    
Author : Deepak K
date : 02-Sep-2016

*/

package com.flytxt.yesbank.processor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.flytxt.yesbank.db.connection.DBConnection;
import com.flytxt.yesbank.mapper.HdfsEngineMapper;

public class HdfsToHbaseEngine {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Hdfs to Hbase Engine requires the model Id as Input ...");
			System.exit(1);
		}

		String modelId = args[0];

		DBConnection dbConnection = DBConnection.getInstance();
		dbConnection.loadDbProperties();
		dbConnection.initializeDataBaseConnection();

		String hfdsInputLoc = dbConnection.getHdfsInputDirectory(modelId);

		if (hfdsInputLoc != null) {

			Configuration conf = new Configuration();
			String params = args[0];
			conf.set("test", params);
			Job job = new Job(conf);

			// Configuration conf = new Configuration();
			// Job job = Job.getInstance(conf, "hfds to hbase Engine");
			job.setJarByClass(HdfsToHbaseEngine.class);
			job.setMapperClass(HdfsEngineMapper.class);
			// job.setCombinerClass(test.class);
			// job.setReducerClass(test.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(hfdsInputLoc));
			job.setOutputFormatClass(NullOutputFormat.class);
			// FileOutputFormat.setOutputPath(job, new Path("/output_dir"));
			// conf.set("argParamValue", args[2]);
			// System.out.println("second argument value ------" + args[0]);
			// System.out.println(" in main method conf.getStrings --------" +
			// conf.get("argParamValue"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else {
			System.out.println(" Hdfs Input Location deos not exists .. Unable to process the Request ....");
			System.exit(0);
		}
	}

}
