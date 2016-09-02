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

package com.flytxt.yesbank.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.flytxt.yesbank.utils.CONSTANTS;

public class ModelProcessor {
	/*
	 * static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; static final
	 * String DB_URL = "jdbc:mysql://localhost:3306/test";
	 * 
	 * // Database credentials static final String USER = "root"; static final
	 * String PASS = "root";
	 */
	private Class _class = null;
	private Connection _connection = null;
	private Statement stmt = null;
	private ResultSet rs = null;

	private String JDBC_DRIVER = null;
	private String DB_URL = null;
	private String USERNAME = null;
	private String PASSWORD = null;

	public void loadDbProperties() throws IOException {
		InputStream inputStream = null;
		try {
			Properties prop = new Properties();
			String propFileName = CONSTANTS.DB_PROPERTY_FILE;
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			inputStream = classLoader.getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			// get the property value
			JDBC_DRIVER = prop.getProperty("JDBC_DRIVER");
			DB_URL = prop.getProperty("DB_URL");
			USERNAME = prop.getProperty("USER");
			PASSWORD = prop.getProperty("PASS");

			System.out.format(
					"Data Base Connection Details : JDBC DRIVER : %s  , DBURL  :  %s , USER NAME   : %s , PASSWORD  : %s ",
					JDBC_DRIVER, DB_URL, USERNAME, PASSWORD);
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		}
	}

	public void initializeDataBaseConnection() throws ClassNotFoundException, SQLException {
		_class = Class.forName(JDBC_DRIVER);
		_connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
	}

	public String getHdfsInputDirectory(String modelId) throws Exception {
		String hdfsInputLocation = null;
		String queryForhdfsIpDir = CONSTANTS.QUERY_HDFS_INPUT_LOCATION + modelId + CONSTANTS.QUERY_CLOSING_TERMINTATOR;

		Statement st = _connection.createStatement();
		ResultSet rs = st.executeQuery(queryForhdfsIpDir);

		while (rs.next()) {
			hdfsInputLocation = rs.getString("hdfs_input_loc");
		}
		System.out.format(Locale.ENGLISH, "For the Model Id : " + "%s,  the hfds input location  is  : %s, ", modelId,
				hdfsInputLocation);

		if (hdfsInputLocation != null) {
			return hdfsInputLocation;
		}
		return null;
	}

	private String getDelimiter(String modelId) throws Exception {

		String inputDataSplitter = null;

		String queryDataSplitter = CONSTANTS.QUERY_DATA_SPLITTER + modelId + CONSTANTS.QUERY_CLOSING_TERMINTATOR;

		Statement st = _connection.createStatement();

		ResultSet rs = st.executeQuery(queryDataSplitter);

		while (rs.next()) {
			inputDataSplitter = rs.getString("data_splitter");
		}
		System.out.format(Locale.ENGLISH, "For the Model Id : " + "%s,  the data spliitter  is  : %s, ", modelId,
				inputDataSplitter);

		if (inputDataSplitter != null) {
			return inputDataSplitter;
		}

		return null;
	}

	public static class HdfsProcessMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String delimiter = ",";

		private String JDBC_DRIVER;
		private String DB_URL;
		private String USER;
		private String PASS;

		private Class _class;
		private Connection _connection;
		private Statement stmt;
		private ResultSet rs;

		private String modelId;

		public void loadDbProperties() throws IOException {
			InputStream inputStream = null;
			try {
				Properties prop = new Properties();
				String propFileName = "dbConfig.properties";
				ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
				inputStream = classLoader.getResourceAsStream("dbConfig.properties");

				if (inputStream != null) {
					prop.load(inputStream);
				} else {
					throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
				}

				// get the property value and print it out
				JDBC_DRIVER = prop.getProperty("JDBC_DRIVER");
				DB_URL = prop.getProperty("DB_URL");
				USER = prop.getProperty("USER");
				PASS = prop.getProperty("PASS");

				System.out.println("######## DATA BASE DRIVER DETAILS ######### " + JDBC_DRIVER + ", " + DB_URL + ", "
						+ USER + ", " + PASS);
			} catch (Exception e) {
				System.out.println("Exception: " + e);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {

			loadDbProperties();
			System.out.println(" ############ CONNECT TO SQL DATA BASE ########  ");
			try {
				_class = Class.forName(JDBC_DRIVER);
				_connection = DriverManager.getConnection(DB_URL, USER, PASS);
			} catch (ClassNotFoundException | SQLException e) {
				e.printStackTrace();
			}
			System.out.println(" ########### CONNECTION SUCCESSFUL TO SQL DATA BASE ");

			Configuration conf = context.getConfiguration();

			String argParamValues = conf.get("test");
			// String[] argParamsSplit = argParamValues.split(",");

			modelId = argParamValues;
			System.out.println(" Arg Parameter Values #########  " + argParamValues);

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// To do get delimiter from sql table.

			String[] hdfsDataArray = value.toString().split(",");

			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			Configuration conf = context.getConfiguration();
			System.out.println("no of tokens  " + itr.countTokens());
			for (int i = 0; i < itr.countTokens(); i++) {
				System.out.println(" next token values " + itr.nextToken());
			}

			String custId_rowKey = hdfsDataArray[0];
			String tag_hl = hdfsDataArray[1];
			String tag_cl = hdfsDataArray[2];
			String tag_pl = hdfsDataArray[3];

			// read tag_info from sql table.

			String sql = "select * from tag_info where model_id=" + modelId + ";";
			System.out.println(" Generated Query ======" + sql);
			try {
				stmt = _connection.createStatement();
				rs = stmt.executeQuery(sql);

				while (rs.next()) {
					// Retrieve by column name
					String table_name = rs.getString("tbl_name");
					String col_family = rs.getString("col_family");
					String tag_name = rs.getString("tag_name");

					// Display values
					System.out.println("table name   : " + table_name);
					System.out.println(" column family : " + col_family);
					System.out.println(" tag name   " + tag_name);

					// Retrieve tag info from sql table.

					// Create / update the hbase database.
					Configuration hbaseConfig = HBaseConfiguration.create();

					HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
					// Instantiating HTable class
					HTable hTable = new HTable(hbaseConfig, table_name);

					// Instantiating Put class
					// accepts a row name.
					Put p = new Put(Bytes.toBytes(custId_rowKey));

					// adding values using add() method
					// accepts column family name, qualifier/row name ,value
					if (tag_name.equals("home_loan"))
						p.add(Bytes.toBytes(col_family), Bytes.toBytes(tag_name), Bytes.toBytes(tag_hl));
					if (tag_name.equals("car_loan"))
						p.add(Bytes.toBytes(col_family), Bytes.toBytes(tag_name), Bytes.toBytes(tag_cl));

					if (tag_name.equals("personal_loan"))
						p.add(Bytes.toBytes(col_family), Bytes.toBytes(tag_name), Bytes.toBytes(tag_pl));

					// Saving the put Instance to the HTable.
					hTable.put(p);
					System.out.println("Hbase data inserted successfully ");

				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/*
	 * public static class IntSumReducer extends Reducer<Text, IntWritable,
	 * Text, IntWritable> { private IntWritable result = new IntWritable();
	 * 
	 * public void reduce(Text key, Iterable<IntWritable> values, Context
	 * context) throws IOException, InterruptedException { int sum = 0; for
	 * (IntWritable val : values) { sum += val.get(); } result.set(sum);
	 * context.write(key, result); } }
	 */

	public static void main(String[] args) throws Exception {

		String modelId = args[0];

		ModelProcessor modelProcessor = new ModelProcessor();

		modelProcessor.loadDbProperties();

		modelProcessor.initializeDataBaseConnection();

		String hfdsInputLoc = modelProcessor.getHdfsInputDirectory(modelId);

		if (hfdsInputLoc != null) {

			Configuration conf = new Configuration();
			String params = args[0];
			conf.set("test", params);

			Job job = new Job(conf);

			// Configuration conf = new Configuration();
			// Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(ModelProcessor.class);
			job.setMapperClass(HdfsProcessMapper.class);
			// job.setCombinerClass(IntSumReducer.class);
			// job.setReducerClass(IntSumReducer.class);
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
