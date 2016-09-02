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

package com.flytxt.yesbank.mapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.flytxt.yesbank.db.connection.DBConnection;
import com.flytxt.yesbank.db.model.TagInfoBean;

public class HdfsEngineMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private String delimiter = ",";

	private String modelId;

	private DBConnection dbConnection;

	private Connection connection;

	private Statement statement;

	private Map<Integer, TagInfoBean> tagInfoMap;

	private TagInfoBean tagInfoBean = null;

	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		String argParamValues = conf.get("test");

		modelId = argParamValues;

		System.out.println(" Arg Parameter Values ######### Model ID #####  " + argParamValues);

		dbConnection = DBConnection.getInstance();
		connection = dbConnection.getConnection();
		try {
			tagInfoMap = dbConnection.getAllTagsInfo(modelId);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] hdfsDataArray = value.toString().split(",");

		for (String val : hdfsDataArray) {
			System.out.println(" hdfs data values   :" + val);
		}

		List<TagInfoBean> hbaseStoreTagValueList = new ArrayList<>();

		StringTokenizer itr = new StringTokenizer(value.toString(), ",");

		Configuration conf = context.getConfiguration();

		System.out.println("no of tokens  " + itr.countTokens());

		for (int i = 0; i < itr.countTokens(); i++) {

			System.out.println(" next token values " + itr.nextToken());

		}

		String customerIdAsRowkey = null;

		for (int ii = 0; ii < hdfsDataArray.length; ii++) {

			if (ii == 0) {
				customerIdAsRowkey = hdfsDataArray[ii];
				continue;
			}

			if (tagInfoMap.containsKey(ii)) {
				tagInfoBean = tagInfoMap.get(ii);

				tagInfoBean.setTagNameValue_hdfs(hdfsDataArray[ii]);
				tagInfoBean.setCustomerIdRowKey_hfds(customerIdAsRowkey);
				hbaseStoreTagValueList.add(tagInfoBean);

			} else {
				System.out.format(" Tag Info  Header key is not avaiable for the column %i%n", ii);
			}
		}

		// Create / update the hbase database.
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

		for (TagInfoBean tagInfo : hbaseStoreTagValueList) {

			HTable hTable = new HTable(hbaseConfig, tagInfo.getTableName());

			Put p = new Put(Bytes.toBytes(tagInfo.getCustomerIdRowKey_hfds()));

			p.add(Bytes.toBytes(tagInfo.getColumnFamily()), Bytes.toBytes(tagInfo.getTagName()),
					Bytes.toBytes(tagInfo.getTagNameValue_hdfs()));
			hTable.put(p);
			System.out.println("Hbase data inserted successfully ");
		}
	}
}
