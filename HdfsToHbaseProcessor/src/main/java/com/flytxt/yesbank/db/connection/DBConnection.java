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

package com.flytxt.yesbank.db.connection;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.flytxt.yesbank.db.model.TagInfoBean;
import com.flytxt.yesbank.utils.CONSTANTS;

public class DBConnection {

	private Class _class = null;
	private Connection _connection = null;
	private Statement stmt = null;
	private ResultSet rs = null;

	private String JDBC_DRIVER = null;
	private String DB_URL = null;
	private String USERNAME = null;
	private String PASSWORD = null;

	private static DBConnection dbConnectionInstance;

	private Map<Integer, TagInfoBean> tagHeaderInfoCacheMap = new HashMap<>();

	public static DBConnection getInstance() {
		if (dbConnectionInstance == null) {
			dbConnectionInstance = new DBConnection();
		}

		return dbConnectionInstance;
	}

	public Connection getConnection() {
		return _connection;
	}

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
					"Data Base Connection Details :%n JDBC DRIVER : %s%n   DBURL  :  %s%n  USER NAME   : %s%n  PASSWORD  : %s%n ",
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

	public String getDelimiter(String modelId) throws Exception {

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

	public Map<Integer, TagInfoBean> getAllTagsInfo(String modelId) throws SQLException {

		String queryAllTags = CONSTANTS.QUERY_ALL_TAGS_INFO + modelId + CONSTANTS.QUERY_CLOSING_TERMINTATOR;

		Statement st = _connection.createStatement();
		ResultSet rs = st.executeQuery(queryAllTags);

		while (rs.next()) {
			// Retrieve by column name
			int tag_id = rs.getInt("tag_id");
			String table_name = rs.getString("tbl_name");
			String col_family = rs.getString("col_family");
			String tag_name = rs.getString("tag_name");
			String db_type = rs.getString("db_type");
			String default_value = rs.getString("default_value");
			String tag_type = rs.getString("tag_type");
			int model_id = rs.getInt("model_id");

			int header = rs.getInt("header");

			TagInfoBean tagInfoBean = new TagInfoBean(tag_id, table_name, col_family, tag_name, db_type, default_value,
					tag_type, model_id, header);

			tagHeaderInfoCacheMap.put(header, tagInfoBean);

			// Display values
			System.out.println("Tag Info Bean ##########" + tagInfoBean.toString());

		}
		return tagHeaderInfoCacheMap;
	}

	public Map<Integer, TagInfoBean> getTagHeaderInfoCache() {
		return tagHeaderInfoCacheMap;
	}
}
