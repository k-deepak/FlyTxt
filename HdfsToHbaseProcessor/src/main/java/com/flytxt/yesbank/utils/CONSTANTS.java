package com.flytxt.yesbank.utils;

public class CONSTANTS {

	// ## SQL QUERIES

	public static String QUERY_HDFS_INPUT_LOCATION = "select * from tag_model_info where model_id=";

	public static String QUERY_DATA_SPLITTER = "select data_splitter from tag_model_info where model_id=";

	public static String QUERY_ALL_TAGS_INFO = "select * from tag_info where model_id=";

	public static String QUERY_CLOSING_TERMINTATOR = ";";

	// ### ProperTy File
	public static String DB_PROPERTY_FILE = "dbConfig.properties";

}
