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
