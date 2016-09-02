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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;

public class TestReadConfig {
	String result = "";
	InputStream inputStream;

	public String getPropValues() throws IOException {

		try {
			Properties prop = new Properties();

			String propFileName = "dbConfig.properties";

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

			InputStream inputStream = classLoader.getResourceAsStream("dbConfig.properties");

			// inputStream = new FileInputStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			Date time = new Date(System.currentTimeMillis());

			// get the property value and print it out
			String user = prop.getProperty("JDBC_DRIVER");
			String company1 = prop.getProperty("DB_URL");
			String company2 = prop.getProperty("USER");
			String company3 = prop.getProperty("PASS");

			result = "Company List = " + company1 + ", " + company2;

			System.out.println(result + "\nProgram Ran on " + time + " by user=" + user);

		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
		}
		return result;
	}

	public static void main(String[] args) throws IOException {

		TestReadConfig testReadConfig = new TestReadConfig();
		testReadConfig.getPropValues();

	}
}