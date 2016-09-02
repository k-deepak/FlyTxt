package com.flytxt.yesbank.test;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

/* Author : Deepak K
27-Aug-2016 6:16:14 PM 
fly
*/

public class TestReadConfigFile {
	public static void main(String[] args) {

		Properties prop = new Properties();
		OutputStream output = null;

		try {

			output = new FileOutputStream("dbConfig.properties");

			// set the properties value
			prop.setProperty("database", "localhost");
			prop.setProperty("dbuser", "Deepak");
			prop.setProperty("dbpassword", "password");

			// save properties to project root folder
			prop.store(output, null);
			

		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}
}
