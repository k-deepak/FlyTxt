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
