/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBEnvironment {
	private static String PostgresURL = "jdbc:postgresql://localhost/cqa1";
	private static String PostgresUsername = "postgres";
	private static String PostgresPassword = "postgres123";

	private static String MySQLURL = "jdbc:mysql://localhost:3306/cqa";
	private static String MySQLUsername = "root";
	private static String MySQLPassword = "mysql123";

	public Connection getConnection() {
		return getPostgresConnection(PostgresURL, PostgresUsername, PostgresPassword);
	}

	public Connection getMySQLConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			return DriverManager.getConnection(MySQLURL, MySQLUsername, MySQLPassword);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Connection getPostgresConnection(String url, String username, String password) {
		try {
			Properties props = new Properties();
			props.setProperty("user", username);
			props.setProperty("password", password);
			return DriverManager.getConnection(url, props);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
