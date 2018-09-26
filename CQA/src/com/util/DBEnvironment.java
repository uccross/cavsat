package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBEnvironment {
	private static String url = "jdbc:postgresql://localhost";
	private static String username = "postgres";
	private static String password = "postgres123";
	private static String dbname = "cqa1";

	public Connection getConnection() {
		return getConnection(url, dbname, username, password);
	}

	public Connection getConnection(String url, String dbname, String username, String password) {
		try {
			Properties props = new Properties();
			props.setProperty("user", username);
			props.setProperty("password", password);
			return DriverManager.getConnection(url + "/" + dbname, props);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
