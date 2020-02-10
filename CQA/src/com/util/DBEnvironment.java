/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBEnvironment {
	private static String PostgresURL = "jdbc:postgresql://localhost/cqa1";
	private static String PostgresUsername = "postgres";
	private static String PostgresPassword = "postgres123";

	private static String MySQLURL = "jdbc:mysql://localhost:3306/cqa";
	private static String MySQLUsername = "root";
	private static String MySQLPassword = "mysql123";

	private static String AWSSQLServerURL = "jdbc:sqlserver://rds-cqa-sqlserver.cnoykvezfq6g.us-east-2.rds.amazonaws.com:1433;databaseName=kw-rewriting-experiment";
	private static String AWSSQLServerUsername = "akadixit";
	private static String AWSSQLServerPassword = "sqlserver123";

	private static String LocalSQLServerURL = "jdbc:sqlserver://DESKTOP-T2DFRH7:1433;databaseName=kw-rewriting-experiment";
	private static String LocalSQLServerUsername = "akadixit";
	private static String LocalSQLServerPassword = "sqlserver123";

	public Connection getConnection() {
		try {
			//return getLocalSQLServerConnection();
			 return getPostgresConnection();
			// return getMySQLConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Connection getMySQLConnection() throws SQLException {
		return DriverManager.getConnection(MySQLURL, MySQLUsername, MySQLPassword);
	}

	public Connection getAWSSQLServerConnection() throws SQLException {
		return DriverManager.getConnection(AWSSQLServerURL, AWSSQLServerUsername, AWSSQLServerPassword);
	}

	public Connection getLocalSQLServerConnection() throws SQLException {
		return DriverManager.getConnection(LocalSQLServerURL, LocalSQLServerUsername, LocalSQLServerPassword);
	}

	public Connection getPostgresConnection() throws SQLException {
		return DriverManager.getConnection(PostgresURL, PostgresUsername, PostgresPassword);
	}
}
