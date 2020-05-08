/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import com.cavsatapp.model.bean.SQLQuery;
import com.cavsatapp.model.bean.Stats;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;
import com.cavsatapp.util.ExecCommand;
import com.fasterxml.jackson.core.JsonProcessingException;

public class AnswersComputer {
	private Connection con;

	public AnswersComputer(Connection con) {
		super();
		this.con = con;
	}

	public Stats computeBooleanAnswer(String filename, String solvername) {
		ExecCommand command = new ExecCommand();
		if (solvername.equalsIgnoreCase("MaxHS")) {
			command.executeCommand(new String[] { "maxhs", filename }, Constants.SAT_OUTPUT_FILE_NAME);
		} else if (solvername.equalsIgnoreCase("Glucose")) {
			command.executeCommand(new String[] { "glucose", filename }, Constants.SAT_OUTPUT_FILE_NAME);
		} else if (solvername.equalsIgnoreCase("lingeling")) {
			command.executeCommand(new String[] { "lingeling", filename }, Constants.SAT_OUTPUT_FILE_NAME);
		}
		return command.isSAT(Constants.SAT_OUTPUT_FILE_NAME, solvername);
	}

	public void buildFinalAnswers(CAvSATSQLQueries sqlQueriesImpl) throws SQLException {
		// con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME)).execute();
		PreparedStatement ps = con
				.prepareStatement("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?");
		ps.setString(1, Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME);
		ResultSet rs = ps.executeQuery();
		List<String> columns = new ArrayList<String>();
		while (rs.next()) {
			String s = rs.getString(1);
			if (!s.toUpperCase().startsWith("CAVSAT"))
				columns.add(s);
		}
		rs.close();
		ps.close();
		con.prepareStatement(sqlQueriesImpl.getBuildFinalAnswers(columns)).execute();
	}

	public void buildFinalAnswersUnOpt(CAvSATSQLQueries sqlQueriesImpl) throws SQLException {
		// con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME)).execute();
		PreparedStatement ps = con
				.prepareStatement("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?");
		ps.setString(1, Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME);
		ResultSet rs = ps.executeQuery();
		List<String> columns = new ArrayList<String>();
		while (rs.next()) {
			String s = rs.getString(1);
			if (!s.toUpperCase().startsWith("CAVSAT"))
				columns.add(s);
		}
		rs.close();
		ps.close();

		con.prepareStatement("SELECT * INTO " + Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME + " FROM (SELECT "
				+ String.join(",", columns) + " FROM " + Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ ") a").execute();
	}

	public String computeSQLQueryAnswers(SQLQuery sqlQuery, String intoTable, CAvSATSQLQueries sqlQueriesImpl,
			int returnRowsLimit) throws SQLException, JsonProcessingException {
		con.prepareStatement(sqlQueriesImpl.getDropTableQuery(intoTable)).execute();
		if (!sqlQuery.isSelectDistinct()) {
			sqlQuery.setSelectDistinct(true);
			con.prepareStatement(sqlQuery.getSQLSyntax(intoTable)).execute();
			sqlQuery.setSelectDistinct(false);
		}
		return sqlQueriesImpl.getTablePreviewAsJSON(intoTable, con, returnRowsLimit);
	}

	public String computeSQLQueryAnswers(String sqlQuery, CAvSATSQLQueries sqlQueriesImpl, int returnRowsLimit)
			throws SQLException, JsonProcessingException {
		return sqlQueriesImpl.getQueryResultPreviewAsJSON(sqlQuery, con, returnRowsLimit);
	}

	public long eliminatePotentialAnswersInMemory(String filename, String infinity) throws SQLException {
		boolean moreAnswers = true;
		BufferedWriter wr = null;
		ResultSet rsSelect = con
				.prepareStatement(
						"SELECT CAVSAT_PVAR FROM " + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME)
				.executeQuery();
		Set<Integer> consistentAnswers = new HashSet<Integer>();
		while (rsSelect.next())
			consistentAnswers.add(rsSelect.getInt(1));
		String output = "";
		Set<Integer> assignment = new HashSet<Integer>();
		ExecCommand command = new ExecCommand();
		int i = 0;
		long start, time = 0;
		while (moreAnswers) {
			System.out.println("iteration " + (i++) + ", remaining potential answers: " + consistentAnswers.size());
			assignment.clear();
			moreAnswers = false;
			start = System.currentTimeMillis();
			command.executeCommand(new String[] { "maxhs", filename }, Constants.SAT_OUTPUT_FILE_NAME);
			time = time + System.currentTimeMillis() - start;
			try {
				copyFileUsingStream(new File(Constants.SAT_OUTPUT_FILE_NAME),
						new File(Constants.SAT_OUTPUT_FILE_NAME + i));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			output = command.readOutput(Constants.SAT_OUTPUT_FILE_NAME);
			StringTokenizer st = new StringTokenizer(output.substring(1), " ");
			while (st.hasMoreTokens())
				assignment.add(Integer.parseInt(st.nextToken()));
			Iterator<Integer> it = consistentAnswers.iterator();
			int answer;
			try {
				wr = new BufferedWriter(new FileWriter(filename, true));
				while (it.hasNext()) {
					answer = it.next();
					if (assignment.contains(answer)) {
						it.remove();
						wr.append(infinity + " -" + answer + " 0 c I\n");
						moreAnswers = true;
					}
				}
				wr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// con.prepareStatement("DROP TABLE IF EXISTS
		// CAVSAT_CONSISTENT_PVARS").execute();
		con.prepareStatement("CREATE TABLE CAVSAT_CONSISTENT_PVARS (CAVSAT_PVAR INT PRIMARY KEY)").execute();
		PreparedStatement psInsert = con.prepareStatement("INSERT INTO CAVSAT_CONSISTENT_PVARS VALUES (?)");
		for (int pvar : consistentAnswers) {
			psInsert.setInt(1, pvar);
			psInsert.addBatch();
		}
		psInsert.executeBatch();
		/*
		 * con.prepareStatement("DELETE FROM " +
		 * Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
		 * " WHERE NOT EXISTS (SELECT C.CAVSAT_PVAR FROM CAVSAT_CONSISTENT_PVARS C WHERE C.CAVSAT_PVAR = "
		 * + Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
		 * ".CAVSAT_PVAR)").execute(); System.out.println("finished deleting");
		 */
		return time;
	}

	public long eliminatePotentialAnswersUnOptInMemory(String filename, String infinity) throws SQLException {
		boolean moreAnswers = true;
		BufferedWriter wr = null;
		ResultSet rsSelect = con
				.prepareStatement("SELECT CAVSAT_PVAR FROM " + Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME)
				.executeQuery();
		Set<Integer> consistentAnswers = new HashSet<Integer>();
		while (rsSelect.next())
			consistentAnswers.add(rsSelect.getInt(1));
		String output = "";
		Set<Integer> assignment = new HashSet<Integer>();
		ExecCommand command = new ExecCommand();
		int i = 0;
		long start, time = 0;
		while (moreAnswers) {
			System.out.println("iteration " + (i++) + ", remaining potential answers: " + consistentAnswers.size());
			assignment.clear();
			moreAnswers = false;
			start = System.currentTimeMillis();
			command.executeCommand(new String[] { "maxhs", filename }, Constants.SAT_OUTPUT_FILE_NAME);
			time = time + System.currentTimeMillis() - start;
			try {
				copyFileUsingStream(new File(Constants.SAT_OUTPUT_FILE_NAME),
						new File(Constants.SAT_OUTPUT_FILE_NAME + i));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			output = command.readOutput(Constants.SAT_OUTPUT_FILE_NAME);
			StringTokenizer st = new StringTokenizer(output.substring(1), " ");
			while (st.hasMoreTokens())
				assignment.add(Integer.parseInt(st.nextToken()));
			Iterator<Integer> it = consistentAnswers.iterator();
			int answer;
			try {
				wr = new BufferedWriter(new FileWriter(filename, true));
				while (it.hasNext()) {
					answer = it.next();
					if (assignment.contains(answer)) {
						it.remove();
						wr.append(infinity + " -" + answer + " 0 c I\n");
						moreAnswers = true;
					}
				}
				wr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// con.prepareStatement("DROP TABLE IF EXISTS
		// CAVSAT_CONSISTENT_PVARS").execute();
		con.prepareStatement("CREATE TABLE CAVSAT_CONSISTENT_PVARS (CAVSAT_PVAR INT PRIMARY KEY)").execute();
		PreparedStatement psInsert = con.prepareStatement("INSERT INTO CAVSAT_CONSISTENT_PVARS VALUES (?)");
		for (int pvar : consistentAnswers) {
			psInsert.setInt(1, pvar);
			psInsert.addBatch();
		}
		psInsert.executeBatch();
		/*
		 * con.prepareStatement("DELETE FROM " +
		 * Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
		 * " WHERE NOT EXISTS (SELECT C.CAVSAT_PVAR FROM CAVSAT_CONSISTENT_PVARS C WHERE C.CAVSAT_PVAR = "
		 * + Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
		 * ".CAVSAT_PVAR)").execute(); System.out.println("finished deleting");
		 */
		return time;
	}

	private void copyFileUsingStream(File source, File dest) throws IOException {
		InputStream is = null;
		OutputStream os = null;
		try {
			is = new FileInputStream(source);
			os = new FileOutputStream(dest);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			}
		} finally {
			is.close();
			os.close();
		}
	}

	public int getRowCount(String tableName, CAvSATSQLQueries sqlQueriesImpl) throws SQLException {
		ResultSet rs = con.prepareStatement(sqlQueriesImpl.getNumberOfRows(tableName)).executeQuery();
		rs.next();
		return rs.getInt(1);
	}
}
