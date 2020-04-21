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
			command.executeCommand(new String[] { "./maxhs", filename }, Constants.SAT_OUTPUT_FILE_NAME);
		} else if (solvername.equalsIgnoreCase("Glucose")) {
			command.executeCommand(new String[] { "./glucose", filename }, Constants.SAT_OUTPUT_FILE_NAME);
		} else if (solvername.equalsIgnoreCase("lingeling")) {
			command.executeCommand(new String[] { "./lingeling", filename }, Constants.SAT_OUTPUT_FILE_NAME);
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

	/*
	 * public String computePotentialAnswersFromFOQuery(Schema schema, Query query,
	 * CAvSATSQLQueries sqlQueriesImpl) throws SQLException, JsonProcessingException
	 * { List<String> selectAttributes = new ArrayList<String>(); Set<String>
	 * fromTables = new HashSet<String>(); Set<String> whereConditions = new
	 * HashSet<String>();
	 * 
	 * for (Atom atom : query.getAtoms()) { fromTables.add(atom.getName()); } for
	 * (String var : query.getFreeVars()) { String attribute =
	 * query.getAttributeFromVar(schema, null, var, -1);
	 * selectAttributes.add(attribute); }
	 * 
	 * // Forming join conditions Map<String, List<String>> varAttrMap = new
	 * HashMap<String, List<String>>(); for (Atom atom : query.getAtoms()) {
	 * Relation relation = schema.getRelationByName(atom.getName()); for (int i = 0;
	 * i < atom.getVars().size(); i++) { String var = atom.getVars().get(i); if
	 * (varAttrMap.containsKey(var)) { varAttrMap.get(var).add(relation.getName() +
	 * "." + relation.getAttributes().get(i)); } else { List<String> list = new
	 * ArrayList<String>(); list.add(relation.getName() + "." +
	 * relation.getAttributes().get(i)); varAttrMap.put(var, list); } if
	 * (atom.getConstants().contains(var)) varAttrMap.get(var).add(var); } } for
	 * (String var : varAttrMap.keySet()) { if (varAttrMap.get(var).size() > 1) {
	 * String first = varAttrMap.get(var).get(0); for (int i = 1; i <
	 * varAttrMap.get(var).size(); i++) { whereConditions.add(first + "=" +
	 * varAttrMap.get(var).get(i)); } } } // Create table with distinct potential
	 * answers and add pVars to them //
	 * con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.
	 * CAvSAT_ALL_ANS_TABLE_NAME)).execute(); String allAnswers =
	 * sqlQueriesImpl.getDistinctPotentialAnswersQuery(selectAttributes, fromTables,
	 * Constants.CAvSAT_ALL_DISTINCT_POTENTIAL_ANS_TABLE_NAME, whereConditions);
	 * con.prepareStatement(allAnswers).execute(); return
	 * sqlQueriesImpl.getTablePreviewAsJSON(Constants.
	 * CAvSAT_ALL_DISTINCT_POTENTIAL_ANS_TABLE_NAME, con, 100); }
	 */

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

	/*
	 * public long eliminatePotentialAnswers(String filename, int infinity) throws
	 * SQLException { boolean moreAnswers = true; BufferedWriter wr = null;
	 * PreparedStatement psSelect = con.prepareStatement("SELECT CAVSAT_PVAR FROM "
	 * + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
	 * " WHERE CAVSAT_IS_CONSISTENT = 1"); PreparedStatement psUpdate = con
	 * .prepareStatement("UPDATE " +
	 * Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME +
	 * " SET CAVSAT_IS_CONSISTENT = 0 WHERE CAVSAT_PVAR = ?"); String output = "";
	 * Set<Integer> assignment = new HashSet<Integer>(); ResultSet
	 * rsPotentialAnswers = null; // int iterationCount = 0; // long time = 0, start
	 * = 0; ExecCommand command = new ExecCommand(); int i = 0; while (moreAnswers)
	 * { System.out.println("iteration " + (i++)); assignment.clear(); moreAnswers =
	 * false; // start = System.currentTimeMillis(); command.executeCommand(new
	 * String[] { "maxhs", filename }, Constants.SAT_OUTPUT_FILE_NAME); output =
	 * command.readOutput(Constants.SAT_OUTPUT_FILE_NAME); // iterationCount++; //
	 * time += (System.currentTimeMillis() - start); StringTokenizer st = new
	 * StringTokenizer(output.substring(1), " "); while (st.hasMoreTokens())
	 * assignment.add(Integer.parseInt(st.nextToken())); try { rsPotentialAnswers =
	 * psSelect.executeQuery(); wr = new BufferedWriter(new FileWriter(filename,
	 * true)); while (rsPotentialAnswers.next()) { if
	 * (assignment.contains(rsPotentialAnswers.getInt(1))) { wr.append(infinity +
	 * " -" + rsPotentialAnswers.getInt(1) + " 0 c I\n"); psUpdate.setInt(1,
	 * rsPotentialAnswers.getInt(1)); //
	 * System.out.println("Removed "+rsPotentialAnswers.getInt(1));
	 * psUpdate.addBatch(); moreAnswers = true; } } wr.close();
	 * System.out.println("starting batch");
	 * System.out.println(psUpdate.executeBatch().length);
	 * System.out.println("done with batch"); } catch (IOException e) {
	 * e.printStackTrace(); } } // System.out.println("MaxSAT Iterations: " +
	 * iterationCount); return -1; }
	 */

	public int getRowCount(String tableName, CAvSATSQLQueries sqlQueriesImpl) throws SQLException {
		ResultSet rs = con.prepareStatement(sqlQueriesImpl.getNumberOfRows(tableName)).executeQuery();
		rs.next();
		return rs.getInt(1);
	}
}
