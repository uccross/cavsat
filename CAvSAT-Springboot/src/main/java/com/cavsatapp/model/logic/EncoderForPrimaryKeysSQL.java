/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cavsatapp.model.bean.Clause;
import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.SQLQuery;
import com.cavsatapp.model.bean.Schema;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;

/**
 * @author Akhil
 *
 */
public class EncoderForPrimaryKeysSQL {
	private Schema schema;
	private CAvSATSQLQueries sqlQueriesImpl;
	private Connection con;
	private BufferedWriter br;
	private int varIndex = 1;
	private Map<Integer, Integer> factIDBoolVarMap;

	public EncoderForPrimaryKeysSQL(Schema schema, Connection con, String formulaFileName,
			CAvSATSQLQueries SQLQueriesImpl) throws IOException {
		super();
		this.schema = schema;
		this.con = con;
		this.sqlQueriesImpl = SQLQueriesImpl;
		this.factIDBoolVarMap = new HashMap<Integer, Integer>();
		this.br = new BufferedWriter(new FileWriter(formulaFileName));
	}

	public void createAlphaClausesOpt(SQLQuery query) throws IOException, SQLException {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		factIDBoolVarMap.clear();
		Relation r = null;
		for (String relationName : query.getFrom()) {
			r = schema.getRelationByName(relationName);
			String csvKeyAttributes = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
			String alphaClausesQuery = sqlQueriesImpl
					.getAlphaClausesQuery(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + r.getName(), csvKeyAttributes);
			psKeyEqualGroups = con.prepareStatement(alphaClausesQuery);
			ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
			String curValue = "", receivedValue = "";
			int factID;
			while (rsKeyEqualGroups.next()) {
				factID = rsKeyEqualGroups.getInt(Constants.CAvSAT_FACTID_COLUMN_NAME);
				Integer xVar = factIDBoolVarMap.getOrDefault(factID, null);
				if (xVar == null) {
					xVar = varIndex;
					factIDBoolVarMap.put(factID, xVar);
					varIndex++;
				}
				receivedValue = "";
				for (int i = 1; i < rsKeyEqualGroups.getMetaData().getColumnCount(); i++) {
					receivedValue += rsKeyEqualGroups.getString(i);
				}
				if (!receivedValue.equals(curValue)) {
					if (null != clause) {
						clause.setDescription("A");
						br.append(clause.getDimacsLine());
					}
					clause = new Clause();
					clause.addVar(xVar);
					curValue = receivedValue;
				} else {
					clause.addVar(xVar);
				}
			}
			if (null != clause) {
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
			}
		}
	}

	public void createAlphaClausesUnOpt(SQLQuery query) throws IOException, SQLException {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		Relation r = null;
		for (String relationName : query.getFrom()) {
			r = schema.getRelationByName(relationName);
			String csvKeyAttributes = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
			String alphaClausesQuery = sqlQueriesImpl.getAlphaClausesUnOptQuery(r.getName(), csvKeyAttributes);
			psKeyEqualGroups = con.prepareStatement(alphaClausesQuery);
			ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
			String curValue = "", receivedValue = "";
			int factID;
			while (rsKeyEqualGroups.next()) {
				factID = rsKeyEqualGroups.getInt(Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME);
				Integer xVar = factIDBoolVarMap.getOrDefault(factID, null);
				if (xVar == null) {
					xVar = varIndex;
					factIDBoolVarMap.put(factID, xVar);
					varIndex++;
				}
				receivedValue = "";
				for (int i = 1; i < rsKeyEqualGroups.getMetaData().getColumnCount(); i++) {
					receivedValue += rsKeyEqualGroups.getString(i);
				}
				if (!receivedValue.equals(curValue)) {
					if (null != clause) {
						clause.setDescription("A");
						br.append(clause.getDimacsLine());
					}
					clause = new Clause();
					clause.addVar(xVar);
					curValue = receivedValue;
				} else {
					clause.addVar(xVar);
				}
			}
			if (null != clause) {
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
			}
		}
	}

	public void createBetaClausesOpt(SQLQuery query) throws SQLException, IOException {
		SQLQuery betaQuery = new SQLQuery(query);
		betaQuery.setFrom(
				query.getFrom().stream().map(relationName -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName)
						.collect(Collectors.toList()));
		betaQuery.setSelect(query.getSelect().stream().map(attribute -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX
				+ attribute + " AS " + attribute.replaceAll("\\.", "_")).collect(Collectors.toList()));
		betaQuery.setSelectDistinct(true);

		List<String> newConditions = new ArrayList<String>();
		String newCondition;
		for (String condition : betaQuery.getWhereConditions()) {
			newCondition = condition;
			for (String relationName : query.getFrom())
				newCondition = newCondition.replaceAll("(?i)" + relationName + "\\.",
						Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName + "\\.");
			newConditions.add(newCondition);
		}
		betaQuery.setWhereConditions(newConditions);
		// Create table with distinct potential answers and add pVars to them
		// con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
		// .execute();
		// System.out.println(betaQuery.getSQLSyntax(Constants.CAvSAT_DISTINCT_POTENTIAL_ANS_TABLE_NAME));
		con.prepareStatement(betaQuery.getSQLSyntax(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
				.execute();
		con.prepareStatement("ALTER TABLE " + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ " ADD CAVSAT_PVAR INT IDENTITY(" + varIndex + ",1) PRIMARY KEY").execute();
		// Create witnesses with factIDs
		betaQuery.getSelect()
				.addAll(query.getFrom().stream()
						.map(relationName -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName + "."
								+ Constants.CAvSAT_FACTID_COLUMN_NAME + " AS " + relationName + "_"
								+ Constants.CAvSAT_FACTID_COLUMN_NAME)
						.collect(Collectors.toList()));
		// con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME))
		// .execute();
		con.prepareStatement(betaQuery.getSQLSyntax(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME)).execute();
		betaQuery.setSelect(query.getFrom().stream()
				.map(relationName -> Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME + "." + relationName + "_"
						+ Constants.CAvSAT_FACTID_COLUMN_NAME + " AS " + relationName + "_"
						+ Constants.CAvSAT_FACTID_COLUMN_NAME)
				.collect(Collectors.toList()));
		betaQuery.getSelect()
				.add(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + ".CAVSAT_PVAR AS CAVSAT_PVAR");
		betaQuery.getFrom().clear();
		betaQuery.getFrom().add(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME);
		betaQuery.getFrom().add(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME);
		betaQuery.setWhereConditions(new ArrayList<String>(query.getSelect().stream()
				.map(attribute -> Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME + "."
						+ attribute.replaceAll("\\.", "_") + "="
						+ Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + "."
						+ attribute.replaceAll("\\.", "_"))
				.collect(Collectors.toList())));
		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		Set<Integer> addedPVars = new HashSet<Integer>();
		while (rs.next()) {
			Clause beta = new Clause();
			int pVar = rs.getInt("CAVSAT_PVAR");
			beta.addVar(-1 * pVar);
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				int factID = rs.getInt(i);
				if (factID != pVar)
					beta.addVar(-1 * factID);
			}
			beta.setDescription("B");
			br.append(beta.getDimacsLine());

			if (!addedPVars.contains(pVar)) {
				addedPVars.add(pVar);
				Clause soft = new Clause();
				soft.addVar(pVar);
				soft.setDescription("S");
				br.append(soft.getDimacsLine());
			}
		}
		br.close();
	}

	public void createBetaClausesUnOpt(SQLQuery query) throws SQLException, IOException {
		SQLQuery betaQuery = new SQLQuery(query);
		betaQuery.setSelect(query.getSelect().stream()
				.map(attribute -> attribute + " AS " + attribute.replaceAll("\\.", "_")).collect(Collectors.toList()));
		betaQuery.setSelectDistinct(true);

		con.prepareStatement(betaQuery.getSQLSyntax(Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
				.execute();
		con.prepareStatement("ALTER TABLE " + Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ " ADD CAVSAT_PVAR INT IDENTITY(" + varIndex + ",1) PRIMARY KEY").execute();

		// Create witnesses with factIDs
		betaQuery.getSelect()
				.addAll(query.getFrom().stream()
						.map(relationName -> relationName + "." + Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME + " AS "
								+ relationName + "_" + Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME)
						.collect(Collectors.toList()));
		con.prepareStatement(betaQuery.getSQLSyntax(Constants.CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME)).execute();
		betaQuery.setSelect(query.getFrom().stream()
				.map(relationName -> Constants.CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME + "." + relationName + "_"
						+ Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME + " AS " + relationName + "_"
						+ Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME)
				.collect(Collectors.toList()));
		betaQuery.getSelect()
				.add(Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + ".CAVSAT_PVAR AS CAVSAT_PVAR");
		betaQuery.getFrom().clear();
		betaQuery.getFrom().add(Constants.CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME);
		betaQuery.getFrom().add(Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME);
		betaQuery.setWhereConditions(new ArrayList<String>(query.getSelect().stream()
				.map(attribute -> Constants.CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME + "."
						+ attribute.replaceAll("\\.", "_") + "="
						+ Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + "."
						+ attribute.replaceAll("\\.", "_"))
				.collect(Collectors.toList())));
		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		Set<Integer> addedPVars = new HashSet<Integer>();
		while (rs.next()) {
			Clause beta = new Clause();
			int pVar = rs.getInt("CAVSAT_PVAR");
			beta.addVar(-1 * pVar);
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				int factID = rs.getInt(i);
				if (factID != pVar)
					beta.addVar(-1 * factID);
			}
			beta.setDescription("B");
			br.append(beta.getDimacsLine());

			if (!addedPVars.contains(pVar)) {
				addedPVars.add(pVar);
				Clause soft = new Clause();
				soft.addVar(pVar);
				soft.setDescription("S");
				br.append(soft.getDimacsLine());
			}
		}
		br.close();
	}

	public String writeFinalFormulaFile(String formulaFileName) {
		Set<String> clauses = new HashSet<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(formulaFileName));
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				clauses.add(sCurrentLine);
			}
			br.close();
			BufferedWriter wr = new BufferedWriter(new FileWriter(formulaFileName));
			String infinity = Integer.toString(clauses.size() + 1);
			String firstLine = "p wcnf " + varIndex + " " + clauses.size() + " " + infinity + "\n";
			wr.write(firstLine);
			// this.clauseCount = clauses.size();
			for (String s : clauses) {
				if (s.contains("S")) {
					wr.append("1 " + s + "\n");
				} else {
					wr.append(infinity + " " + s + "\n");
				}
			}
			wr.close();
			return infinity;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return "";
		}
	}
}
