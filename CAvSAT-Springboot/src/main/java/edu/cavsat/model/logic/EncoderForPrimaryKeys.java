/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.logic;

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

import edu.cavsat.model.bean.Atom;
import edu.cavsat.model.bean.Clause;
import edu.cavsat.model.bean.Query;
import edu.cavsat.model.bean.Relation;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.util.CAvSATSQLQueries;
import edu.cavsat.util.Constants;

public class EncoderForPrimaryKeys {

	private Schema schema;
	private CAvSATSQLQueries sqlQueriesImpl;
	private Connection con;
	private BufferedWriter br;
	private int varIndex = 1;
	private Map<Integer, Integer> factIDBoolVarMap;
	private String filename;

	public EncoderForPrimaryKeys(Schema schema, Connection con, String formulaFileName, CAvSATSQLQueries SQLQueriesImpl)
			throws IOException {
		super();
		this.schema = schema;
		this.con = con;
		this.filename = formulaFileName;
		this.sqlQueriesImpl = SQLQueriesImpl;
		this.factIDBoolVarMap = new HashMap<Integer, Integer>();
		this.br = new BufferedWriter(new FileWriter(formulaFileName));
	}

	public void openBr() {
		try {
			this.br = new BufferedWriter(new FileWriter(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createAlphaClauses(Query query) throws IOException, SQLException {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		for (Relation r : schema.getRelationsByNames(query.getParticipatingRelationNames())) {
			String csvKeyAttributes = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
			psKeyEqualGroups = con.prepareStatement(
					sqlQueriesImpl.getAlphaClausesQuery(Constants.CAvSAT_TBL_PREFIX + r.getName(), csvKeyAttributes));
			ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
			String curValue = "", receivedValue = "";
			while (rsKeyEqualGroups.next()) {
				Integer xVar = factIDBoolVarMap.get(rsKeyEqualGroups.getInt(2));
				if (xVar == null) {
					xVar = varIndex;
					factIDBoolVarMap.put(rsKeyEqualGroups.getInt(2), xVar);
					varIndex++;
				}
				receivedValue = rsKeyEqualGroups.getString(1);
				if (!receivedValue.equals(curValue)) {
					if (null != clause) {
						clause.setDescription("Alpha");
						br.append(clause.getDimacsLine());
						// clauseCount++;
					}
					clause = new Clause();
					clause.addVar(xVar);
					curValue = receivedValue;
				} else {
					clause.addVar(xVar);
				}
			}
			if (null != clause) {
				clause.setDescription("Alpha");
				br.append(clause.getDimacsLine());
				// clauseCount++;
			}
		}
	}

	public void createAlphaClausesFast(Query query) throws IOException, SQLException {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		factIDBoolVarMap.clear();
		for (Relation r : schema.getRelationsByNames(query.getParticipatingRelationNames())) {
			String csvKeyAttributes = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
			String alphaClausesQuery = sqlQueriesImpl
					.getAlphaClausesQuery(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + r.getName(), csvKeyAttributes);
			psKeyEqualGroups = con.prepareStatement(alphaClausesQuery);
			ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
			String curValue = "", receivedValue = "";
			while (rsKeyEqualGroups.next()) {
				Integer xVar = factIDBoolVarMap.get(rsKeyEqualGroups.getInt(Constants.CAvSAT_FACTID_COLUMN_NAME));
				if (xVar == null) {
					xVar = varIndex;
					factIDBoolVarMap.put(rsKeyEqualGroups.getInt(Constants.CAvSAT_FACTID_COLUMN_NAME), xVar);
					varIndex++;
				}
				receivedValue = "";
				for (int i = 1; i < rsKeyEqualGroups.getMetaData().getColumnCount(); i++) {
					receivedValue += rsKeyEqualGroups.getString(i);
				}
				if (!receivedValue.equals(curValue)) {
					if (null != clause) {
						clause.setDescription("Alpha");
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
				clause.setDescription("Alpha");
				br.append(clause.getDimacsLine());
			}
		}
	}

	public void createBetaClausesOpt(Query query) throws SQLException, IOException {
		List<String> selectAttributes = new ArrayList<String>();
		Set<String> fromTables = new HashSet<String>();
		Set<String> whereConditions = new HashSet<String>();

		for (Atom atom : query.getAtoms()) {
			fromTables.add(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + atom.getName());
		}
		for (String var : query.getFreeVars()) {
			String attribute = query.getAttributeFromVar(schema, null, var, -1);
			selectAttributes.add(
					Constants.CAvSAT_RELEVANT_TABLE_PREFIX + attribute + " AS " + attribute.replaceAll("\\.", "_"));
		}

		// Forming join conditions
		Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
		for (Atom atom : query.getAtoms()) {
			Relation relation = schema.getRelationByName(atom.getName());
			for (int i = 0; i < atom.getVars().size(); i++) {
				String var = atom.getVars().get(i);
				if (varAttrMap.containsKey(var)) {
					varAttrMap.get(var).add(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relation.getName() + "."
							+ relation.getAttributes().get(i));
				} else {
					List<String> list = new ArrayList<String>();
					list.add(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relation.getName() + "."
							+ relation.getAttributes().get(i));
					varAttrMap.put(var, list);
				}
			}
		}
		for (String var : varAttrMap.keySet()) {
			if (varAttrMap.get(var).size() > 1) {
				String first = varAttrMap.get(var).get(0);
				for (int i = 1; i < varAttrMap.get(var).size(); i++) {
					whereConditions.add(first + "=" + varAttrMap.get(var).get(i));
				}
			}
		}
		System.out.println(varAttrMap);
		System.out.println(whereConditions);
		// Create table with distinct potential answers and add pVars to them
		con.prepareStatement(
				sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
				.execute();
		String distinctPotentialAnswers = sqlQueriesImpl.getDistinctPotentialAnswersQuery(selectAttributes, fromTables,
				Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME, whereConditions);
		System.out.println("From init\n" + distinctPotentialAnswers);
		con.prepareStatement(distinctPotentialAnswers).execute();
		con.prepareStatement("ALTER TABLE " + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ " ADD CAVSAT_PVAR INT IDENTITY(" + varIndex + ",1) PRIMARY KEY").execute();
		con.prepareStatement("ALTER TABLE " + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ " ADD CAVSAT_IS_CONSISTENT INT").execute();
		con.prepareStatement("UPDATE " + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME
				+ " SET CAVSAT_IS_CONSISTENT = 1").executeUpdate();

		for (Atom atom : query.getAtoms()) {
			selectAttributes.add(
					Constants.CAvSAT_RELEVANT_TABLE_PREFIX + atom.getName() + "." + Constants.CAvSAT_FACTID_COLUMN_NAME
							+ " AS " + atom.getName() + "_" + Constants.CAvSAT_FACTID_COLUMN_NAME);
		}
		// Create witnesses with factIDs
		con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME))
				.execute();
		String witnessesWithFactIDs = sqlQueriesImpl.getDistinctPotentialAnswersQuery(selectAttributes, fromTables,
				Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME, whereConditions);
		con.prepareStatement(witnessesWithFactIDs).execute();

		String selectQuery;
		selectAttributes.clear();
		for (Atom atom : query.getAtoms()) {
			selectAttributes.add(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME + "." + atom.getName() + "_"
					+ Constants.CAvSAT_FACTID_COLUMN_NAME + " AS " + atom.getName() + "_"
					+ Constants.CAvSAT_FACTID_COLUMN_NAME);
		}
		selectAttributes
				.add(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + ".CAVSAT_PVAR AS CAVSAT_PVAR");
		selectQuery = "SELECT " + selectAttributes.stream().collect(Collectors.joining(","));
		selectQuery += " FROM " + Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME + ","
				+ Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME;
		whereConditions.clear();
		for (String var : query.getFreeVars()) {
			String attribute = query.getAttributeFromVar(schema, null, var, -1);
			whereConditions
					.add(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME + "." + attribute.replaceAll("\\.", "_")
							+ "=" + Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + "."
							+ attribute.replaceAll("\\.", "_"));
		}
		selectQuery += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));

		// Get witnesses factIDs joined with their pVars to form beta clauses
		ResultSet rs = con.prepareStatement(selectQuery).executeQuery();
		Set<Integer> addedPVars = new HashSet<Integer>();
		while (rs.next()) {
			Clause beta = new Clause();
			int pVar = rs.getInt("CAVSAT_PVAR");
			beta.addVar(-1 * pVar);
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				int factID = rs.getInt(i);
				if (factID != pVar)
					beta.addVar(factID);
			}
			beta.setDescription("Beta");
			br.append(beta.getDimacsLine());
			// clauseCount++;

			if (!addedPVars.contains(pVar)) {
				addedPVars.add(pVar);
				Clause soft = new Clause();
				soft.addVar(pVar);
				soft.setDescription("Soft unit clause for pVar");
				br.append(soft.getDimacsLine());
				// clauseCount++;
			}
		}
		br.close();
	}

	public void writeFinalFormulaFile(String formulaFileName) {
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
				if (s.contains("pVar")) {
					wr.append("1 " + s + "\n");
				} else {
					wr.append(infinity + " " + s + "\n");
				}
			}
			wr.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
