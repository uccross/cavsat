/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.core;

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

import com.beans.Atom;
import com.beans.Clause;
import com.beans.Expression;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class Encoder4 {

	private Schema schema;
	private Connection con;
	private BufferedWriter br;
	private int varIndex = 1;
	private int clauseCount = 0;
	private Map<Integer, Integer> factIDBoolVarMap;
	private String filename;

	public Encoder4(Schema schema, Connection con, String formulaFileName) {
		super();
		this.schema = schema;
		this.con = con;
		this.filename = formulaFileName;
		this.factIDBoolVarMap = new HashMap<Integer, Integer>();
		try {
			this.br = new BufferedWriter(new FileWriter(formulaFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getClauseCount() {
		return clauseCount;
	}

	public void openBr() {
		try {
			this.br = new BufferedWriter(new FileWriter(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyschema1.txt");
		// schema.print();
		List<Query> uCQ = pp.parseUCQ(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt");
		Encoder4 encoder = new Encoder4(schema, new DBEnvironment().getConnection(),
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\formula.txt");
		encoder.createAlphaClausesFaster(uCQ.get(0));
		try {
			encoder.br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// encoder.writeFinalFormulaFile(
		// "C:\\Users\\Akhil\\OneDrive -
		// ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\formula.txt");
	}

	public void createAlphaClausesFaster(Query query) {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		try {
			long time = System.currentTimeMillis();
			for (Relation r : schema.getRelationsByNames(query.getParticipatingRelationNames())) {
				System.out.println("Started");
				String s = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
				String q = "SELECT " + s + ", STRING_AGG(cast (FactID as text), cast (? as text)) AS LIST FROM " + r.getName() + " GROUP BY " + s;
				System.out.println(q);
				psKeyEqualGroups = con.prepareStatement(q);
				psKeyEqualGroups.setString(1, ",");
				ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
				System.out.println("Got resultset");
				while (rsKeyEqualGroups.next()) {
					String keyGroup = rsKeyEqualGroups.getString("LIST");
					clause = new Clause();
					for (String factid : keyGroup.split(",")) {
						clause.addVar(Integer.parseInt(factid));
					}
					clause.setDescription("K");
					br.append(clause.getDimacsLine());
				}
				System.out.println("Iterated resultset");
			}
			System.out.println("Time: " + (System.currentTimeMillis() - time));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public void createAlphaClauses(Query query) {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		try {
			for (Relation r : schema.getRelationsByNames(query.getParticipatingRelationNames())) {
				String s = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
				String q = "SELECT (" + s + "), FactID FROM " + r.getName() + " ORDER BY " + s;
				psKeyEqualGroups = con.prepareStatement(q);
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
							clause.setDescription("K");
							br.append(clause.getDimacsLine());
							clauseCount++;
						}
						clause = new Clause();
						clause.addVar(xVar);
						curValue = receivedValue;
					} else {
						clause.addVar(xVar);
					}
				}
				if (null != clause) {
					clause.setDescription("K");
					br.append(clause.getDimacsLine());
					clauseCount++;
				}
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public void createBetaClausesFaster(Query query, int totalFacts) {
		varIndex = totalFacts + 1;
		String sqlQuery = getSQLQuery(query.getAtoms(), new ArrayList<Expression>(), query.getFreeVars(), true);
		Map<String, Integer> potentialAnswers = new HashMap<String, Integer>();
		try {
			ResultSet rsSelect = con.prepareStatement(sqlQuery).executeQuery();
			Integer pVar = null;
			while (rsSelect.next()) {
				pVar = potentialAnswers.get(rsSelect.getString(2));
				if (null == pVar) {
					pVar = varIndex;
					potentialAnswers.put(rsSelect.getString(2), pVar);
					varIndex++;

					Clause soft = new Clause();
					soft.addVar(pVar);
					soft.setDescription("A");
					br.append(soft.getDimacsLine());
					clauseCount++;
				}
				Clause beta = new Clause();
				beta.addVar(-1 * pVar);
				for (int fact : parseWitnessString(rsSelect.getString(1))) {
					beta.addVar(-1 * fact);
				}
				beta.setDescription("B");
				br.append(beta.getDimacsLine());
				clauseCount++;
			}
			br.close();
			con.prepareStatement("DROP TABLE IF EXISTS POTENTIAL_ANSWERS").execute();
			con.prepareStatement("CREATE TABLE POTENTIAL_ANSWERS (Answer TEXT, pVar INT)").execute();
			PreparedStatement psInsert = con.prepareStatement("INSERT INTO POTENTIAL_ANSWERS VALUES (?, ?)");
			for (String answer : potentialAnswers.keySet()) {
				psInsert.setString(1, answer);
				psInsert.setInt(2, potentialAnswers.get(answer));
				psInsert.addBatch();
			}
			psInsert.executeBatch();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public void createBetaClauses(Query query) {
		String sqlQuery = getSQLQuery(query.getAtoms(), new ArrayList<Expression>(), query.getFreeVars(), true);
		Map<String, Integer> potentialAnswers = new HashMap<String, Integer>();
		try {
			ResultSet rsSelect = con.prepareStatement(sqlQuery).executeQuery();
			Integer pVar = null;
			while (rsSelect.next()) {
				pVar = potentialAnswers.get(rsSelect.getString(2));
				if (null == pVar) {
					pVar = varIndex;
					potentialAnswers.put(rsSelect.getString(2), pVar);
					varIndex++;

					Clause soft = new Clause();
					soft.addVar(pVar);
					soft.setDescription("A");
					br.append(soft.getDimacsLine());
					clauseCount++;
				}
				Clause beta = new Clause();
				beta.addVar(-1 * pVar);
				for (int fact : parseWitnessString(rsSelect.getString(1))) {
					beta.addVar(-1 * fact);
				}
				beta.setDescription("B");
				br.append(beta.getDimacsLine());
				clauseCount++;
			}
			br.close();
			con.prepareStatement("DROP TABLE IF EXISTS POTENTIAL_ANSWERS").execute();
			con.prepareStatement("CREATE TABLE POTENTIAL_ANSWERS (Answer TEXT, pVar INT)").execute();
			PreparedStatement psInsert = con.prepareStatement("INSERT INTO POTENTIAL_ANSWERS VALUES (?, ?)");
			for (String answer : potentialAnswers.keySet()) {
				psInsert.setString(1, answer);
				psInsert.setInt(2, potentialAnswers.get(answer));
				psInsert.addBatch();
			}
			psInsert.executeBatch();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
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
			String firstLine = "p wcnf " + (varIndex - 1) + " " + clauses.size() + "\n";
			wr.write(firstLine);
			String infinity = Integer.toString(clauses.size() + 1);
			this.clauseCount = clauses.size();
			System.out.println((varIndex - 1) + " vars, " + clauseCount + " clauses");
			for (String s : clauses) {
				if (s.contains("A")) {
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

	private int getTotalFacts() {
		String sqlQuery = "SELECT MAX(FactID) FROM (", prefix = "";
		for (Relation r : schema.getRelations()) {
			sqlQuery = sqlQuery + prefix + "SELECT MAX(FactID) AS FactID FROM " + r.getName();
			prefix = " UNION ";
		}
		sqlQuery += ") AS ID";
		ResultSet rs = null;
		try {
			rs = con.prepareStatement(sqlQuery).executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private Set<Integer> parseWitnessString(String s) {
		Set<Integer> witnesses = new HashSet<Integer>();
		if (s == null)
			return witnesses;
		for (String str : s.replaceAll("\\(", "").replaceAll("\\)", "").split(",")) {
			witnesses.add(Integer.parseInt(str));
		}
		return witnesses;
	}

	private String getSQLQuery(List<Atom> atomList, List<Expression> expressionList, List<String> freeVars,
			boolean factIDAsOneGroup) {
		String selectClause = "SELECT ", fromClause = " FROM ", whereclause = " WHERE ", prefix = "", andPrefix = "";
		Map<String, String> varAttrMap = new HashMap<String, String>();
		int i = 0;// i acts as an atom index, and used as an alias
		if (factIDAsOneGroup)
			selectClause += "CAST ((";
		for (Atom atom : atomList) {
			selectClause += prefix + atom.getName() + "_" + i + ".FactID";
			fromClause += prefix + atom.getName() + " " + atom.getName() + "_" + i;
			prefix = ", ";
			int j = 0;
			List<String> attributes = schema.getRelationByName(atom.getName()).getAttributes();
			for (String var : atom.getVars()) {
				if (atom.getConstants().contains(var)) {
					whereclause += andPrefix + atom.getName() + "_" + i + "." + attributes.get(j) + "=" + "'" + var
							+ "'";
					andPrefix = " AND ";
				}
				if (varAttrMap.keySet().contains(var)) {
					whereclause += andPrefix + varAttrMap.get(var) + "=" + atom.getName() + "_" + i + "."
							+ attributes.get(j);
					andPrefix = " AND ";
				} else {
					varAttrMap.put(var, atom.getName() + "_" + i + "." + attributes.get(j));
				}
				j++;
			}
			i++;
		}
		if (factIDAsOneGroup)
			selectClause += ") AS TEXT)";
		for (String var : freeVars) {
			selectClause += prefix + varAttrMap.get(var);
		}
		for (Expression exp : expressionList) {
			String left = "", right = "";
			if (exp.getVar1().startsWith("'") && exp.getVar1().endsWith("'")) {
				left = exp.getVar1();
			} else {
				left = varAttrMap.get(exp.getVar1());
			}

			if (exp.getVar2().startsWith("'") && exp.getVar2().endsWith("'")) {
				right = exp.getVar2();
			} else {
				right = varAttrMap.get(exp.getVar2());
			}
			whereclause += andPrefix + left + exp.getOp() + right;
		}
		return selectClause + fromClause + whereclause;
	}
	/*
	 * private String getQuestionMarksCSV(int howmany) { String str = "", prefix =
	 * ""; for (int i = 0; i < howmany; i++) { str += prefix + "?"; prefix = ","; }
	 * return str; }
	 */
}
