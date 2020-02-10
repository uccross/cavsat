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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beans.Atom;
import com.beans.Clause;
import com.beans.DenialConstraint;
import com.beans.Expression;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class Encoder2 {
	private List<Query> uCQ;
	private Schema schema;
	private Connection con;
	private BufferedWriter br;
	private int varIndex = 0;
	private int clauseCount = 0;

	public Encoder2(Schema schema, List<Query> uCQ, Connection con, String formulaFileName) {
		super();
		this.uCQ = uCQ;
		this.schema = schema;
		this.con = con;
		try {
			this.br = new BufferedWriter(new FileWriter(formulaFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(args[0]);
		schema.print();
		Connection con = new DBEnvironment().getConnection();
		List<Query> uCQ = pp.parseUCQ(args[1]);
		Encoder2 encoder = new Encoder2(schema, uCQ, con, "dc-formula.txt");
		encoder.createAlphaClausesInMemory();
		encoder.createBetaClausesInMemory();
		encoder.createGammaClausesInMemory();
		encoder.createThetaClausesInMemory();
		try {
			encoder.br.close(); // Don't forget
		} catch (IOException e) {
			e.printStackTrace();
		}
		encoder.writeFinalFormulaFile("dc-formula.txt");

		AnswersComputer computer = new AnswersComputer(con);
		try {
			computer.eliminatePotentialAnswers2("dc-formula.txt", encoder.clauseCount + 1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int getClauseCount() {
		return clauseCount;
	}

	public void closeConnections() {
		try {
			br.close(); // Don't forget
		} catch (IOException e) {
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
			System.out.println((varIndex - 1) + " " + clauseCount);
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

	/**
	 * This method fills the tables Near_violations and yVars. These tables are
	 * necessary to compute beta and gamma clauses, and theta expressions. Hence
	 * this method must be called first.
	 */
	private Map<Integer, Integer> factIDBoolVarMap = new HashMap<Integer, Integer>();
	private Map<Set<Integer>, Integer> nearViolationYVarMap = new HashMap<Set<Integer>, Integer>();
	private Map<Integer, Set<Integer>> xYVarsMap = new HashMap<Integer, Set<Integer>>();

	public void createAlphaClausesInMemory() {
		varIndex = 1;
		Set<Integer> minViolation = new HashSet<Integer>();
		PreparedStatement psSelect = null;
		try {
			ResultSet rs = con.prepareStatement(getAllFactIDsSQL()).executeQuery();
			while (rs.next()) {
				factIDBoolVarMap.put(rs.getInt(1), varIndex);
				xYVarsMap.put(varIndex, new HashSet<Integer>());
				varIndex++;
			}
			System.out.println("Total Facts: " + (varIndex - 1));
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		for (DenialConstraint dc : schema.getConstraints()) {
			String sqlQuery = getSQLQueryFromDC(dc);
			try {
				psSelect = con.prepareStatement(sqlQuery);
				ResultSet rs = psSelect.executeQuery();
				while (rs.next()) {
					minViolation.clear();
					Clause clause = new Clause();
					int xVar;
					for (int i = 0; i < dc.getAtoms().size(); i++) {
						xVar = factIDBoolVarMap.get(rs.getInt(i + 1));
						minViolation.add(xVar);
						clause.addVar(-1 * xVar);
					}
					clause.setDescription("V");
					br.write(clause.getDimacsLine()); // Alpha clauses done
					clauseCount++;
					if (minViolation.size() > 1) {
						Set<Integer> temp = new HashSet<Integer>(minViolation);
						Integer yVar;
						for (int fact : minViolation) {
							temp.remove(fact);
							if (temp.size() == 1) {
								// No need for extra y-variable
								yVar = temp.iterator().next();
								nearViolationYVarMap.put(new HashSet<Integer>(Arrays.asList(temp.iterator().next())),
										temp.iterator().next());
							} else {
								yVar = nearViolationYVarMap.get(temp);
								if (null == yVar) {
									yVar = varIndex;
									nearViolationYVarMap.put(new HashSet<Integer>(temp), yVar);
									varIndex++;
								}
							}
							xYVarsMap.get(fact).add(yVar);
							temp.add(fact);
						}
					}
				}
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void createAlphaClauses() {
		Set<Integer> minViolation = new HashSet<Integer>();
		PreparedStatement psUpdate = null, psSelect = null, psInsert = null, psSelectFromYVars = null;
		int totalFacts = 0;

		try {
			con.prepareStatement("DROP TABLE IF EXISTS NEAR_VIOLATIONS").execute();
			con.prepareStatement("CREATE TABLE NEAR_VIOLATIONS (FactID int, yVars text)").execute();
			con.prepareStatement("DROP TABLE IF EXISTS YVARS").execute();
			con.prepareStatement("CREATE TABLE YVARS (NearViolation text, yvar int)").execute();
			totalFacts = con
					.prepareStatement(
							"INSERT INTO NEAR_VIOLATIONS (FactID) (" + getAllFactIDsSQL() + " ORDER BY FactID)")
					.executeUpdate();
			psInsert = con.prepareStatement(
					"INSERT INTO YVARS SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM YVARS WHERE NearViolation = ?)");
			psSelectFromYVars = con.prepareStatement("SELECT yVar FROM YVARS WHERE NearViolation = ?");
			psUpdate = con.prepareStatement("UPDATE NEAR_VIOLATIONS SET yVars = CONCAT(yVars,?) WHERE FactID = ?");
			System.out.println("Total Facts: " + totalFacts);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		varIndex = totalFacts + 1;
		for (DenialConstraint dc : schema.getConstraints()) {
			String sqlQuery = getSQLQueryFromDC(dc);
			try {
				psSelect = con.prepareStatement(sqlQuery);
				System.out.println(sqlQuery);
				ResultSet rs = psSelect.executeQuery();
				while (rs.next()) {
					minViolation.clear();
					Clause clause = new Clause();
					for (int i = 0; i < dc.getAtoms().size(); i++) {
						minViolation.add(rs.getInt(i + 1));
						clause.addVar(-1 * rs.getInt(i + 1));
					}
					clause.setDescription("V");
					br.write(clause.getDimacsLine()); // Alpha clauses done
					clauseCount++;
					if (minViolation.size() > 1) {
						Set<Integer> temp = new HashSet<Integer>(minViolation);
						int yVar;
						for (int fact : minViolation) {
							temp.remove(fact);
							if (temp.size() == 1) {
								// No need for extra y-variable
								yVar = temp.iterator().next();
							} else {
								psInsert.setString(1, temp.toString());
								psInsert.setInt(2, varIndex);
								psInsert.setString(3, temp.toString());

								if (psInsert.executeUpdate() == 0) {
									psSelectFromYVars.setString(1, temp.toString());
									ResultSet rsSelectFromYVars = psSelectFromYVars.executeQuery();
									rsSelectFromYVars.next();
									yVar = rsSelectFromYVars.getInt(1);
									rsSelectFromYVars.close();
								} else {
									yVar = varIndex;
									varIndex++;
								}
							}
							psUpdate.setString(1, ";" + yVar);
							psUpdate.setInt(2, fact);
							temp.add(fact);
							psUpdate.addBatch();
						}
					}
				}
				psUpdate.executeBatch();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void createBetaClausesInMemory() {
		Map<String, Integer> potentialAnswers = new HashMap<String, Integer>();
		try {
			String sqlQuery = "", prefix = "";
			Integer pVar;
			for (Query query : uCQ) {
				sqlQuery += prefix + getSQLQueryFromCQ(query);
				prefix = " UNION ";
			}
			ResultSet rs = con.prepareStatement(sqlQuery).executeQuery();
			System.out.println(sqlQuery);
			while (rs.next()) {
				pVar = potentialAnswers.get(rs.getString(2));
				if (null == pVar) {
					pVar = varIndex;
					potentialAnswers.put(rs.getString(2), pVar);
					varIndex++;
				}

				Clause clause = new Clause();
				clause.addVar(-1 * pVar);
				String witnessString = rs.getString(1);
				for (int fact : parseWitnessString(witnessString)) {
					clause.addVar(-1 * factIDBoolVarMap.get(fact));
				}
				clause.setDescription("B");
				br.append(clause.getDimacsLine());
				clauseCount++;

				clause = new Clause();
				clause.addVar(pVar);
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
				clauseCount++;
			}
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

	public void createBetaClauses() {
		PreparedStatement psInsert = null, psSelectPotentialAnswer;
		try {
			con.prepareStatement("DROP TABLE IF EXISTS Potential_Answers").execute();
			con.prepareStatement("CREATE TABLE Potential_Answers (Answer text, pVar int)").execute();
			psInsert = con.prepareStatement(
					"INSERT INTO Potential_Answers SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM Potential_Answers WHERE Answer = ?)");
			psSelectPotentialAnswer = con.prepareStatement("SELECT pVar FROM Potential_Answers WHERE Answer = ?");
			String sqlQuery = "", prefix = "";
			int pVar;
			for (Query query : uCQ) {
				sqlQuery += prefix + getSQLQueryFromCQ(query);
				prefix = " UNION ";
			}
			ResultSet rs = con.prepareStatement(sqlQuery).executeQuery();
			while (rs.next()) {
				psInsert.setString(1, rs.getString(2));
				psInsert.setInt(2, varIndex);
				psInsert.setString(3, rs.getString(2));
				if (psInsert.executeUpdate() == 0) {
					psSelectPotentialAnswer.setString(1, rs.getString(2));
					ResultSet rsSelectPotentialAnswer = psSelectPotentialAnswer.executeQuery();
					rsSelectPotentialAnswer.next();
					pVar = rsSelectPotentialAnswer.getInt(1);
					rsSelectPotentialAnswer.close();
				} else {
					pVar = varIndex;
					varIndex++;
				}
				Clause clause = new Clause();
				clause.addVar(-1 * pVar);
				String witnessString = rs.getString(1);
				for (int fact : parseWitnessString(witnessString)) {
					clause.addVar(-1 * fact);
				}
				clause.setDescription("B");
				br.append(clause.getDimacsLine());
				clauseCount++;

				clause = new Clause();
				clause.addVar(pVar);
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
				clauseCount++;
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public void createGammaClausesInMemory() {
		try {
			for (int x : xYVarsMap.keySet()) {
				Clause clause = new Clause();
				clause.addVar(x);
				for (int y : xYVarsMap.get(x)) {
					clause.addVar(y);
				}
				clause.setDescription("G");
				br.append(clause.getDimacsLine());
				clauseCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createGammaClauses() {
		try {
			ResultSet rs = con.prepareStatement("SELECT FactID, yVars FROM Near_Violations").executeQuery();
			Clause clause = null;
			while (rs.next()) {
				int factID = rs.getInt(1);
				Set<Integer> yVars = parseYVarsString(rs.getString(2));
				clause = new Clause();
				clause.addVar(factID);
				for (int yVar : yVars)
					clause.addVar(yVar);
				clause.setDescription("G");
				br.append(clause.getDimacsLine());
				clauseCount++;
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public void createThetaClausesInMemory() {
		int yVar;
		try {
			for (Set<Integer> nv : nearViolationYVarMap.keySet()) {
				yVar = nearViolationYVarMap.get(nv);
				Clause thetaLong = new Clause();
				thetaLong.addVar(yVar);
				for (int x : nv) {
					Clause thetaShort = new Clause();
					thetaShort.addVar(-1 * yVar);
					thetaShort.addVar(x);
					thetaShort.setDescription("T");
					br.append(thetaShort.getDimacsLine());
					clauseCount++;
					thetaLong.addVar(-1 * x);
				}
				thetaLong.setDescription("T");
				br.append(thetaLong.getDimacsLine());
				clauseCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createThetaClauses() {
		try {
			ResultSet rs = con.prepareStatement("SELECT NearViolation, yVar FROM yVars").executeQuery();
			Clause theta1 = null, theta2 = null;
			while (rs.next()) {
				Set<Integer> nearViolation = parseNearViolationString(rs.getString(1));
				int yVar = rs.getInt(2);
				theta2 = new Clause();
				theta2.addVar(yVar);
				theta2.setDescription("T");
				for (int x : nearViolation) {
					theta1 = new Clause();
					theta1.addVar(-1 * yVar);
					theta1.addVar(x);
					theta1.setDescription("T");
					br.append(theta1.getDimacsLine());
					clauseCount++;
					theta2.addVar(-1 * x);
				}
				br.append(theta2.getDimacsLine());
				clauseCount++;
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	private Set<Integer> parseYVarsString(String s) {
		Set<Integer> yVars = new HashSet<Integer>();
		if (s == null)
			return yVars;
		for (String str : s.split(";"))
			if (!str.isEmpty())
				yVars.add(Integer.parseInt(str));
		return yVars;
	}

	private Set<Integer> parseNearViolationString(String nv) {
		Set<Integer> nearViolations = new HashSet<Integer>();
		if (nv == null)
			return nearViolations;
		for (String str : nv.substring(1, nv.length() - 1).split(",")) {
			// Substring removes square bracket characters '[' and ']'
			nearViolations.add(Integer.parseInt(str.replaceAll("\\s+", "")));
		}
		return nearViolations;
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

	public void createMinimalWitnesses() {
		Set<Integer> minWitness = new HashSet<Integer>();
		Set<String> witnesses = new HashSet<String>();
		PreparedStatement psInsert = null, psSelect;
		try {
			con.prepareStatement("DROP TABLE IF EXISTS MIN_WITNESSES CASCADE").execute();
			con.prepareStatement("CREATE TABLE MIN_WITNESSES (WITNESS text)").execute();
			psInsert = con.prepareStatement("INSERT INTO MIN_WITNESSES VALUES (?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for (Query q : uCQ) {
			String sqlQuery = getSQLQueryFromCQ(q);
			try {
				psSelect = con.prepareStatement(sqlQuery);
				ResultSet rs = psSelect.executeQuery();
				while (rs.next()) {
					minWitness.clear();
					for (int i = 0; i < q.getAtoms().size(); i++) {
						minWitness.add(rs.getInt(i + 1));
					}
					witnesses.add(minWitness.toString());
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			System.out.println();
		}
		try {
			for (String w : witnesses) {
				psInsert.setString(1, w);
				psInsert.addBatch();
			}
			psInsert.executeBatch();
		} catch (SQLException e) {

		}
	}

	private String getSQLQueryFromCQ(Query query) {
		return getSQLQuery(query.getAtoms(), new ArrayList<Expression>(), query.getFreeVars(), true);
	}

	private String getSQLQueryFromDC(DenialConstraint dc) {
		return getSQLQuery(dc.getAtoms(), dc.getExpressions(), new ArrayList<String>(), false);
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

	private String getAllFactIDsSQL() {
		Map<String, String> q = new HashMap<String, String>();
		for (Relation r : schema.getRelations()) {
			q.put(r.getName(), "SELECT FactID from " + r.getName());
		}
		String sqlQuery = "";
		String prefix = "";
		for (String key : q.keySet()) {
			sqlQuery = sqlQuery + prefix + q.get(key);
			prefix = " UNION ";
		}
		return sqlQuery;
	}
}
