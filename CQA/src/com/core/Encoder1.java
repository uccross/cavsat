/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.core;

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
import com.beans.CNFFormula;
import com.beans.Clause;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ExecCommand;

public class Encoder1 {
	private Query query;
	private Set<Relation> relations;
	private Connection con;
	private Map<List<String>, Integer> potentialAnswersMap;

	public Encoder1(Schema schema, Query query, Connection con) {
		super();
		this.query = query;
		this.relations = schema.getRelationsByNames(query.getParticipatingRelationNames());
		this.con = con;
	}

	public CNFFormula createNonBoolNegClauses(Schema schema, int noOfVariables) {
		CNFFormula formula = new CNFFormula();
		Connection con = new DBEnvironment().getConnection();
		potentialAnswersMap = new HashMap<List<String>, Integer>();

		String potentialAnswersQuery = getQueryToFindWitnesses(query, schema, false, true, false);
		PreparedStatement psWitnesses;
		try {
			psWitnesses = con.prepareStatement(potentialAnswersQuery);
			System.out.println("Finding witnesses..");
			ResultSet rsWitnesses = psWitnesses.executeQuery();
			System.out.println("Found witnesses");
			while (rsWitnesses.next()) {
				Clause clause = new Clause();
				for (int i = 1; i <= query.getAtoms().size(); i++) {
					clause.addVar(rsWitnesses.getInt(i) * (-1));
				}
				List<String> potentialAnswer = new ArrayList<String>();
				for (int varIndex : extractFreeVarColumnNames(potentialAnswersQuery)) {
					potentialAnswer.add(rsWitnesses.getString(varIndex));
				}
				if (!potentialAnswersMap.containsKey(potentialAnswer)) {
					potentialAnswersMap.put(potentialAnswer, noOfVariables + 1);
					clause.addVar(-1 * (noOfVariables + 1));
					noOfVariables++;
				} else {
					clause.addVar(-1 * potentialAnswersMap.get(potentialAnswer));
				}
				clause.setDescription("W");
				formula.addClause(clause);
			}
			formula.setNoOfVariables(potentialAnswersMap.size());
			return formula;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (con != null)
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}

	public CNFFormula createPotAnswerClauses() {
		CNFFormula f = new CNFFormula();
		Clause clause;
		for (List<String> key : potentialAnswersMap.keySet()) {
			clause = new Clause();
			clause.addVar(potentialAnswersMap.get(key));
			clause.setDescription("A");
			f.addClause(clause);
		}
		f.setNoOfVariables(0);
		return f;
	}

	private List<Integer> extractFreeVarColumnNames(String query) {
		String str = query.split("FROM")[0].replaceAll("SELECT", "").replaceAll(" ", "");
		String[] columns = str.split(",");
		List<Integer> output = new ArrayList<Integer>();
		for (int i = 0; i < columns.length; i++) {
			if (!columns[i].contains("factID"))
				output.add(i + 1);
		}
		return output;
	}

	public CNFFormula createPositiveClausesFromAllFacts() {
		PreparedStatement psKeyEqualGroups;
		CNFFormula formula = new CNFFormula();
		Clause clause = null;
		int totalFacts = 0;
		try {
			for (Relation r : relations) {
				String s = r.getAttributesFromIndexesCSV(r.getKeyAttributes(), r.getName() + "_VIEW");
				String q = "SELECT (" + s + "), FactID FROM " + r.getName() + "_VIEW ORDER BY " + s;
				psKeyEqualGroups = con.prepareStatement(q);
				System.out.println(q);
				ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
				String curValue = "", receivedValue = "";
				while (rsKeyEqualGroups.next()) {
					receivedValue = rsKeyEqualGroups.getString(1);
					if (!receivedValue.equals(curValue)) {
						if (null != clause && !clause.isEmpty()) {
							clause.setDescription("K");
							formula.addClause(clause);
						}
						clause = new Clause();
						clause.addVar(rsKeyEqualGroups.getInt(2));
						curValue = receivedValue;
					} else {
						clause.addVar(rsKeyEqualGroups.getInt(2));
					}
					totalFacts++;
				}
				if (null != clause) {
					clause.setDescription("K");
					formula.addClause(clause);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		formula.setNoOfVariables(totalFacts);
		return formula;
	}

	private String getQueryToFindWitnesses(Query query, Schema schema, boolean isQueryBoolean, boolean includeFactIDs,
			boolean selectRelevantAttribues) {
		String selectClause = "", fromClause = "", whereClause = "";

		// Forming select-clause and from-clause
		if (includeFactIDs)
			for (Atom atom : query.getAtoms()) {
				selectClause += atom.getName() + ".factID AS " + atom.getName()
						 + "fact,";
				fromClause += atom.getName() + "_VIEW " + atom.getName() + ",";
			}
		else {
			for (Atom atom : query.getAtoms()) {
				fromClause += atom.getName() + "_VIEW " + atom.getName() + ",";
			}
		}

		if (selectRelevantAttribues) {
			for (Atom atom : query.getAtoms()) {
				for (String attribute : schema.getRelationByName(atom.getName()).getRelevantAttributes()) {
					selectClause += atom.getName() + "." + attribute + " AS " + atom.getName()
							+ "_" + attribute + ",";
				}
			}
		}

		if (!isQueryBoolean && !selectRelevantAttribues) {
			int attributeIndex;
			boolean flag = false;
			for (String var : query.getFreeVars()) {
				flag = false;
				for (Atom atom : query.getAtoms()) {
					attributeIndex = 0;
					for (String attribute : atom.getVars()) {
						attributeIndex++;
						if (attribute.equals(var)) {
							selectClause += atom.getName() + "."
									+ schema.getAttributeNameByIndex(atom.getName(), attributeIndex) + ",";
							flag = true;
							break;
						}
					}
					if (flag)
						break;
				}
			}
		}

		// Forming where-clause
		for (Atom atom1 : query.getAtoms()) {
			for (Atom atom2 : query.getAtoms()) {
				if (!(atom1.equals(atom2))) {
					String[] atom1attr = atom1.getVarsCSV().split(",");
					String[] atom2attr = atom2.getVarsCSV().split(",");
					int index1 = 0, index2 = 0;
					for (String attr1 : atom1attr) {
						index1++;
						index2 = 0;
						for (String attr2 : atom2attr) {
							index2++;
							if (attr1.startsWith("'")) { // Attr1 has a constant value in a query
								whereClause += atom1.getName() + "."
										+ schema.getAttributeNameByIndex(atom1.getName(), index1) + "=" + attr1
										+ " AND ";
								break;
							} else if (attr1.equals(attr2)) {
								whereClause += atom1.getName() + "."
										+ schema.getAttributeNameByIndex(atom1.getName(), index1) + "="
										+ atom2.getName() + "."
										+ schema.getAttributeNameByIndex(atom2.getName(), index2) + " AND ";
							}
						}
					}
				}
			}
		}

		selectClause = selectClause.substring(0, selectClause.length() - 1); // Truncating last comma
		fromClause = fromClause.substring(0, fromClause.length() - 1); // Truncating last comma
		String thetaJoin = "SELECT " + selectClause + " FROM " + fromClause;
		if (!whereClause.isEmpty()) {
			// Truncating last AND and spaces around it
			whereClause = whereClause.substring(0, whereClause.length() - 5);
			thetaJoin += " WHERE " + whereClause;
		}
		System.out.println(thetaJoin);
		return thetaJoin;
	}

	public int createViews(Set<Relation> relations) {
		int noOfFacts = 0;
		DBEnvironment db = new DBEnvironment();
		Connection con = db.getConnection();
		try {
			for (Relation r : relations) {
				String createViewStatement = "CREATE OR REPLACE VIEW " + r.getName() + "_VIEW AS SELECT "
						+ r.getAllAttributesCSV("") + ", ROW_NUMBER() OVER (ORDER BY " + r.getKeysCSV() + ") + "
						+ noOfFacts + " AS FactID FROM " + r.getName();
				PreparedStatement psCreateView = con.prepareStatement(createViewStatement);
				psCreateView.executeUpdate();

				PreparedStatement psSelect = con.prepareStatement("SELECT COUNT(*) FROM " + r.getName() + "_VIEW");
				ResultSet rs = psSelect.executeQuery();
				rs.next();
				noOfFacts += rs.getInt(1);
			}
			return noOfFacts;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void eliminatePotentialAnswers(String filename, CNFFormula f) {
		boolean moreAnswers = true;
		List<String> assignment = null;
		Set<Integer> inconsistentAnswers = new HashSet<Integer>();
		int iterationCount = 0;
		String output = "";
		while (moreAnswers) {
			iterationCount++;
			moreAnswers = false;
			ExecCommand command = new ExecCommand();
			command.executeCommand(new String[] { "./maxhs", filename }, "output.txt");
			output = command.readOutput("output.txt");
			assignment = Arrays.asList(output.replaceAll("\n", "").split(" "));
			for (int answer : potentialAnswersMap.values()) {
				if (assignment.contains(Integer.toString(answer))) {
					moreAnswers = true;
					inconsistentAnswers.add(answer);
				}
			}
			// deleteInconsistentAdditionalAnswers(inconsistentAnswers);
			if (moreAnswers)
				changeFormula(inconsistentAnswers, filename, f);
		}
		System.out.println("MaxSAT Iterations: " + iterationCount);
	}

	private void changeFormula(Set<Integer> inconsistentAnswers, String filename, CNFFormula f) {
		for (int answer : inconsistentAnswers) {
			Clause clause = new Clause();
			clause.addVar(-1 * answer);
			clause.setDescription("I");
			f.addClause(clause);
		}
		Encoder.createDimacsFile(filename, f, 0);
	}
}
