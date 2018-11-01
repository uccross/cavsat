package com.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.beans.Atom;
import com.beans.CNFFormula;
import com.beans.Clause;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class Encoder {
	private Query query;
	private Set<Relation> relations;
	private Connection con;

	public Encoder(Schema schema, Query query, Connection con) {
		super();
		this.query = query;
		this.relations = schema.getRelationsByNames(query.getParticipatingRelationNames());
		this.con = con;
	}

	public CNFFormula createNegativeClauses(int totalRelevantFacts, int approach) {
		if (query.isBoolean())
			return createBooleanNegClauses(totalRelevantFacts);
		else {
			createAdditionalPotentialAnswers(totalRelevantFacts);
			return createNonBooleanNegativeClauses(approach);
		}
	}

	private CNFFormula createBooleanNegClauses(int totalFacts) {
		String q = "SELECT ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_" + atom.getAtomIndex() + "_factid,";
		}
		q = q.substring(0, q.length() - 1);
		q += " FROM WITNESSES_WITH_FACTID";
		CNFFormula formula = new CNFFormula();
		PreparedStatement psWitnessFacts;
		try {
			psWitnessFacts = con.prepareStatement(q);
			ResultSet rsWitnessFacts = psWitnessFacts.executeQuery();
			while (rsWitnessFacts.next()) {
				Clause clause = new Clause();
				for (int i = 1; i <= query.getSize(); i++) {
					clause.addVar(-1 * rsWitnessFacts.getInt(i));
				}
				clause.setDescription("W");
				formula.addClause(clause);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		formula.setNoOfVariables(totalFacts);
		return formula;
	}

	/**
	 * @param approach
	 *            0 for MaxSAT, 1 for Iterative SAT. Default is 0.
	 * @return
	 */
	private CNFFormula createNonBooleanNegativeClauses(int approach) {
		int querySize = query.getSize();
		Set<Integer> vars = new HashSet<Integer>();
		String q = "SELECT ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_" + atom.getAtomIndex() + "_factid,";
		}
		q += "ADDITIONAL_ANSWERS.FactID FROM WITNESSES_WITH_FACTID, ADDITIONAL_ANSWERS WHERE ";
		for (String var : query.getFreeVars()) {
			String attr = getAttributeFromQueryVar(null, var);
			q += "WITNESSES_WITH_FACTID." + attr + "=" + "ADDITIONAL_ANSWERS." + attr + " AND ";
		}
		q = q.substring(0, q.length() - 5);
		//System.out.println(q);
		CNFFormula formula = new CNFFormula();
		Clause additionalPositiveClause = new Clause();
		PreparedStatement psWitnessFacts;
		try {
			psWitnessFacts = con.prepareStatement(q);
			ResultSet rsWitnessFacts = psWitnessFacts.executeQuery();
			while (rsWitnessFacts.next()) {
				Clause clause = new Clause();
				for (int i = 1; i <= querySize; i++) {
					clause.addVar(-1 * rsWitnessFacts.getInt(i));
				}
				int extraVar = rsWitnessFacts.getInt(querySize + 1);// Additional variable corresponding to the
																	// potential answer
				clause.addVar(-1 * extraVar);
				if (approach == 0) {
					additionalPositiveClause = new Clause();
				}
				additionalPositiveClause.addVar(extraVar);
				vars.add(extraVar);
				clause.setDescription("W");
				formula.addClause(clause);
				if (approach == 0) {
					additionalPositiveClause.setDescription("A");
					formula.addClause(additionalPositiveClause);
				}
			}
			if (approach == 1) {
				additionalPositiveClause.setDescription("A");
				formula.addClause(additionalPositiveClause);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		formula.setNoOfVariables(vars.size());
		return formula;
	}

	private void createAdditionalPotentialAnswers(int totalFacts) {
		String attrs = "";
		for (String var : query.getFreeVars()) {
			attrs += getAttributeFromQueryVar(null, var) + ",";
		}
		attrs = attrs.substring(0, attrs.length() - 1);
		String q = "CREATE TABLE ADDITIONAL_ANSWERS AS SELECT " + attrs + "," + totalFacts + "+"
				+ " ROW_NUMBER() OVER (ORDER BY 1) AS FactID";
		q += " FROM WITNESSES_WITH_FACTID GROUP BY " + attrs;
		//System.out.println(q);
		try {
			con.prepareStatement("DROP TABLE IF EXISTS ADDITIONAL_ANSWERS").execute();
			PreparedStatement psAdditionalAnswers = con.prepareStatement(q);
			psAdditionalAnswers.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public CNFFormula createPositiveClauses(int totalFacts) {
		PreparedStatement psKeyEqualGroups;
		CNFFormula formula = new CNFFormula();
		Clause clause = null;
		try {
			for (Relation r : relations) {
				String s = r.getAttributesFromIndexesCSV(r.getKeyAttributes(), "RELEVANT_" + r.getName());
				String q = "SELECT (" + s + "), FactID FROM RELEVANT_" + r.getName() + " ORDER BY " + s;
				psKeyEqualGroups = con.prepareStatement(q);
				//System.out.println(q);
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

	private String getAttributeFromQueryVar(Atom atom, String var) {
		if (atom != null) {
			for (Relation r : relations) {
				if (r.getName().equalsIgnoreCase(atom.getName())) {
					return atom.getName() + atom.getAtomIndex() + "_"
							+ r.getAttributes().get(atom.getAttributes().indexOf(var));
				}
			}
		} else {
			for (Atom curAtom : query.getAtoms()) {
				if (curAtom.getAttributes().indexOf(var) != -1) {
					for (Relation r : relations) {
						if (r.getName().equalsIgnoreCase(curAtom.getName())) {
							return curAtom.getName() + curAtom.getAtomIndex() + "_"
									+ r.getAttributes().get(curAtom.getAttributes().indexOf(var));
						}
					}
				}
			}
		}
		return null;
	}

	public static void createDimacsFile(String filepath, CNFFormula formula, int approach) {
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(filepath));
			int clauses = formula.getClauses().size();
			if (approach == 0) {
				int infinity = formula.getNoOfVariables() + 1;
				writer.append("p wcnf " + formula.getNoOfVariables() + " " + clauses + "\n");
				for (Clause c : formula.getClauses()) {
					if (c.getDescription().contains("A"))
						writer.append("1 " + c.getDimacsLine());
					else
						writer.append(infinity + " " + c.getDimacsLine());
				}
			} else {
				writer.append("p cnf " + formula.getNoOfVariables() + " " + clauses + "\n");
				for (Clause c : formula.getClauses()) {
					writer.append(c.getDimacsLine());
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}