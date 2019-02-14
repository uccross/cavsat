package com.core;

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
import com.util.Constants;

public class Encoder2 {
	private Query query;
	private Set<Relation> relations;
	private Connection con;

	public Encoder2(Schema schema, Query query, Connection con) {
		super();
		this.query = query;
		this.relations = schema.getRelationsByNames(query.getParticipatingRelationNames());
		this.con = con;
	}

	public static void main(String[] args) {

	}

	public CNFFormula createNegativeClauses(int totalRelevantFacts, int approach) {
		if (query.isBoolean())
			return createBooleanNegClauses(totalRelevantFacts);
		else {
			createAdditionalPotentialAnswers(totalRelevantFacts);
			return createNonBooleanNegativeClauses();
		}
	}

	private CNFFormula createBooleanNegClauses(int totalFacts) {
		String q = "SELECT ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_" + atom.getAtomIndex() + "_factid,";
		}
		q = q.substring(0, q.length() - 1);
		q += " FROM " + Constants.witnessesWithFactIDs;
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

	private CNFFormula createNonBooleanNegativeClauses() {
		int querySize = query.getSize();
		Set<Integer> vars = new HashSet<Integer>();
		String q = "SELECT ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_" + atom.getAtomIndex() + "_factid,";
		}
		q += Constants.additionalAnswers + ".FactID FROM " + Constants.witnessesWithFactIDs + ","
				+ Constants.additionalAnswers + " WHERE ";
		for (String var : query.getFreeVars()) {
			String attr = getAttributeFromQueryVar(null, var);
			q += Constants.witnessesWithFactIDs + "." + attr + "=" + Constants.additionalAnswers + "." + attr + " AND ";
		}
		q = q.substring(0, q.length() - 5);
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
				additionalPositiveClause = new Clause();
				additionalPositiveClause.addVar(extraVar);
				vars.add(extraVar);
				clause.setDescription("W");
				formula.addClause(clause);
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
		String q = "CREATE TABLE " + Constants.additionalAnswers + " AS SELECT " + attrs + "," + totalFacts + "+"
				+ " ROW_NUMBER() OVER (ORDER BY 1) AS FactID";
		q += " FROM " + Constants.witnessesWithFactIDs + " GROUP BY " + attrs;
		// System.out.println(q);
		try {
			con.prepareStatement("DROP TABLE IF EXISTS " + Constants.additionalAnswers).execute();
			PreparedStatement psAdditionalAnswers = con.prepareStatement(q);
			psAdditionalAnswers.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String getAttributeFromQueryVar(Atom atom, String var) {
		if (atom != null) {
			for (Relation r : relations) {
				if (r.getName().equalsIgnoreCase(atom.getName())) {
					return atom.getName() + atom.getAtomIndex() + "_"
							+ r.getAttributes().get(atom.getVars().indexOf(var));
				}
			}
		} else {
			for (Atom curAtom : query.getAtoms()) {
				if (curAtom.getVars().indexOf(var) != -1) {
					for (Relation r : relations) {
						if (r.getName().equalsIgnoreCase(curAtom.getName())) {
							return curAtom.getName() + curAtom.getAtomIndex() + "_"
									+ r.getAttributes().get(curAtom.getVars().indexOf(var));
						}
					}
				}
			}
		}
		return null;
	}
}
