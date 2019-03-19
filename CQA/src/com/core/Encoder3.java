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
import java.util.HashSet;
import java.util.Set;

import com.beans.Atom;
import com.beans.Clause;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class Encoder3 {
	private Query query;
	private Set<Relation> relations;
	private Connection con;
	private BufferedWriter br;

	public Encoder3(Schema schema, Query query, Connection con, String formulaFileName) {
		super();
		this.query = query;
		this.relations = schema.getRelationsByNames(query.getParticipatingRelationNames());
		this.con = con;
		try {
			this.br = new BufferedWriter(new FileWriter(formulaFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeConnection() {
		try {
			this.con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeBufferedReader() {
		try {
			this.br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int[] createNegativeClauses(int totalRelevantFacts) {
		if (query.isBoolean())
			return createBooleanNegClauses();
		else {
			createAdditionalPotentialAnswers(totalRelevantFacts);
			return createNonBooleanNegClauses();
		}
	}

	private int[] createBooleanNegClauses() {
		String q = "SELECT ";
		int clauses = 0;
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_factid,";
		}
		q = q.substring(0, q.length() - 1);
		q += " FROM WITNESSES_WITH_FACTID";
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
				br.append(clause.getDimacsLine());
				clauses++;
			}
			return new int[] { -1, clauses };
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return new int[] { -1, -1 };
		}
	}

	public void writeFinalFormula(String formulaFileName, int vars, int clauses) {
		try {
			String firstLine = "p wcnf " + vars + " " + clauses + "\n";
			int infinity = clauses * 10;
			BufferedReader br = new BufferedReader(new FileReader(formulaFileName));
			BufferedWriter wr = new BufferedWriter(new FileWriter("final" + formulaFileName));
			wr.append(firstLine);
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("A")) {
					wr.append("1 " + sCurrentLine + "\n");
				} else {
					wr.append(Integer.toString(infinity) + " " + sCurrentLine + "\n");
				}
			}
			br.close();
			wr.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private int[] createNonBooleanNegClauses() {
		int querySize = query.getSize();
		int clauses = 0;
		Set<Integer> vars = new HashSet<Integer>();
		String q = "SELECT ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + "_factid,";
		}
		q += "ADDITIONAL_ANSWERS.FactID FROM WITNESSES_WITH_FACTID, ADDITIONAL_ANSWERS WHERE ";
		for (String var : query.getFreeVars()) {
			String attr = getAttributeFromQueryVar(null, var);
			q += "WITNESSES_WITH_FACTID." + attr + "=" + "ADDITIONAL_ANSWERS." + attr + " AND ";
		}
		q = q.substring(0, q.length() - 5);
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
				vars.add(extraVar);
				clause.addVar(-1 * extraVar);
				clause.setDescription("W");
				br.append(clause.getDimacsLine());
				clauses++;

				additionalPositiveClause = new Clause();
				additionalPositiveClause.addVar(extraVar);
				additionalPositiveClause.setDescription("A");
				br.append(additionalPositiveClause.getDimacsLine());
				clauses++;
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return new int[] { vars.size(), clauses };
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
		try {
			con.prepareStatement("DROP TABLE IF EXISTS ADDITIONAL_ANSWERS").execute();
			PreparedStatement psAdditionalAnswers = con.prepareStatement(q);
			psAdditionalAnswers.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int createPositiveClauses() {
		PreparedStatement psKeyEqualGroups;
		int clauses = 0;
		Clause clause = null;
		try {
			for (Relation r : relations) {
				String s = r.getAttributesFromIndexesCSV(r.getKeyAttributes(), "RELEVANT_" + r.getName());
				String q = "SELECT (" + s + "), FactID FROM RELEVANT_" + r.getName() + " ORDER BY " + s;
				psKeyEqualGroups = con.prepareStatement(q);
				ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
				String curValue = "", receivedValue = "";
				while (rsKeyEqualGroups.next()) {
					receivedValue = rsKeyEqualGroups.getString(1);
					if (!receivedValue.equals(curValue)) {
						if (null != clause) {
							clause.setDescription("K");
							br.append(clause.getDimacsLine());
							clauses++;
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
					br.append(clause.getDimacsLine());
					clauses++;
				}
			}
			return clauses;
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	private String getAttributeFromQueryVar(Atom atom, String var) {
		if (atom != null) {
			for (Relation r : relations) {
				if (r.getName().equalsIgnoreCase(atom.getName())) {
					return atom.getName() + "_" + r.getAttributes().get(atom.getVars().indexOf(var));
				}
			}
		} else {
			for (Atom curAtom : query.getAtoms()) {
				if (curAtom.getVars().indexOf(var) != -1) {
					for (Relation r : relations) {
						if (r.getName().equalsIgnoreCase(curAtom.getName())) {
							return curAtom.getName() + "_" + r.getAttributes().get(curAtom.getVars().indexOf(var));
						}
					}
				}
			}
		}
		return null;
	}
}