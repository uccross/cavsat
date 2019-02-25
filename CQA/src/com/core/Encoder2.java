package com.core;

import java.io.BufferedWriter;
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

import com.beans.Atom;
import com.beans.Clause;
import com.beans.DenialConstraint;
import com.beans.Expression;
import com.beans.Query;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class Encoder2 {
	private List<Query> uCQ;
	private Schema schema;
	private Connection con;
	private BufferedWriter br;
	private int yVarIndex = 0;

	public Encoder2(Schema schema, List<Query> uCQ, Connection con, String formulaFileName) {
		super();
		this.uCQ = uCQ;
		this.schema = schema;
		this.con = con;
		try {
			this.br = new BufferedWriter(new FileWriter(formulaFileName, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\dc-schema.txt");
		schema.print();
		List<Query> uCQ = pp.parseUCQ(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\dc-q1.txt");
		Encoder2 encoder = new Encoder2(schema, uCQ, new DBEnvironment().getConnection(),
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\dc-formula.txt");
		encoder.createAlphaClauses();
		encoder.createGammaClauses();
		encoder.createThetaClauses();
		encoder.createBetaClauses();
		try {
			encoder.br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method fills the tables Near_violations and yVars. These tables are
	 * necessary to compute gamma clauses and theta expressions. Hence this method
	 * must be called before creating gamma clauses and theta expressions.
	 */
	public void createAlphaClauses() {
		Set<Integer> minViolation = new HashSet<Integer>();
		PreparedStatement psUpdate = null, psSelect = null, psInsert = null, psSelectFromYVars = null;
		int totalFacts = 0;

		try {
			con.prepareStatement("DROP TABLE IF EXISTS NEAR_VIOLATIONS").execute();
			con.prepareStatement("CREATE TABLE NEAR_VIOLATIONS (FactID int, yVars varchar(1000))").execute();
			con.prepareStatement("DROP TABLE IF EXISTS YVARS").execute();
			con.prepareStatement("CREATE TABLE YVARS (NearViolation varchar(1000), yvar int)").execute();
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

		yVarIndex = totalFacts + 1;
		for (DenialConstraint dc : schema.getConstraints()) {
			String sqlQuery = getSQLQueryFromDC(dc);
			try {
				psSelect = con.prepareStatement(sqlQuery);
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
					System.out.print(clause.getDimacsLine());
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
								psInsert.setInt(2, yVarIndex);
								psInsert.setString(3, temp.toString());

								if (psInsert.executeUpdate() == 0) {
									psSelectFromYVars.setString(1, temp.toString());
									ResultSet rsSelectFromYVars = psSelectFromYVars.executeQuery();
									rsSelectFromYVars.next();
									yVar = rsSelectFromYVars.getInt(1);
									rsSelectFromYVars.close();
								} else {
									yVar = yVarIndex;
									yVarIndex++;
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

	public void createBetaClauses() {
		PreparedStatement psInsert = null, psSelectPotentialAnswer;
		try {
			con.prepareStatement("DROP TABLE IF EXISTS Potential_Answers").execute();
			con.prepareStatement("CREATE TABLE Potential_Answers (Answer varchar(1000), pVar int)").execute();
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
				psInsert.setInt(2, yVarIndex);
				psInsert.setString(3, rs.getString(2));
				if (psInsert.executeUpdate() == 0) {
					psSelectPotentialAnswer.setString(1, rs.getString(2));
					ResultSet rsSelectPotentialAnswer = psSelectPotentialAnswer.executeQuery();
					rsSelectPotentialAnswer.next();
					pVar = rsSelectPotentialAnswer.getInt(1);
					rsSelectPotentialAnswer.close();
				} else {
					pVar = yVarIndex;
					yVarIndex++;
				}
				Clause clause = new Clause();
				clause.addVar(-1 * pVar);
				String witnessString = rs.getString(1);
				for (int fact : parseWitnessString(witnessString)) {
					clause.addVar(-1 * fact);
				}
				clause.setDescription("B");
				br.append(clause.getDimacsLine());
			}
		} catch (SQLException | IOException e) {
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
			}
		} catch (SQLException | IOException e) {
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
					theta2.addVar(-1 * x);
				}
				br.append(theta2.getDimacsLine());
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	private Set<Integer> parseYVarsString(String s) {
		Set<Integer> yVars = new HashSet<Integer>();
		for (String str : s.split(";"))
			if (!str.isEmpty())
				yVars.add(Integer.parseInt(str));
		return yVars;
	}

	private Set<Integer> parseNearViolationString(String nv) {
		Set<Integer> yVars = new HashSet<Integer>();
		for (String str : nv.substring(1, nv.length() - 1).split(",")) {
			// Substring removes square bracket characters '[' and ']'
			yVars.add(Integer.parseInt(str.replaceAll("\\s+", "")));
		}
		return yVars;
	}

	private Set<Integer> parseWitnessString(String s) {
		Set<Integer> witnesses = new HashSet<Integer>();
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
			con.prepareStatement("CREATE TABLE MIN_WITNESSES (WITNESS VARCHAR(1000))").execute();
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
		for (Query query : uCQ) {
			for (Atom atom : query.getAtoms()) {
				q.put(atom.getName(), "SELECT FactID from " + atom.getName());
			}
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
