package com.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class Preprocessor {
	private Query query;
	private Set<Relation> relations;
	private Connection con;

	public Preprocessor(Schema schema, Query query, Connection con) {
		super();
		this.query = query;
		this.relations = schema.getRelationsByNames(query.getParticipatingRelationNames());
		this.con = con;
	}

	public void createKeysViews() {
		try {
			for (Relation r : relations) {
				con.prepareStatement("DROP TABLE IF EXISTS KEYS_TABLE_" + r.getName() + " CASCADE").execute();
				PreparedStatement psKeys = con.prepareStatement("CREATE TABLE KEYS_TABLE_" + r.getName() + " AS SELECT "
						+ r.getAttributesFromIndexesCSV(r.getKeyAttributes(), r.getName()) + " FROM " + r.getName()
						+ " GROUP BY " + r.getAttributesFromIndexesCSV(r.getKeyAttributes(), r.getName())
						+ " HAVING COUNT(*) > 1");
				psKeys.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createIndexesOnKeys() {
		try {
			for (Relation r : relations) {
				//con.prepareStatement("DROP INDEX IF EXISTS INDEX_" + r.getName()).execute();
				con.prepareStatement("CREATE INDEX INDEX_" + r.getName() + " ON " + r.getName() + "("
						+ r.getAttributesFromIndexesCSV(r.getKeyAttributes(), null) + ")").execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createAnsFromCons() {
		String q = "CREATE TABLE ANS_FROM_CONS AS SELECT ";
		if (query.isBoolean()) {
			q += "TRUE ";
		} else {
			for (String var : query.getFreeVars()) {
				System.out.println(var);
				String attr = getAttributeFromQueryVar(null, var);
				System.out.println(attr);
				q += attr + " AS " + attr.replaceAll("\\.", "_") + ",";
			}
			q = q.substring(0, q.length() - 1);
		}
		q += " FROM ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + " " + atom.getName() + atom.getAtomIndex() + ",";
		}
		q = q.substring(0, q.length() - 1) + " WHERE ";

		// Forming join conditions
		for (Atom first : query.getAtoms()) {
			for (Atom second : query.getAtoms()) {
				if (first.equals(second))
					continue;
				for (String attr1 : first.getAttributes()) {
					for (String attr2 : second.getAttributes()) {
						if (attr1.equals(attr2)) {
							q += getAttributeFromQueryVar(first, attr1) + "=" + getAttributeFromQueryVar(second, attr2)
									+ " AND ";
						}
					}
				}
			}
		}
		if (q.endsWith(" AND "))
			q = q.substring(0, q.length() - 5);

		// Taking from only the consistent part
		for (Relation r : relations) {
			 q += " AND (" + r.getAttributesFromIndexesCSV(r.getKeyAttributes(),
			 r.getName() + "1")
			 + ") NOT IN (SELECT * FROM KEYS_TABLE_" + r.getName() + ")";

			/*q += " AND NOT EXISTS (SELECT 1 FROM KEYS_TABLE_" + r.getName() + " WHERE ";
			for (String s : r.getKeyAttributesList()) {
				q += r.getName() + "1." + s + "=KEYS_TABLE_" + r.getName() + "." + s + " AND ";
			}
			if (q.endsWith(" AND "))
				q = q.substring(0, q.length() - 5);
			q += " LIMIT 1)";*/
		}
		 if (q.endsWith(" AND "))
		 q = q.substring(0, q.length() - 5);
		if (!query.isBoolean()) {
			q += " GROUP BY ";
			for (String var : query.getFreeVars()) {
				q += getAttributeFromQueryVar(null, var) + ",";
			}
			q = q.substring(0, q.length() - 1);
		}
		System.out.println(q);
		try {
			//con.prepareStatement("DROP TABLE IF EXISTS ANS_FROM_CONS CASCADE").execute();
			PreparedStatement psAnsFromCons = con.prepareStatement(q);
			psAnsFromCons.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean checkBooleanConsAnswer() {
		String q = "SELECT COUNT(*) FROM ANS_FROM_CONS";
		try {
			ResultSet rsCheckBooleanConsAnswer = con.prepareStatement(q).executeQuery();
			if (rsCheckBooleanConsAnswer.next()) {
				return rsCheckBooleanConsAnswer.getInt(1) != 0;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return false;
	}

	public void createWitnesses(boolean includeFactIDs) {
		String prefix = "", relationName = "";
		if (includeFactIDs)
			relationName = "WITNESSES_WITH_FactID";
		else
			relationName = "WITNESSES";
		String q = "CREATE TABLE " + relationName + " AS SELECT ";
		for (Atom atom : query.getAtoms()) {
			for (String var : atom.getAttributes()) {
				String attr = getAttributeFromQueryVar(atom, var);
				q += attr + " AS " + attr.replaceAll("\\.", "_") + ",";
			}
		}
		q = q.substring(0, q.length() - 1);
		prefix = "";
		if (includeFactIDs) {
			for (Atom atom : query.getAtoms()) {
				q += ", " + atom.getName() + atom.getAtomIndex() + ".FactID AS " + atom.getName() + "_"
						+ atom.getAtomIndex() + "_FactID";
			}
			prefix = "RELEVANT_";
		}
		q += " FROM ";
		for (Atom atom : query.getAtoms()) {
			q += prefix + atom.getName() + " " + atom.getName() + atom.getAtomIndex() + ",";
		}
		q = q.substring(0, q.length() - 1);

		q += " WHERE ";
		// Forming join conditions
		for (Atom first : query.getAtoms()) {
			for (Atom second : query.getAtoms()) {
				if (first.equals(second))
					continue;
				for (String attr1 : first.getAttributes()) {
					for (String attr2 : second.getAttributes()) {
						if (attr1.equals(attr2)) {
							q += getAttributeFromQueryVar(first, attr1) + "=" + getAttributeFromQueryVar(second, attr2)
									+ " AND ";
						}
					}
				}
			}
		}

		if (query.isBoolean()) {
			if (q.endsWith(" AND "))
				q = q.substring(0, q.length() - 5);
		} else {
			if (!q.endsWith(" AND "))
				q += " AND ";

			
			  q += "("; for (String var : query.getFreeVars()) { q +=
			  getAttributeFromQueryVar(null, var) + ","; } q = q.substring(0, q.length() -
			  1); q += ")"; q += " NOT IN (SELECT * FROM ANS_FROM_CONS)";
			 

			/*q += "NOT EXISTS (SELECT 1 FROM ANS_FROM_CONS WHERE ";
			for (String var : query.getFreeVars()) {
				String attr = getAttributeFromQueryVar(null, var);
				q += attr + " = ANS_FROM_CONS." + attr.replaceAll("\\.", "_") + " AND ";
			}
			if (q.endsWith(" AND "))
				q = q.substring(0, q.length() - 5);
			q += ")";*/
		}

		try {
			//con.prepareStatement("DROP TABLE IF EXISTS " + relationName + " CASCADE").execute();
			System.out.println(q);
			PreparedStatement psWitnesses = con.prepareStatement(q);
			psWitnesses.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int createRelevantViews() {
		int count = 0;
		for (Relation r : relations) {
			String q = "CREATE TABLE RELEVANT_" + r.getName() + " AS SELECT " + r.getName() + ".*, " + count
					+ " + ROW_NUMBER() OVER (ORDER BY 1) AS FactID FROM WITNESSES, " + r.getName() + " WHERE ";
			/*
			 * for (String key : r.getKeyAttributesList()) { q += " INNER JOIN " +
			 * r.getName() + " ON (" + r.getName() + "." + key + " = WITNESSES." + key +
			 * ")"; }
			 */
			for (Atom atom : query.getAtoms()) {
				if (atom.getName().equalsIgnoreCase(r.getName())) {
					for (String key : r.getKeyAttributesList()) {
						q += r.getName() + "." + key + " = WITNESSES." + atom.getName() + atom.getAtomIndex() + "_"
								+ key + " AND ";
					}
				}
			}
			/*
			 * for (String key : r.getKeyAttributesList()) { q += r.getName() + "." + key +
			 * " = WITNESSES." + key + " AND "; }
			 */
			if (q.endsWith(" AND "))
				q = q.substring(0, q.length() - 5);
			q += " GROUP BY " + r.getAllAttributesCSV(r.getName());
			System.out.println(q);
			try {
				//con.prepareStatement("DROP TABLE IF EXISTS RELEVANT_" + r.getName() + " CASCADE").execute();
				PreparedStatement psRelevant = con.prepareStatement(q);
				psRelevant.execute();
				ResultSet rs = con.prepareStatement("SELECT COUNT(*) FROM RELEVANT_" + r.getName()).executeQuery();
				rs.next();
				count += rs.getInt(1);
			} catch (SQLException e) {
				e.printStackTrace();
				return -1;
			}
		}
		return count;
	}

	private String getAttributeFromQueryVar(Atom atom, String var) {
		if (atom != null) {
			for (Relation r : relations) {
				if (r.getName().equalsIgnoreCase(atom.getName())) {
					return atom.getName() + atom.getAtomIndex() + "."
							+ r.getAttributes().get(atom.getAttributes().indexOf(var));
				}
			}
		} else {
			for (Atom curAtom : query.getAtoms()) {
				if (curAtom.getAttributes().indexOf(var) != -1) {
					for (Relation r : relations) {
						if (r.getName().equalsIgnoreCase(curAtom.getName())) {
							return curAtom.getName() + curAtom.getAtomIndex() + "."
									+ r.getAttributes().get(curAtom.getAttributes().indexOf(var));
						}
					}
				}
			}
		}
		return null;
	}
	
	public void dropAllTables() {
		try {
			con.prepareStatement("DROP TABLE IF EXISTS ANS_FROM_CONS CASCADE").execute();
			con.prepareStatement("DROP TABLE IF EXISTS WITNESSES CASCADE").execute();
			con.prepareStatement("DROP TABLE IF EXISTS WITNESSES_WITH_FACTID CASCADE").execute();
			for (Relation r : relations) {
				con.prepareStatement("DROP TABLE IF EXISTS RELEVANT_" + r.getName() + " CASCADE").execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}