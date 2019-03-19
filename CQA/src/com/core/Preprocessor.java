package com.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class Preprocessor {
	private Query query;
	private Set<Relation> relations;
	private Connection con;

	public Query getQuery() {
		return query;
	}

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
				// con.prepareStatement("DROP INDEX IF EXISTS INDEX_" + r.getName()).execute();
				con.prepareStatement("CREATE INDEX INDEX_" + r.getName() + " ON " + r.getName() + "("
						+ r.getAttributesFromIndexesCSV(r.getKeyAttributes(), null) + ")").execute();
			}
		} catch (SQLException e) {
			// e.printStackTrace();
		}
	}

	public void createAnsFromCons() {
		String q = "CREATE TABLE ANS_FROM_CONS AS SELECT ";
		if (query.isBoolean()) {
			q += "TRUE ";
		} else {
			for (String var : query.getFreeVars()) {
				String attr = getAttributeFromQueryVar(null, var);
				q += attr + " AS " + attr.replaceAll("\\.", "_") + ",";
			}
			q = q.substring(0, q.length() - 1);
		}
		q += " FROM ";
		for (Atom atom : query.getAtoms()) {
			q += atom.getName() + " " + atom.getName() + ",";
		}
		q = q.substring(0, q.length() - 1) + " WHERE ";

		// Forming join conditions
		for (Atom first : query.getAtoms()) {
			for (Atom second : query.getAtoms()) {
				if (first.equals(second))
					continue;
				for (String attr1 : first.getVars()) {
					for (String attr2 : second.getVars()) {
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
			q += " AND (" + r.getAttributesFromIndexesCSV(r.getKeyAttributes(), r.getName())
					+ ") NOT IN (SELECT * FROM KEYS_TABLE_" + r.getName() + ")";
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
		// System.out.println(q);
		try {
			// con.prepareStatement("DROP TABLE IF EXISTS ANS_FROM_CONS CASCADE").execute();
			PreparedStatement psAnsFromCons = con.prepareStatement(q);
			// System.out.println(q);
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

	public void createWitnesses(boolean includeFactIDs, Schema schema) {
		String prefix = "", relationName = "";
		if (includeFactIDs)
			relationName = "WITNESSES_WITH_FactID";
		else
			relationName = "WITNESSES";
		String q = "CREATE TABLE " + relationName + " AS SELECT ";
		for (Atom atom : query.getAtoms()) {
			for (String var : atom.getVars()) {
				String attr = getAttributeFromQueryVar(atom, var);
				q += attr + " AS " + attr.replaceAll("\\.", "_") + ",";
			}
		}
		q = q.substring(0, q.length() - 1);
		prefix = "";
		if (includeFactIDs) {
			for (Atom atom : query.getAtoms()) {
				q += ", " + atom.getName() + ".FactID AS " + atom.getName() + "_FactID";
			}
			prefix = "RELEVANT_";
		}
		q += " FROM ";
		for (Atom atom : query.getAtoms()) {
			q += prefix + atom.getName() + " " + atom.getName() + ",";
		}
		q = q.substring(0, q.length() - 1);

		
		q += " WHERE ";
		// Forming join conditions
		Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
		for (Atom atom : query.getAtoms()) {
			Relation relation = schema.getRelationByName(atom.getName());
			for (int i = 0; i < atom.getVars().size(); i++) {
				String var = atom.getVars().get(i);
				if (varAttrMap.containsKey(var)) {
					varAttrMap.get(var).add(relation.getName() + "." + relation.getAttributes().get(i));
				} else {
					List<String> list = new ArrayList<String>();
					list.add(relation.getName() + "." + relation.getAttributes().get(i));
					varAttrMap.put(var, list);

				}
			}

		}
		for (String var : varAttrMap.keySet()) {
			if (varAttrMap.get(var).size() > 1) {
				String first = varAttrMap.get(var).get(0);
				for (int i = 1; i < varAttrMap.get(var).size(); i++) {
					q = q + first + "=" + varAttrMap.get(var).get(i) + " AND ";
				}
			}
		}

		/*
		 * for (Atom first : query.getAtoms()) { for (Atom second : query.getAtoms()) {
		 * if (first.equals(second)) continue; for (String attr1 : first.getVars()) {
		 * for (String attr2 : second.getVars()) { if (attr1.equals(attr2)) { q +=
		 * getAttributeFromQueryVar(first, attr1) + "=" +
		 * getAttributeFromQueryVar(second, attr2) + " AND "; } } } } }
		 */

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
			// con.prepareStatement("DROP TABLE IF EXISTS " + relationName + "
			// CASCADE").execute();
			System.out.println(q);
			PreparedStatement psWitnesses = con.prepareStatement(q);
			System.out.println("formed preparedstatement");
			psWitnesses.executeUpdate();
			System.out.println("computed witnesses");
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
						q += r.getName() + "." + key + " = WITNESSES." + atom.getName() + "_" + key + " AND ";
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
			// System.out.println(q);
			try {
				// con.prepareStatement("DROP TABLE IF EXISTS RELEVANT_" + r.getName() + "
				// CASCADE").execute();
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
					return atom.getName() + "." + r.getAttributes().get(atom.getVars().indexOf(var));
				}
			}
		} else {
			for (Atom curAtom : query.getAtoms()) {
				if (curAtom.getVars().indexOf(var) != -1) {
					for (Relation r : relations) {
						if (r.getName().equalsIgnoreCase(curAtom.getName())) {
							return curAtom.getName() + "." + r.getAttributes().get(curAtom.getVars().indexOf(var));
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