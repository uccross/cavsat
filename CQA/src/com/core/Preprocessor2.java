package com.core;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class Preprocessor2 {
	private List<Query> uCQ;
	private Schema schema;
	private Connection con;

	public Preprocessor2(Schema schema, List<Query> uCQ, Connection con) {
		super();
		this.uCQ = uCQ;
		this.schema = schema;
		this.con = con;
	}

	public void createWitnessesToUCQ() {
		try {
			String createViewQuery = "CREATE OR REPLACE VIEW WITNESSES AS " + getUCQQuery();
			con.prepareStatement(createViewQuery).execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getUCQQuery() {
		String unionQuery = "", witnessQuery = "";
		for (Query query : this.uCQ) {
			witnessQuery = "";
			Set<Relation> participatingRelations = new HashSet<Relation>();
			Map<String, List<String>> varAttributes = new HashMap<String, List<String>>();
			for (Atom atom : query.getAtoms()) {
				participatingRelations.add(schema.getRelationByName(atom.getName()));
				for (int i = 0; i < atom.getVars().size(); i++) {
					String var = atom.getVars().get(i);
					if (!varAttributes.containsKey(var)) {
						varAttributes.put(var, new ArrayList<String>());
					}
					varAttributes.get(var).add(
							atom.getName() + "." + schema.getRelationByName(atom.getName()).getAttributes().get(i));
				}
			}
			// SELECT
			witnessQuery += "SELECT (";
			for (String var : query.getFreeVars()) {
				witnessQuery += varAttributes.get(var).get(0) + ",";
			}
			// FROM
			if (witnessQuery.endsWith("(")) {
				witnessQuery += "TRUE) FROM ";
			} else {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 1) + ") FROM ";
			}
			for (Relation r : participatingRelations) {
				witnessQuery += r.getName() + ",";
			}
			// WHERE
			witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 1) + " WHERE ";
			for (String var : varAttributes.keySet()) {
				for (int i = 1; i < varAttributes.get(var).size(); i++) {
					witnessQuery += varAttributes.get(var).get(0) + "=" + varAttributes.get(var).get(i) + " AND ";
				}
			}
			if (witnessQuery.endsWith(" WHERE ")) {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 7);
			} else if (witnessQuery.endsWith(" AND ")) {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 5);
			}
			unionQuery = unionQuery + "(" + witnessQuery + ") UNION ";
		}
		if (unionQuery.endsWith(" UNION ")) {
			unionQuery = unionQuery.substring(0, unionQuery.length() - 7);
		}
		return unionQuery;
	}
}
