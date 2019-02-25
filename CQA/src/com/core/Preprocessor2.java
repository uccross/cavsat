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
import com.beans.TRCQuery;
import com.beans.TRCQuery.TupleVar;
import com.util.Constants;

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

	public void createMinimalWitnessesToUCQ() {
		try {
			String createViewQuery = "CREATE OR REPLACE VIEW " + Constants.minimalWitnesses + " AS \n" + getUCQQuery();
			System.out.println(createViewQuery);
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
			Map<String, List<TupleVar>> varAttributes = new HashMap<String, List<TupleVar>>();
			for (Atom atom : query.getAtoms()) {
				participatingRelations.add(schema.getRelationByName(atom.getName()));
				for (int i = 0; i < atom.getVars().size(); i++) {
					String var = atom.getVars().get(i);
					if (!varAttributes.containsKey(var)) {
						varAttributes.put(var, new ArrayList<TupleVar>());
					}
					varAttributes.get(var).add(new TRCQuery().new TupleVar(schema.getRelationByName(atom.getName()),
							schema.getRelationByName(atom.getName()).getAttributes().get(i)));
				}
			}
			// SELECT
			witnessQuery += "SELECT ";
			for (String var : query.getFreeVars()) {
				for (TupleVar pair : varAttributes.get(var)) {
					witnessQuery += pair.getRelation().getName() + "." + pair.getVar() + " AS "
							+ pair.getRelation().getName() + "_" + pair.getVar() + ", ";
				}
			}
			// FROM
			if (query.isBoolean()) {
				witnessQuery += "TRUE FROM ";
			} else {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 2) + " FROM ";
			}
			for (Relation r : participatingRelations) {
				witnessQuery += r.getName() + ",";
			}
			// WHERE
			witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 1) + " WHERE ";
			for (String var : varAttributes.keySet()) {
				for (int i = 1; i < varAttributes.get(var).size(); i++) {
					TupleVar pair = varAttributes.get(var).get(0);
					witnessQuery += pair.getRelation().getName() + "." + pair.getVar() + "="
							+ varAttributes.get(var).get(i).getRelation().getName() + "."
							+ varAttributes.get(var).get(i).getVar() + " AND ";
				}
			}
			if (witnessQuery.endsWith(" WHERE ")) {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 7);
			} else if (witnessQuery.endsWith(" AND ")) {
				witnessQuery = witnessQuery.substring(0, witnessQuery.length() - 5);
			}
			unionQuery = unionQuery + "(" + witnessQuery + ")\n UNION \n";
		}
		if (unionQuery.endsWith("\n UNION \n")) {
			unionQuery = unionQuery.substring(0, unionQuery.length() - 9);
		}
		return unionQuery;
	}
}
