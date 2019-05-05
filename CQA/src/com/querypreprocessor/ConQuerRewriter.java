/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.querypreprocessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.beans.FuxmanAtom;
import com.beans.FuxmanQuery;
import com.beans.QueryVar;
import com.beans.SQLQuery;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.GraphUtils;
import com.util.ProblemParser;
import com.util.ProblemParser2;

public class ConQuerRewriter {
	private Map<FuxmanAtom, List<FuxmanAtom>> joinGraph;
	private Connection con;

	public ConQuerRewriter(FuxmanQuery q, Connection con) {
		super();
		this.con = con;
		this.joinGraph = GraphUtils.buildFuxmanJoinGraph(q);
	}

	public static void main(String[] args) throws SQLException {
		FuxmanQuery q = new ProblemParser2().parseUCQ1(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt")
				.get(0).getFuxmanQuery();
		Schema schema = new ProblemParser().parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyschema1.txt");
		SQLQuery query = new ProblemParser2().parseSQLQueryFromFOSyntax(q.getSyntax(), schema);

		ConQuerRewriter rewriter = new ConQuerRewriter(q, new DBEnvironment().getConnection());
		FuxmanAtom root = GraphUtils.getRoots(rewriter.joinGraph).iterator().next();

		long start = System.currentTimeMillis();
		rewriter.rewriteJoin(query, schema, root);
		System.out.println("Done in " + (System.currentTimeMillis() - start) + "ms");
	}

	public void computeConsistentAnswers(String candidatesQuery, String filterQuery, String rewriting) {
		try {
			long s = System.currentTimeMillis();
			con.prepareStatement("DROP VIEW IF EXISTS CANDIDATES CASCADE").execute();
			con.prepareStatement(candidatesQuery).executeUpdate();
			System.out.println(System.currentTimeMillis() - s);
			con.prepareStatement("DROP VIEW IF EXISTS FILTER CASCADE").execute();
			con.prepareStatement(filterQuery).executeUpdate();
			System.out.println(System.currentTimeMillis() - s);
			con.prepareStatement("DROP TABLE IF EXISTS FINAL_ANSWERS").execute();
			con.prepareStatement(rewriting).executeUpdate();
			System.out.println(System.currentTimeMillis() - s);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void rewriteJoin(SQLQuery query, Schema schema, FuxmanAtom root) {
		List<String> Kroot = schema.getRelationByName(root.getName()).getKeyAttributesList();
		SQLQuery candidates = new SQLQuery(query);
		candidates.setSelectDistinct(true);
		for (String keyAttribute : Kroot) {
			if (!candidates.getSelect().contains(root.getName() + "." + keyAttribute)) {
				candidates.getSelect().add(root.getName() + "." + keyAttribute);
			}
		}
		for (int i = 0; i < candidates.getSelect().size(); i++) {
			candidates.getSelect().set(i,
					candidates.getSelect().get(i) + " AS " + candidates.getSelect().get(i).replaceAll("\\.", "_"));
		}
		System.out.println("Candidates query:");
		candidates.print();
		String filter = "SELECT ";
		filter += String.join(",", Kroot.stream().map(x -> root.getName() + "_" + x).collect(Collectors.toList()))
				+ " \n";
		filter += "FROM CANDIDATES C JOIN " + root.getName() + " ON "
				+ String.join(" AND ",
						Kroot.stream().map(x -> "C." + root.getName() + "_" + x + " = " + root.getName() + "." + x)
								.collect(Collectors.toList()))
				+ " \n";
		filter += "LEFT OUTER JOIN " + getLOJ(joinGraph, schema) + " \n";
		List<String> whereConditions = new ArrayList<String>();
		whereConditions.addAll(
				query.getKj().stream().map(object -> Objects.toString(object, null)).collect(Collectors.toList()));
		List<String> nullChecks = new ArrayList<String>();
		for (int i = 1; i < query.getFrom().size(); i++) {
			String relationName = query.getFrom().get(i);
			List<String> K_i = schema.getRelationByName(relationName).getKeyAttributesList();
			nullChecks.add(String.join(" AND ",
					K_i.stream().map(x -> relationName + "." + x + " IS NULL").collect(Collectors.toList())));
		}
		whereConditions.add("(" + String.join(" OR ", nullChecks) + ")");
		filter += "WHERE " + String.join(" AND ", whereConditions) + " \n";

		filter += "UNION ALL SELECT "
				+ String.join(",",
						Kroot.stream().map(x -> "C." + root.getName() + "_" + x).collect(Collectors.toList()))
				+ " FROM CANDIDATES C \n";
		filter += "GROUP BY " + String.join(",",
				Kroot.stream().map(x -> "C." + root.getName() + "_" + x).collect(Collectors.toList())) + " \n";
		filter += "HAVING COUNT(*) > 1";
		System.out.println("Filter query:");
		System.out.println(filter);

		String rewriting = "SELECT "
				+ String.join(",",
						query.getSelect().stream().map(x -> x.replaceAll("\\.", "_")).collect(Collectors.toList()))
				+ " \nFROM CANDIDATES C \nWHERE NOT EXISTS \n(SELECT * FROM FILTER F WHERE "
				+ String.join(" AND ",
						Kroot.stream().map(x -> "C." + root.getName() + "_" + x + " = F." + root.getName() + "_" + x)
								.collect(Collectors.toList()))
				+ ")";
		System.out.println("\nRewriting:");
		System.out.println(rewriting);
		computeConsistentAnswers("CREATE VIEW CANDIDATES AS (" + candidates.getSQLSyntax() + ")",
				"CREATE VIEW FILTER AS (" + filter + ")", "CREATE TABLE FINAL_ANSWERS AS (" + rewriting + ")");
	}

	private String getLOJ(Map<FuxmanAtom, List<FuxmanAtom>> T, Schema schema) {
		FuxmanAtom R = GraphUtils.getRoots(T).iterator().next();
		if (T.get(R).isEmpty())
			return "";
		Set<String> joinConditions = new HashSet<String>();
		List<Map<FuxmanAtom, List<FuxmanAtom>>> subtrees = new ArrayList<Map<FuxmanAtom, List<FuxmanAtom>>>();
		List<FuxmanAtom> children = new ArrayList<FuxmanAtom>();
		for (FuxmanAtom R_i : T.get(R)) {
			subtrees.add(GraphUtils.getSubTree(T, R_i));
			children.add(R_i);
		}
		for (int i = 0; i < subtrees.size(); i++) {
			Map<FuxmanAtom, List<FuxmanAtom>> subtree = subtrees.get(i);
			FuxmanAtom R_i = children.get(i);
			Set<String> equalityConditions = new HashSet<String>();
			for (QueryVar nonKeyVar : R.getNonKeyVars()) {
				for (QueryVar keyVar : GraphUtils.getRoots(subtree).iterator().next().getKeyVars()) {
					if (nonKeyVar.getVarString().equals(keyVar.getVarString())) {
						equalityConditions.add(R.getName() + "."
								+ schema.getRelationByName(R.getName()).getAttributes()
										.get(R.getVars().indexOf(nonKeyVar))
								+ " = " + R_i.getName() + "." + schema.getRelationByName(R_i.getName()).getAttributes()
										.get(R_i.getVars().indexOf(keyVar)));
					}
				}
			}
			joinConditions.add(R_i.getName() + " ON " + String.join(" AND ", equalityConditions));
		}
		for (Map<FuxmanAtom, List<FuxmanAtom>> subtree : subtrees) {
			joinConditions.add(getLOJ(subtree, schema));
		}
		joinConditions.removeAll(Arrays.asList("", null));
		return String.join(" LEFT OUTER JOIN ", joinConditions);
	}
}
