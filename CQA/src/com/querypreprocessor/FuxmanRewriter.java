/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.querypreprocessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

import com.beans.DRCQuery;
import com.beans.DRCQuery.Quant;
import com.beans.DRCQuery.Quantifier;
import com.beans.FOFormula;
import com.beans.FOFormula.Op;
import com.beans.FuxmanAtom;
import com.beans.FuxmanQuery;
import com.beans.Query;
import com.beans.QueryVar;
import com.beans.SQLQuery;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;
import com.util.ProblemParser2;
import com.util.SyntheticDataGenerator3;

public class FuxmanRewriter {
	private DRCQuery q;
	private Map<FuxmanAtom, List<FuxmanAtom>> joinGraph;
	private Connection con;

	public FuxmanRewriter(DRCQuery q, Connection con) {
		super();
		this.q = q;
		this.con = con;
		buildFuxmanJoinGraph();
		System.out.println(areAllNonKeyToKeyJoinsFull());
	}

	public static void main(String[] args) throws SQLException {
		DRCQuery q = new ProblemParser2().parseUCQ1(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt")
				.get(0);
		// q.printElaborate();
		Schema schema = new ProblemParser().parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyschema1.txt");
		SQLQuery query = new ProblemParser2().parseSQLQueriesFromFOSyntax(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt",
				schema).get(0);
		List<Query> uCQ = new ProblemParser().parseUCQ(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt");

		FuxmanRewriter rewriter = new FuxmanRewriter(q, new DBEnvironment().getConnection());
		FuxmanAtom root = rewriter.getRoots(rewriter.joinGraph).iterator().next();
		// rewriter.rewriteConsistent(q);
		// query.print();
		SyntheticDataGenerator3 gen = new SyntheticDataGenerator3();
		//gen.generateThirdColumnValues(100000);
		gen.generateConsistent(rewriter.con, uCQ.get(0), 950000, 0.1, false);
		gen.addInconsistency(rewriter.con, schema, uCQ.get(0), 100000, 2);
		uCQ.get(0).print();
		long start = System.currentTimeMillis();
		rewriter.rewriteJoin(query, schema, root);
		System.out.println("Done in " + (System.currentTimeMillis() - start) + "ms");
	}

	public Set<FuxmanAtom> getRoots(Map<FuxmanAtom, List<FuxmanAtom>> graph) {
		Set<FuxmanAtom> nonRoots = new HashSet<FuxmanAtom>();
		for (List<FuxmanAtom> value : graph.values()) {
			nonRoots.addAll(value);
		}
		Set<FuxmanAtom> roots = new HashSet<FuxmanAtom>(graph.keySet());
		roots.removeAll(nonRoots);
		return roots;
	}

	/**
	 * @param query
	 * @return Fuxman's join graph as a Map<FuxmanAtom, List<FuxmanAtom>>, in the
	 *         form of an adjacency list
	 */
	private Map<FuxmanAtom, List<FuxmanAtom>> buildFuxmanJoinGraph(FuxmanQuery q) {
		Map<FuxmanAtom, List<FuxmanAtom>> fuxmanJoinGraph = new HashMap<FuxmanAtom, List<FuxmanAtom>>();
		for (int i = 0; i < q.getSize(); i++) {
			fuxmanJoinGraph.put(q.getAtomByPosition(i), new ArrayList<FuxmanAtom>());
			for (int j = i + 1; j < q.getSize(); j++) {
				FuxmanAtom r_i = q.getAtoms().get(i);
				FuxmanAtom r_j = q.getAtoms().get(j);
				if (r_i.getName().equalsIgnoreCase(r_j.getName())) {
					System.out.println("Relation symbol " + r_i.getName() + " appears more than once");
					return null;
				}
				List<QueryVar> arcVars = new ArrayList<QueryVar>(r_i.getNonKeyVars());
				arcVars.retainAll(r_j.getVars());
				List<String> arcVarsStrings = new ArrayList<String>();
				for (QueryVar qv : arcVars)
					arcVarsStrings.add(qv.getVarString());
				for (String var : q.getFreeVars()) {
					arcVarsStrings.remove(var);
				}
				if (!arcVarsStrings.isEmpty())
					fuxmanJoinGraph.get(q.getAtomByPosition(i)).add(q.getAtomByPosition(j));
			}
		}
		return fuxmanJoinGraph;
	}

	private void buildFuxmanJoinGraph() {
		this.joinGraph = buildFuxmanJoinGraph(q.getFuxmanQuery());
	}

	public List<Map<FuxmanAtom, List<FuxmanAtom>>> getConnectedComponents(Map<FuxmanAtom, List<FuxmanAtom>> graph) {
		List<Map<FuxmanAtom, List<FuxmanAtom>>> connectedComponents = new ArrayList<Map<FuxmanAtom, List<FuxmanAtom>>>();
		Map<FuxmanAtom, Boolean> visited = new HashMap<FuxmanAtom, Boolean>();
		List<FuxmanAtom> connectedNodes = new ArrayList<FuxmanAtom>();
		for (FuxmanAtom atom : graph.keySet()) {
			if (visited.get(atom) == null)
				visitDFS(atom, visited, connectedNodes);
			Map<FuxmanAtom, List<FuxmanAtom>> component = new HashMap<FuxmanAtom, List<FuxmanAtom>>();
			for (FuxmanAtom j : connectedNodes) {
				component.put(j, new ArrayList<FuxmanAtom>());
				for (FuxmanAtom a : graph.get(j)) {
					if (connectedNodes.contains(a))
						component.get(j).add(a);
				}
			}
			if (!component.isEmpty())
				connectedComponents.add(component);
			connectedNodes.clear();
		}
		return connectedComponents;
	}

	public void visitDFS(FuxmanAtom v, Map<FuxmanAtom, Boolean> visited, List<FuxmanAtom> connectedNodes) {
		visited.put(v, true);
		connectedNodes.add(v);
		for (FuxmanAtom u : joinGraph.get(v)) {
			if (visited.get(u) == null) {
				visitDFS(u, visited, connectedNodes);
			}
		}
	}

	public boolean areAllNonKeyToKeyJoinsFull() {
		return areAllNonKeyToKeyJoinsFull(q.getFuxmanQuery());
	}

	public boolean areAllNonKeyToKeyJoinsFull(FuxmanQuery query) {
		for (int i = 0; i < query.getSize(); i++) {
			for (int j = i + 1; j < query.getSize(); j++) {
				FuxmanAtom r_i = query.getAtoms().get(i);
				FuxmanAtom r_j = query.getAtoms().get(j);
				if (r_i.getName().equalsIgnoreCase(r_j.getName())) {
					System.out.println("Relation symbol " + r_i.getName() + " appears more than once");
					return false;
				}
				List<QueryVar> r_jKeys = new ArrayList<QueryVar>(r_j.getKeyVars());
				r_jKeys.retainAll(r_i.getNonKeyVars());
				if (r_jKeys.isEmpty()) // There's no nonkey-to-key join between r_i and r_j
					continue;
				if (new ArrayList<QueryVar>(r_j.getKeyVars()).retainAll(r_i.getNonKeyVars())) {
					System.out.println("Join between " + r_i.getName() + " and " + r_j.getName() + " is not full");
					return false;
				}
			}
		}
		return true;
	}

	private void computeConsistentAnswers(String candidatesQuery, String filterQuery, String rewriting) {
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
			// ResultSet rs = con.prepareStatement("SELECT column2 FROM
			// FINAL_ANSWERS").executeQuery();
			// rs.close();
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
		System.out.println("Candidates query:");
		candidates.print();
		String filter = "SELECT ";
		filter += String.join(",", Kroot.stream().map(x -> root.getName() + "." + x).collect(Collectors.toList()))
				+ " \n";
		filter += "FROM CANDIDATES C JOIN " + root.getName() + " ON " + String.join(" AND ",
				Kroot.stream().map(x -> "C." + x + " = " + root.getName() + "." + x).collect(Collectors.toList()))
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

		filter += "UNION ALL SELECT " + String.join(",", Kroot.stream().map(x -> "C." + x).collect(Collectors.toList()))
				+ " FROM CANDIDATES C \n";
		filter += "GROUP BY " + String.join(",", Kroot.stream().map(x -> "C." + x).collect(Collectors.toList()))
				+ " \n";
		filter += "HAVING COUNT(*) > 1";
		System.out.println("Filter query:");
		System.out.println(filter);

		String rewriting = "SELECT "
				+ String.join(",", query.getSelect().stream().map(x -> x.split("\\.")[1]).collect(Collectors.toList()))
				+ " \nFROM CANDIDATES C \nWHERE NOT EXISTS \n(SELECT * FROM FILTER F WHERE "
				+ String.join(" AND ", Kroot.stream().map(x -> "C." + x + " = F." + x).collect(Collectors.toList()))
				+ ")";
		System.out.println("\nRewriting:");
		System.out.println(rewriting);
		computeConsistentAnswers("CREATE VIEW CANDIDATES AS (" + candidates.getSQLSyntax() + ")",
				"CREATE VIEW FILTER AS (" + filter + ")", "CREATE TABLE FINAL_ANSWERS AS (" + rewriting + ")");
	}

	private String getLOJ(Map<FuxmanAtom, List<FuxmanAtom>> T, Schema schema) {
		FuxmanAtom R = getRoots(T).iterator().next();
		// System.out.println("Root is " + R.getName());
		if (T.get(R).isEmpty())
			return "";
		Set<String> joinConditions = new HashSet<String>();
		List<Map<FuxmanAtom, List<FuxmanAtom>>> subtrees = new ArrayList<Map<FuxmanAtom, List<FuxmanAtom>>>();
		List<FuxmanAtom> children = new ArrayList<FuxmanAtom>();
		for (FuxmanAtom R_i : T.get(R)) {
			subtrees.add(getSubTree(T, R_i));
			children.add(R_i);
		}
		for (int i = 0; i < subtrees.size(); i++) {
			Map<FuxmanAtom, List<FuxmanAtom>> subtree = subtrees.get(i);
			FuxmanAtom R_i = children.get(i);
			Set<String> equalityConditions = new HashSet<String>();
			for (QueryVar nonKeyVar : R.getNonKeyVars()) {
				for (QueryVar keyVar : getRoots(subtree).iterator().next().getKeyVars()) {
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

	public void rewriteConsistent(DRCQuery dq) {
		System.err.println("Called rewriteConsistent on " + dq.getSyntax());
		FuxmanQuery q = dq.getFuxmanQuery();
		List<DRCQuery> Q_i = new ArrayList<DRCQuery>();
		for (Map<FuxmanAtom, List<FuxmanAtom>> component : getConnectedComponents(joinGraph)) {
			FuxmanAtom R_i = getRoots(component).iterator().next();
			Set<String> x_i = new HashSet<String>();
			Set<String> y_i = new HashSet<String>();
			Set<String> z_i = new HashSet<String>();
			Set<String> w_i = new HashSet<String>();

			for (QueryVar qv : R_i.getKeyVars()) {
				x_i.add(qv.getVarString());
			}

			for (QueryVar qv : R_i.getNonKeyVars()) {
				y_i.add(qv.getVarString());
			}
			for (FuxmanAtom atom : component.keySet()) {
				for (QueryVar qv : atom.getVars()) {
					if (!qv.isConstant() && !x_i.contains(qv.getVarString())) {
						if (q.getFreeVars().contains(qv.getVarString()))
							z_i.add(qv.getVarString());
						else
							w_i.add(qv.getVarString());
					}
				}
			}
			System.out.println("x_i " + x_i);
			System.out.println("y_i " + y_i);
			System.out.println("z_i " + z_i);
			System.out.println("w_i " + w_i);

			String q_i = "(" + Stream.concat(x_i.stream(), z_i.stream()).collect(Collectors.joining(",")) + "):";
			for (FuxmanAtom atom : component.keySet()) {
				q_i += atom.getName() + "(" + atom.getVarsCSV(true) + ");";
			}
			System.out.println(q_i);
			Q_i.add(rewriteTree(new ProblemParser2().parseQueryFromString1(q_i), x_i, y_i, z_i, w_i));
		}
		FOFormula Q_iConjunction = new FOFormula();
		for (DRCQuery Q : Q_i) {
			if (Q_iConjunction.getLeft() == null) {
				Q_iConjunction.setLeft(Q);
				continue;
			}
			if (Q_iConjunction.getRight() == null) {
				Q_iConjunction.setOp(Op.AND);
				Q_iConjunction.setRight(Q);
				continue;
			}
			Q_iConjunction = new FOFormula(Q, Q_iConjunction, Op.AND);
			// Q.printElaborate();
		}
		DRCQuery rewriting = new DRCQuery();
		rewriting.setFormula(new FOFormula(dq.getFormula(), Q_iConjunction, Op.AND));
		rewriting.setQuantifiers(dq.getQuantifiers());
		System.out.println("REWRITING:");
		rewriting.printElaborate();
	}

	public DRCQuery rewriteTree(DRCQuery dq, Set<String> xBar, Set<String> yBar, Set<String> zBar, Set<String> wBar) {
		System.err.println("Called rewriteTree on " + dq.getSyntax() + " with ");
		System.err.println("xBar " + xBar + ", " + "yBar " + yBar + ", " + "zBar " + zBar + ", " + "wBar " + wBar);
		List<DRCQuery> Q_iQueries = new ArrayList<DRCQuery>();
		DRCQuery Q = null;
		FuxmanQuery q = dq.getFuxmanQuery();
		Map<FuxmanAtom, List<FuxmanAtom>> T = buildFuxmanJoinGraph(q);
		FuxmanAtom R = getRoots(T).iterator().next();

		DRCQuery qLocal = new ProblemParser2().parseQueryFromString1(
				"(" + Stream.concat(xBar.stream(), zBar.stream()).collect(Collectors.joining(",")) + "):" + R.getName()
						+ "(" + R.getVarsCSV(true) + ")");
		DRCQuery QLocal = rewriteLocal(qLocal);
		if (q.getAtoms().size() == 1) {
			Q = QLocal;
		} else {
			// Iterating through children of R in T
			for (FuxmanAtom R_i : T.get(R)) {
				Set<String> x_i = new HashSet<String>(
						R_i.getKeyVars().stream().map(QueryVar::getVarString).collect(Collectors.toSet()));
				Set<String> y_i = new HashSet<String>(
						R_i.getNonKeyVars().stream().map(QueryVar::getVarString).collect(Collectors.toSet()));
				Map<FuxmanAtom, List<FuxmanAtom>> T_i = getSubTree(T, R_i);

				Set<String> w_i = new HashSet<String>();
				Set<String> z_i = new HashSet<String>();

				for (FuxmanAtom atom : T_i.keySet()) {
					for (QueryVar qv : atom.getVars()) {
						if (!qv.isConstant() && !x_i.contains(qv.getVarString())) {
							if (q.getFreeVars().contains(qv.getVarString()))
								z_i.add(qv.getVarString());
							else
								w_i.add(qv.getVarString());
						}
					}
				}

				String q_i = "(" + Stream.concat(x_i.stream(), z_i.stream()).collect(Collectors.joining(",")) + "):";
				for (FuxmanAtom atom : T_i.keySet()) {
					q_i += atom.getName() + "(" + atom.getVarsCSV(true) + ");";
				}
				Q_iQueries.add(rewriteTree(new ProblemParser2().parseQueryFromString1(q_i), x_i, y_i, z_i, w_i));
			}
			Set<String> y0 = new HashSet<String>();
			y0.addAll(yBar);
			y0.retainAll(wBar);
			y0.removeAll(xBar);

			FOFormula Q_iConjunction = FOFormula.getConjunction(Q_iQueries, DRCQuery.class);
			Q_iConjunction.print();
			DRCQuery rightQuery = new DRCQuery();
			for (String var : y0) {
				Quantifier qu = rightQuery.new Quantifier(Quant.FORALL, var);
				if (!rightQuery.getQuantifiers().contains(qu))
					rightQuery.getQuantifiers().add(qu);
			}
			rightQuery.setFormula(new FOFormula(R, Q_iConjunction, Op.IMPLIES));
			// rightQuery.getFormula().eliminateImplication();
			// rightQuery.eliminateUniversalQuantification();
			Q = new DRCQuery();
			Q.setFormula(new FOFormula(QLocal, rightQuery, Op.AND));
		}
		System.err.print("Returning ");
		Q.printElaborate();
		System.err.println(" from rewriteTree of " + dq.getSyntax());
		return Q;
	}

	public DRCQuery rewriteLocal(DRCQuery dq) {
		System.err.println("Called rewriteLocal of " + dq.getSyntax());
		String z = "", zPrime = "";
		DRCQuery QLocal = null;
		FuxmanQuery q = dq.getFuxmanQuery();
		List<FOFormula> eq = new ArrayList<FOFormula>();
		FuxmanAtom R = q.getAtoms().get(0);
		List<String> zBar = q.getFreeVars();
		List<QueryVar> xBar = R.getKeyVars();
		List<QueryVar> yBar = R.getNonKeyVars();
		for (int p = 0; p < yBar.size(); p++) {
			QueryVar w = yBar.get(p);
			z = "newVar" + p;
			if (w.isConstant()) {
				System.out.println("Add equality (" + z + "=" + "'" + w.getVarString() + "')");
				eq.add(new FOFormula(new FOFormula(z), new FOFormula("'" + w.getVarString() + "'"), Op.EQUALS));
			}
			if (xBar.contains(w) || zBar.contains(w.getVarString())) {
				System.out.println("Add equality (" + z + "=" + w.getVarString() + ")");
				eq.add(new FOFormula(new FOFormula(z), new FOFormula(w.getVarString()), Op.EQUALS));
			}
			for (int pPrime = 0; pPrime < yBar.size(); pPrime++) {
				if (p != pPrime && yBar.get(pPrime).equals(w)) {
					zPrime = "newVar" + pPrime;
					System.out.println("Add equality (" + z + "=" + zPrime + ")");
					eq.add(new FOFormula(new FOFormula(z), new FOFormula(zPrime), Op.EQUALS));
				}
			}
		}
		if (eq.isEmpty()) {
			System.err.println("Returning ");
			dq.printElaborate();
			System.err.println(" from rewriteLocal of " + dq.getSyntax());
			return dq;
		} else {
			List<String> yStar = new ArrayList<String>();
			for (int p = 0; p < yBar.size(); p++)
				yStar.add(p, "newVar" + p);

			QLocal = new DRCQuery();
			DRCQuery rightQuery = new DRCQuery();
			for (String yStar_i : yStar) {
				Quantifier qu = rightQuery.new Quantifier(Quant.FORALL, yStar_i);
				if (!rightQuery.getQuantifiers().contains(qu))
					rightQuery.getQuantifiers().add(qu);
			}
			String atomString = R.getName() + "(", prefix = "";
			for (QueryVar qv : R.getVars()) {
				if (qv.isKey()) {
					atomString += prefix + qv.getVarString(true);
				} else {
					atomString += prefix + "newVar" + yBar.indexOf(qv);
				}
				prefix = ",";
			}
			atomString += ")";
			rightQuery.setFormula(new FOFormula(new ProblemParser2().parseAtomFromString(atomString),
					FOFormula.getConjunction(eq, FOFormula.class), Op.IMPLIES));
			// rightQuery.getFormula().eliminateImplication();
			// rightQuery.eliminateUniversalQuantification();
			DRCQuery leftQuery = new DRCQuery();
			leftQuery.setQuantifiers(dq.getQuantifiers());
			leftQuery.setFormula(R);
			QLocal.setFormula(new FOFormula(leftQuery, rightQuery, Op.AND));
			QLocal.setQuantifiers(dq.getQuantifiers());
		}
		System.err.println("Returning ");
		QLocal.printElaborate();
		System.err.println(" from rewriteLocal of " + dq.getSyntax());
		return QLocal;
	}

	private Map<FuxmanAtom, List<FuxmanAtom>> getSubTree(Map<FuxmanAtom, List<FuxmanAtom>> tree, FuxmanAtom root) {
		Map<FuxmanAtom, List<FuxmanAtom>> subtree = new HashMap<FuxmanAtom, List<FuxmanAtom>>();
		Queue<FuxmanAtom> queue = new LinkedList<FuxmanAtom>();
		queue.add(root);
		subtree.put(root, tree.get(root));
		while (!queue.isEmpty()) {
			FuxmanAtom popped = queue.poll();
			if (popped != null) {
				for (FuxmanAtom child : tree.get(popped)) {
					queue.add(child);
					subtree.put(child, tree.get(child));
				}
			}
		}
		return subtree;
	}

	public void drawGraph(Map<FuxmanAtom, List<FuxmanAtom>> toDraw) {
		SingleGraph graph = new SingleGraph("JoinGraph");
		graph.addAttribute("ui.stylesheet",
				"url('C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\stylesheet.css')");
		for (FuxmanAtom atom : toDraw.keySet()) {
			Node node = graph.addNode(atom.getName());
			node.addAttribute("ui.label", atom.getName());
		}
		for (FuxmanAtom atom : toDraw.keySet()) {
			for (FuxmanAtom neighbour : toDraw.get(atom)) {
				graph.addEdge("E" + atom.getName() + "," + neighbour.getName(), atom.getName(), neighbour.getName(),
						true);
			}
		}
		System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		graph.display();
	}
}
