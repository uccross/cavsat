/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cavsat.model.bean.Atom;
import edu.cavsat.model.bean.Graph;
import edu.cavsat.model.bean.Link;
import edu.cavsat.model.bean.Node;
import edu.cavsat.model.bean.Query;
import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.util.Constants;

public class QueryAnalyser {

	private Query query;
	private int n;
	private int m;
	private List<List<String>> key;
	private List<List<String>> nonkey;
	private int[] keyCount;
	private int[] count1; // for fplus
	private int[] count2; // for fsquare plus
	private List<List<Atom>> keyco;
	private List<List<Atom>> co;
	private List<List<Atom>> witnessf;
	private short[][] attack;
	private short[][] joinGraph;
	private int dataComplexity;

	public String analyseQuery(Query query, SQLQuery sqlQuery, Schema schema) throws IOException {
		long start = System.currentTimeMillis();
		this.query = query;
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();

		if (!isSelfJoinFree()) {
			this.dataComplexity = Constants.UNKNOWN;
			node.put("dataComplexity", getDataComplexity());
			node.put("dataComplexityDescription", getDataComplexityDescription());
			node.put("complexityAnalysisTime", System.currentTimeMillis() - start);
			return mapper.writeValueAsString(node);
		}
		this.n = query.getSize();
		this.joinGraph = new short[n][n];
		buildJoinGraph();

		if (sqlQuery.isAggregate()) {
			if (isCForest()) {
				this.dataComplexity = Constants.CFOREST;
				node.put("conquerRewriting",
						new ConQuerRewriter().rewriteAggSQL(sqlQuery, schema, getJoinGraphAdjMtrx()));
			} else
				this.dataComplexity = Constants.UNKNOWN;
			node.put("complexityAnalysisTime", System.currentTimeMillis() - start);
			node.put("dataComplexity", getDataComplexity());
			node.put("dataComplexityDescription", getDataComplexityDescription());
			return mapper.writeValueAsString(node);
		}

		this.m = query.getAllVars().size();
		this.key = new ArrayList<List<String>>();
		this.nonkey = new ArrayList<List<String>>();
		List<Atom> atoms = query.getAtoms();
		for (Atom atom : atoms) {
			key.add(atoms.indexOf(atom), atom.getKeyVars());
			nonkey.add(atoms.indexOf(atom), atom.getNonKeyVars());

			key.get(atoms.indexOf(atom)).removeAll(atom.getConstants());
			nonkey.get(atoms.indexOf(atom)).removeAll(atom.getConstants());
			// Treating free variables as constants while building attack graph
			key.get(atoms.indexOf(atom)).removeAll(query.getFreeVars());
			nonkey.get(atoms.indexOf(atom)).removeAll(query.getFreeVars());
		}
		this.keyCount = new int[n];
		this.count1 = new int[n];
		this.count2 = new int[n];
		this.keyco = new ArrayList<List<Atom>>();
		this.co = new ArrayList<List<Atom>>();
		this.witnessf = new ArrayList<List<Atom>>();
		this.attack = new short[n][n];

		initialize();
		buildAttackGraph();
		this.dataComplexity = findDataComplexity();

		node.putPOJO("attackGraph", getAttackGraph());
		node.putPOJO("joinGraph", getJoinGraph());
		node.put("dataComplexity", getDataComplexity());
		node.put("dataComplexityDescription", getDataComplexityDescription());
		if (getDataComplexity() == Constants.CFOREST) {
			node.put("conquerRewriting",
					new ConQuerRewriter().rewriteForestSQL(sqlQuery, schema, getJoinGraphAdjMtrx()));
			node.putPOJO("kwRewriting",
					mapper.readValue(new KWRewriter().getCertainRewriting(query, schema), ObjectNode.class));
		} else if (getDataComplexity() == Constants.FO_REWRITABLE_BUT_NOT_CFOREST) {
			node.putPOJO("kwRewriting",
					mapper.readValue(new KWRewriter().getCertainRewriting(query, schema), ObjectNode.class));
		}
		node.put("complexityAnalysisTime", System.currentTimeMillis() - start);
		return mapper.writeValueAsString(node);
	}

	public String getDataComplexityDescription() {
		switch (dataComplexity) {
		case Constants.CFOREST:
			return "In C-Forest";
		case Constants.FO_REWRITABLE_BUT_NOT_CFOREST:
			return "SQL-rewritable, but not in C-Forest";
		case Constants.P_BUT_NOT_FO_REWRITABLE:
			return "Polynomial-time computable, but not SQL-rewritable";
		case Constants.CONPCOMPLETE:
			return "CoNP-complete";
		case Constants.UNKNOWN:
			return "Unknown";
		default:
			return "Unknown";
		}
	}

	public int getDataComplexity() {
		return this.dataComplexity;
	}

	private void buildJoinGraph() {
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (i != j) {
					Atom r_i = query.getAtoms().get(i);
					Atom r_j = query.getAtoms().get(j);
					Set<String> arcVars = new HashSet<String>(r_i.getNonKeyVars());
					arcVars.retainAll(r_j.getVars());
					arcVars.removeAll(query.getFreeVars());
					if (!arcVars.isEmpty())
						joinGraph[i][j] = 1;
				}
			}
		}
	}

	private void buildAttackGraph() {
		for (int f = 0; f < n; f++) {
			Set<String> fplus = new HashSet<String>(this.key.get(f));
			Set<String> fSquareplus = new HashSet<String>(this.key.get(f));
			for (int g = 0; g < n; g++) {
				this.witnessf.add(new ArrayList<Atom>());
				this.count1[g] = this.keyCount[g];
				this.count2[g] = this.keyCount[g];
				if (this.keyCount[g] == 0 && g != f) {
					fplus.addAll(this.nonkey.get(g));
				}
			}
			this.count1[f] = m + 1;
			Set<String> update = new HashSet<String>(fplus);
			Set<String> updateSquare = new HashSet<String>(fSquareplus);
			while (!update.isEmpty()) {
				String u = update.iterator().next();
				update.remove(u);
				for (Atom g : this.keyco.get(this.query.getPositionOfVar(u))) {
					count1[query.getPositionOfAtom(g)]--;
					if (count1[query.getPositionOfAtom(g)] == 0) {
						List<String> add = new ArrayList<String>(nonkey.get(query.getPositionOfAtom(g)));
						add.removeAll(fplus);
						fplus.addAll(add);
						update.addAll(add);
					}
				}
			}
			while (!updateSquare.isEmpty()) {
				String u = updateSquare.iterator().next();
				updateSquare.remove(u);
				for (Atom g : this.keyco.get(this.query.getPositionOfVar(u))) {
					count2[query.getPositionOfAtom(g)]--;
					if (count2[query.getPositionOfAtom(g)] == 0) {
						List<String> add = new ArrayList<String>(nonkey.get(query.getPositionOfAtom(g)));
						add.removeAll(fSquareplus);
						fSquareplus.addAll(add);
						updateSquare.addAll(add);
					}
				}
			}
			System.out.println("FPlus of " + query.getAtoms().get(f) + "{"
					+ fplus.stream().collect(Collectors.joining(",")) + "}");
			System.out.println();
			Set<String> uSlashFPlus = new HashSet<String>(query.getAllVars());
			uSlashFPlus.removeAll(fplus);
			uSlashFPlus.removeAll(query.getFreeVars());
			for (String u : uSlashFPlus) {
				List<Atom> GList = co.get(query.getPositionOfVar(u));
				Collections.sort(GList, (o1, o2) -> query.getPositionOfAtom(o1) - (query.getPositionOfAtom(o2)));
				witnessf.get(query.getPositionOfAtom(GList.get(GList.size() - 1))).add(GList.get(0));
				for (int i = 0; i < GList.size() - 1; i++) {
					witnessf.get(query.getPositionOfAtom(GList.get(i))).add(GList.get(i + 1));
				}
			}
			for (int g = 0; g < n; g++) {
				if (g != f && isReachable(query.getAtomByPosition(f), query.getAtomByPosition(g))) {
					if (count2[g] == 0)
						attack[f][g] = 1;
					else
						attack[f][g] = 2;
				}
			}
			this.witnessf.clear();
		}
	}

	private boolean isReachable(Atom source, Atom destination) {
		boolean visited[] = new boolean[n];
		List<Atom> queue = new ArrayList<Atom>();
		visited[query.getPositionOfAtom(source)] = true;
		queue.add(source);
		Iterator<Atom> i;
		while (queue.size() != 0) {
			source = queue.remove(0);
			Atom a;
			i = witnessf.get(query.getPositionOfAtom(source)).iterator();
			while (i.hasNext()) {
				a = i.next();
				if (query.getPositionOfAtom(a) == query.getPositionOfAtom(destination))
					return true;
				if (!visited[query.getPositionOfAtom(a)]) {
					visited[query.getPositionOfAtom(a)] = true;
					queue.add(a);
				}
			}
		}
		return false;
	}

	private void initialize() {
		// Loops through the variables appearing in q, encoded as 0, 1, ... m - 1
		for (int u = 0; u < m; u++) {
			this.co.add(new ArrayList<Atom>());
			this.keyco.add(new ArrayList<Atom>());
		}
		// Loops through the atoms of q. This is essentially same as looping through
		// K(q), because K(q) contains one FD per atom
		for (int f = 0; f < n; f++) {
			for (String u : this.key.get(f)) {
				this.keyco.get(this.query.getPositionOfVar(u)).add(query.getAtomByPosition(f));
			}
			this.keyCount[f] = this.key.get(f).size();
			for (String u : new HashSet<String>(query.getAtomByPosition(f).getVars())) {
				this.co.get(this.query.getPositionOfVar(u)).add(query.getAtomByPosition(f));
			}
			for (int g = 0; g < n; g++) {
				this.attack[f][g] = 0;
			}
		}
	}

	public void print() {
		System.out.print("Data Complexity: ");
		switch (this.dataComplexity) {
		case Constants.CFOREST:
			System.out.println("C-forest");
			break;
		case Constants.FO_REWRITABLE_BUT_NOT_CFOREST:
			System.out.println("FO-rewritable but not C-forest");
			break;
		case Constants.P_BUT_NOT_FO_REWRITABLE:
			System.out.println("P but not FO-rewritable");
			break;
		case Constants.CONPCOMPLETE:
			System.out.println("CoNP-complete");
			break;
		case Constants.UNKNOWN:
			System.out.println("Unknown due to the presence of one or more self-joins");
			return;
		}
		System.out.println("Attack Graph: ");
		for (int i = 0; i < attack.length; i++)
			System.out.print("\t" + query.getAtomByPosition(i));
		System.out.println();
		for (int i = 0; i < attack.length; i++) {
			System.out.print(query.getAtomByPosition(i));
			for (int j = 0; j < attack.length; j++)
				System.out.print("\t" + attack[i][j]);
			System.out.println();
		}
		System.out.println("Join Graph: ");
		for (int i = 0; i < joinGraph.length; i++)
			System.out.print("\t" + query.getAtomByPosition(i));
		System.out.println();
		for (int i = 0; i < joinGraph.length; i++) {
			System.out.print(query.getAtomByPosition(i));
			for (int j = 0; j < joinGraph.length; j++)
				System.out.print("\t" + joinGraph[i][j]);
			System.out.println();
		}
	}

	public Graph getAttackGraph() {
		Graph graph = new Graph(new HashSet<Node>(), new HashSet<Link>());
		for (Atom atom : query.getAtoms()) {
			graph.getNodes().add(new Node(atom.getName(), 1));
		}
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (attack[i][j] > 0) {
					Atom atom1 = query.getAtomByPosition(i);
					Atom atom2 = query.getAtomByPosition(j);
					graph.getLinks().add(new Link(atom1.getName(), atom2.getName(), attack[i][j]));
				}
			}
		}
		return graph;
	}

	public Graph getJoinGraph() {
		Graph graph = new Graph(new HashSet<Node>(), new HashSet<Link>());
		for (Atom atom : query.getAtoms()) {
			graph.getNodes().add(new Node(atom.getName(), 1));
		}
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (joinGraph[i][j] > 0) {
					Atom atom1 = query.getAtomByPosition(i);
					Atom atom2 = query.getAtomByPosition(j);
					graph.getLinks().add(new Link(atom1.getName(), atom2.getName(), 1));
				}
			}
		}
		return graph;
	}

	public short[][] getJoinGraphAdjMtrx() {
		return this.joinGraph;
	}

	private int findDataComplexity() {
		if (!isSelfJoinFree())
			return Constants.UNKNOWN;
		if (isCForest())
			return Constants.CFOREST;
		boolean weakCycle = false;
		for (int i = 0; i < attack.length; i++) {
			for (int j = 0; j < attack.length; j++) {
				if (i != j) {
					if (attack[i][j] == 2 && attack[j][i] == 2)
						return Constants.CONPCOMPLETE;
					else if (attack[i][j] > 0 && attack[j][i] > 0)
						weakCycle = true;
				}
			}
		}
		return weakCycle ? Constants.P_BUT_NOT_FO_REWRITABLE : Constants.FO_REWRITABLE_BUT_NOT_CFOREST;
	}

	private boolean isSelfJoinFree() {
		Set<String> s = new HashSet<String>();
		for (Atom atom : query.getAtoms()) {
			if (!s.add(atom.getName()))
				return false;
		}
		return true;
	}

	private boolean isCForest() {
		return areAllNonKeyToKeyJoinsFull() && isJoinGraphAcyclic();
	}

	private boolean isJoinGraphAcyclic() {
		boolean[] visited = new boolean[n];
		boolean[] stack = new boolean[n];
		for (short i = 0; i < n; i++)
			if (isCyclicHelper(i, visited, stack))
				return false;
		return true;
	}

	private boolean isCyclicHelper(short i, boolean[] visited, boolean[] stack) {
		if (stack[i])
			return true;
		if (stack[i])
			return false;
		visited[i] = true;
		stack[i] = true;
		for (short j = 0; j < n; j++)
			if (joinGraph[i][j] > 0)
				if (isCyclicHelper(j, visited, stack))
					return true;
		stack[i] = false;
		return false;
	}

	private boolean areAllNonKeyToKeyJoinsFull() {
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (i != j) {
					Atom r_i = query.getAtoms().get(i);
					Atom r_j = query.getAtoms().get(j);
					Set<String> r_jKeys = new HashSet<String>(r_j.getKeyVars());
					r_jKeys.retainAll(r_i.getNonKeyVars());
					if (r_jKeys.isEmpty()) // There's no nonkey-to-key join between r_i and r_j
						continue;
					r_jKeys = new HashSet<String>(r_j.getKeyVars());
					if (!r_i.getNonKeyVars().containsAll(r_jKeys))
						// There's a nonkey-to-key join between r_i and r_j, but it is not full
						return false;
				}
			}
		}
		return true;
	}
}
