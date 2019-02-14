package com.querypreprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

import com.beans.Atom;
import com.beans.Query;

public class AttackGraphBuilder {

	private Query query;
	private int n;
	private int m;
	private List<List<String>> key;
	private List<List<String>> nonkey;
	private List<Integer> keyCount;
	private List<Integer> count;
	private List<List<Atom>> keyco;
	private List<List<Atom>> co;
	private List<List<Atom>> witnessf;
	private boolean[][] attack;
	private boolean FO;

	public AttackGraphBuilder(Query query) {
		super();
		this.query = query;
		this.n = query.getAtoms().size();
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
		System.out.println("KEYS: "+key);
		System.out.println("NON-KEYS: "+nonkey);
		this.keyCount = new ArrayList<Integer>();
		this.count = new ArrayList<Integer>();
		this.keyco = new ArrayList<List<Atom>>();
		this.co = new ArrayList<List<Atom>>();
		this.witnessf = new ArrayList<List<Atom>>();
		this.attack = new boolean[n][n];
	}

	public void topologicalSortUtil(int v, boolean visited[], Stack<Integer> stack) {
		visited[v] = true;
		for (int j = 0; j < n; j++) {
			if (attack[v][j] && !visited[j])
				topologicalSortUtil(j, visited, stack);
		}
		stack.push(v);
	}

	public List<Atom> topologicalSort() {
		List<Atom> sortedAtoms = new ArrayList<Atom>();
		Stack<Integer> stack = new Stack<Integer>();
		boolean visited[] = new boolean[n];
		for (int i = 0; i < n; i++)
			visited[i] = false;
		for (int i = 0; i < n; i++)
			if (!visited[i])
				topologicalSortUtil(i, visited, stack);
		while (!stack.isEmpty())
			sortedAtoms.add(query.getAtoms().get(stack.pop()));
		return sortedAtoms;
	}

	public boolean isQueryFO() {
		initialize();
		buildAttackGraph();
		print();
		drawAttackGraph();
		return this.FO;
	}

	public void buildAttackGraph() {
		for (int f = 0; f < n; f++) {
			Set<String> fplus = new HashSet<String>(this.key.get(f));
			for (int g = 0; g < n; g++) {
				this.witnessf.add(new ArrayList<Atom>());
				this.count.add(g, this.keyCount.get(g));
				if (this.keyCount.get(g) == 0 && g != f) {
					fplus.addAll(this.nonkey.get(g));
				}
			}
			this.count.set(f, m + 1);
			Set<String> update = new HashSet<String>(fplus);
			while (!update.isEmpty()) {
				String u = update.iterator().next();
				update.remove(u);
				for (Atom g : this.keyco.get(this.query.getPositionOfVar(u))) {
					count.set(query.getPositionOfAtom(g), count.get(query.getPositionOfAtom(g)) - 1);
					if (count.get(query.getPositionOfAtom(g)) == 0) {
						List<String> add = new ArrayList<String>(nonkey.get(query.getPositionOfAtom(g)));
						add.removeAll(fplus);
						fplus.addAll(add);
						update.addAll(add);
					}
				}
			}
			Set<String> uSlashFPlus = new HashSet<String>(query.getAllVars());
			uSlashFPlus.removeAll(fplus);
			uSlashFPlus.removeAll(query.getFreeVars());
			System.out.println("fplus " + f + " " + fplus + " " + uSlashFPlus);
			for (String u : uSlashFPlus) {
				List<Atom> GList = co.get(query.getPositionOfVar(u));
				Collections.sort(GList, (o1, o2) -> query.getPositionOfAtom(o1) - (query.getPositionOfAtom(o2)));
				witnessf.get(query.getPositionOfAtom(GList.get(GList.size() - 1))).add(GList.get(0));
				for (int i = 0; i < GList.size() - 1; i++) {
					witnessf.get(query.getPositionOfAtom(GList.get(i))).add(GList.get(i + 1));
				}
			}
			for (int g = 0; g < n; g++) {
				if (g != f) {
					if (isReachable(query.getAtomByPosition(f), query.getAtomByPosition(g))) {
						attack[f][g] = true;
						if (attack[g][f])
							FO = false;
					}
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
		for (int u = 0; u < m; u++) {
			this.co.add(new ArrayList<Atom>());
			this.keyco.add(new ArrayList<Atom>());
		}
		for (int f = 0; f < n; f++) {
			for (String u : this.key.get(f)) {
				this.keyco.get(this.query.getPositionOfVar(u)).add(query.getAtomByPosition(f));
			}
			this.keyCount.add(f, this.key.get(f).size());
			for (String u : new HashSet<String>(query.getAtomByPosition(f).getVars())) {
				this.co.get(this.query.getPositionOfVar(u)).add(query.getAtomByPosition(f));
			}
			for (int g = 0; g < n; g++) {
				this.attack[f][g] = false;
			}
		}
		this.FO = true;
	}

	public void print() {
		this.query.print();
		System.out.println(this.n + " atoms, " + this.m + " variables");
		for (int i = 0; i < n; i++) {
			System.out.println("Atom " + i + ", keys: " + this.key.get(i) + ", nonkeys: " + this.nonkey.get(i));
		}
		for (int i = 0; i < m; i++) {
			System.out.println("co[" + query.getVarByPosition(i) + "]: " + this.co.get(i));
			for (Atom atom : co.get(i)) {
				System.out.print(atom.getName() + " ");
			}
			System.out.println();
		}
		for (int i = 0; i < m; i++) {
			System.out.println("keyco[" + query.getVarByPosition(i) + "]: " + this.keyco.get(i));
		}
	}

	private void drawAttackGraph() {
		SingleGraph graph = new SingleGraph("AttackGraph");
		graph.addAttribute("ui.stylesheet",
				"url('C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\stylesheet.css')");
		for (Atom atom : query.getAtoms()) {
			Node node = graph.addNode(atom.getName() + atom.getVars());
			node.addAttribute("ui.label", atom.getName() + atom.getVars());
		}
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (attack[i][j]) {
					Atom atom1 = query.getAtomByPosition(i);
					Atom atom2 = query.getAtomByPosition(j);
					try {
						Edge edge = graph.addEdge("E" + i + "," + j, atom1.getName() + atom1.getVars(),
								atom2.getName() + atom2.getVars(), true);
						edge.addAttribute("ui.label", "E" + i + "," + j);
					} catch (Exception e) {

					}
				}
			}
		}
		System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		graph.display();
	}
}