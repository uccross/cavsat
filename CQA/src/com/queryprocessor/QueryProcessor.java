package com.queryprocessor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.graphstream.algorithm.Dijkstra;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.Path;
import org.graphstream.graph.implementations.SingleGraph;

import com.beans.Atom;
import com.beans.AtomFD;
import com.beans.ClosureVars;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;
import com.util.ProblemParser;

public class QueryProcessor {

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();
		String qfile = "C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\toyquery3.txt";
		String sfile = "C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\toyschema3.txt";
		Query query = new QueryProcessor().addKeysToAtoms(pp.parseQueryFromFile(qfile), pp.parseSchema(sfile));
		query.print();
		List<AtomFD> K = new ArrayList<AtomFD>();
		for (Atom atom : query.getAtoms()) {
			AtomFD atomFD = new AtomFD(atom.getName(), atom.getKeyAttributes(), atom.getAttributes());
			K.add(atomFD);
		}
		for (AtomFD atomfd : K)
			atomfd.print();
		System.out.println("-------------------------------------------------------------");
		List<ClosureVars> closures = new ArrayList<ClosureVars>();
		for (Atom atom : query.getAtoms()) {
			ClosureVars closure = new ClosureVars(atom.getName());
			boolean closureStillIncomplete = true;
			for (AtomFD atomFD : K) {
				if (atomFD.getAtomName().equalsIgnoreCase(atom.getName())) {
					closure.addVars(new HashSet<String>(atomFD.getLeft()));
					break;
				}
			}
			while (closureStillIncomplete) {
				closureStillIncomplete = false;
				for (AtomFD atomFD : K) {
					if (atomFD.getAtomName().equalsIgnoreCase(atom.getName()))
						continue;
					if (closure.getVars().containsAll(atomFD.getLeft())
							&& !closure.getVars().containsAll(atomFD.getRight())) {
						closure.addVars(new HashSet<String>(atomFD.getRight()));
						closureStillIncomplete = true;
					}
				}
			}
			closures.add(closure);
		}
		for (ClosureVars closure : closures) {
			closure.print();
		}
		QueryProcessor queryProcessor = new QueryProcessor();
		SingleGraph intersectionTree = queryProcessor.drawIntersectionTree(query);
		SingleGraph attackGraph = queryProcessor.drawAttackGraph(query, intersectionTree, closures);
		System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		intersectionTree.display();
		attackGraph.display();
	}

	// Works only for self-join-free queries
	private Query addKeysToAtoms(Query query, Schema schema) {
		for (Relation relation : schema.getRelations()) {
			if (query.getAtomByName(relation.getName()) != null) {
				for (int keyIndex : relation.getKeyAttributes()) {
					query.getAtomByName(relation.getName())
							.addKeyAttribute(query.getAtomByName(relation.getName()).getAttributeByIndex(keyIndex));
				}
			}
		}
		return query;
	}

	public SingleGraph drawIntersectionTree(Query query) {
		SingleGraph graph = new SingleGraph("Graph");
		Atom previous = null;
		for (Atom atom : query.getAtoms()) {
			Node node = graph.addNode(atom.getName() + atom.getAtomIndex());
			node.addAttribute("ui.label", atom.getNameWithAttributes());
			if (previous != null) {
				Edge e = graph.addEdge(
						previous.getName() + previous.getAtomIndex() + atom.getName() + atom.getAtomIndex(),
						previous.getName() + previous.getAtomIndex(), atom.getName() + atom.getAtomIndex(), false);
				e.addAttribute("ui.label", previous.getSharedVars(atom).toString());
				e.addAttribute("label", previous.getSharedVars(atom));
			}
			previous = atom;
		}
		graph.addAttribute("ui.stylesheet",
				"url('C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\stylesheet.css')");
		return graph;
	}

	public SingleGraph drawAttackGraph(Query query, SingleGraph intersectionTree, List<ClosureVars> closures) {
		SingleGraph graph = new SingleGraph("Graph");
		for (Atom atom : query.getAtoms()) {
			Node node = graph.addNode(atom.getName() + atom.getAtomIndex());
			node.addAttribute("ui.label", atom.getNameWithAttributes());
		}

		Dijkstra dijkstra = new Dijkstra(Dijkstra.Element.EDGE, null, null);
		dijkstra.init(intersectionTree);

		for (Atom atom1 : query.getAtoms()) {
			dijkstra.setSource(intersectionTree.getNode(atom1.getName() + atom1.getAtomIndex()));
			dijkstra.compute();
			for (Atom atom2 : query.getAtoms()) {
				if (atom1.equals(atom2))
					continue;
				Path path = dijkstra.getPath(intersectionTree.getNode(atom2.getName() + atom2.getAtomIndex()));
				ClosureVars F = null;
				for (ClosureVars closure : closures) {
					if (closure.getAtomName().equalsIgnoreCase(atom1.getName()))
						F = closure;
					// else if (closure.getAtomName().equalsIgnoreCase(atom2.getName()))
					// G = closure;
				}
				boolean edgeFG = true;
				for (Edge edge : path.getEdgeSet()) {
					if (F.getVars().containsAll(edge.getAttribute("label"))) {
						// no edge from F to G
						//if (((List<String>) edge.getAttribute("label")).size() != 0)
							edgeFG = false;
						break;
					}
				}
				if (edgeFG) {
					graph.addEdge(atom1.getName() + atom1.getAtomIndex() + atom2.getName() + atom2.getAtomIndex(),
							atom1.getName() + atom1.getAtomIndex(), atom2.getName() + atom2.getAtomIndex(), true);
				}
			}
		}
		graph.addAttribute("ui.stylesheet",
				"url('C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\stylesheet.css')");
		return graph;
	}

	public void tp() {
		SingleGraph graph = new SingleGraph("Use");
		graph.addNode("A");
		graph.addNode("B");
		graph.addNode("C");
		graph.addNode("D");
		graph.addNode("E");
		graph.addNode("F");
		graph.addEdge("AB", "A", "B", true);
		graph.addEdge("BA", "B", "A", true);
		graph.addEdge("BC", "B", "C", true);
		graph.addEdge("CA", "C", "A", true);
		graph.addEdge("CD", "C", "D", true);
		graph.addEdge("DF", "D", "F", true);
		graph.addEdge("EF", "E", "F", true);
		graph.addEdge("DE", "D", "E", true);
		graph.addAttribute("ui.stylesheet",
				"url('C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\stylesheet.css')");
		System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		graph.display();
		findShortestPath(graph);
	}

	public void findShortestPath(SingleGraph g) {
		Dijkstra dijkstra = new Dijkstra(Dijkstra.Element.EDGE, null, null);

		// Compute the shortest paths in g from A to all nodes
		dijkstra.init(g);
		dijkstra.setSource(g.getNode("A"));
		dijkstra.compute();

		// Print the lengths of all the shortest paths
		for (Node node : g)
			System.out.printf("%s->%s:%10.2f%n", dijkstra.getSource(), node, dijkstra.getPathLength(node));

		// Print the shortest path from A to B
		System.out.println(dijkstra.getPath(g.getNode("F")));
	}
}
