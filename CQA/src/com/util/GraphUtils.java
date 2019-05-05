/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

import com.beans.FuxmanAtom;
import com.beans.FuxmanQuery;
import com.beans.QueryVar;

class Object<T> {
	T obj;

	Object(T obj) {
		this.obj = obj;
	}

	public T getObject() {
		return this.obj;
	}
}

public class GraphUtils {

	/**
	 * @param query
	 * @return Fuxman's join graph as a Map<FuxmanAtom, List<FuxmanAtom>>, in the
	 *         form of an adjacency list
	 */
	public static Map<FuxmanAtom, List<FuxmanAtom>> buildFuxmanJoinGraph(FuxmanQuery q) {
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

	public static Set<FuxmanAtom> getRoots(Map<FuxmanAtom, List<FuxmanAtom>> graph) {
		Set<FuxmanAtom> nonRoots = new HashSet<FuxmanAtom>();
		for (List<FuxmanAtom> value : graph.values()) {
			nonRoots.addAll(value);
		}
		Set<FuxmanAtom> roots = new HashSet<FuxmanAtom>(graph.keySet());
		roots.removeAll(nonRoots);
		return roots;
	}

	public static List<Map<FuxmanAtom, List<FuxmanAtom>>> getConnectedComponents(
			Map<FuxmanAtom, List<FuxmanAtom>> graph) {
		List<Map<FuxmanAtom, List<FuxmanAtom>>> connectedComponents = new ArrayList<Map<FuxmanAtom, List<FuxmanAtom>>>();
		Map<FuxmanAtom, Boolean> visited = new HashMap<FuxmanAtom, Boolean>();
		List<FuxmanAtom> connectedNodes = new ArrayList<FuxmanAtom>();
		for (FuxmanAtom atom : graph.keySet()) {
			if (visited.get(atom) == null)
				visitDFS(graph, atom, visited, connectedNodes);
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

	private static void visitDFS(Map<FuxmanAtom, List<FuxmanAtom>> graph, FuxmanAtom v,
			Map<FuxmanAtom, Boolean> visited, List<FuxmanAtom> connectedNodes) {
		visited.put(v, true);
		connectedNodes.add(v);
		for (FuxmanAtom u : graph.get(v)) {
			if (visited.get(u) == null) {
				visitDFS(graph, u, visited, connectedNodes);
			}
		}
	}

	public static Map<FuxmanAtom, List<FuxmanAtom>> getSubTree(Map<FuxmanAtom, List<FuxmanAtom>> tree,
			FuxmanAtom root) {
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

	public static void drawGraph(Map<FuxmanAtom, List<FuxmanAtom>> toDraw) {
		SingleGraph graph = new SingleGraph("JoinGraph");
		graph.addAttribute("ui.stylesheet", Constants.graphStyleSheetURL);
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

	// Traversal function to check if graph is connected or not
	public static <T> void conTraversal(Map<T, List<T>> graph, T element, List<T> Visited) { // Traversing the map using
																								// dfs
		if (Visited.indexOf(element) == -1) // If the element is not visited add it to the visited array
		{
			Visited.add(element);
			int t = graph.get(element).size(); // Get the number of edges of the element
			for (int i = 0; i < t; i++) {
				conTraversal(graph, graph.get(element).get(i), Visited); // Traverse through the vertices connected to
																			// these edges
			}
		}
	}

	// Overloaded functions for directed and undirected graph to check if graph is
	// cyclic
	// When the graph is undirected
	public static <T> boolean Traversal(Map<T, List<T>> graph, T element, ArrayList<T> Visited) {
		for (T key : graph.keySet()) {
			if (Visited.indexOf(key) == -1) {
				Visited.add(key);
				int t = graph.get(key).size();
				int check = 0;
				for (int i = 0; i < t; i++) {
					if (Visited.indexOf(graph.get(key).get(i)) == -1) {
						check = 1;
						boolean bool = Traversal(graph, graph.get(key).get(i), Visited);
						if (bool)
							return true;
					}
				}
				if (check == 0 && t > 1) {
					return true;
				}
			}
		}
		return false;
	}

	// When the graph is directed
	public static <T> boolean Traversal(Map<T, List<T>> graph, T element, ArrayList<T> Visited, ArrayList<T> Dvisited) {
		if (Visited.indexOf(element) == -1) {
			Visited.add(element);

			int t = graph.get(element).size();
			boolean tmp = false;
			for (int i = 0; i < t; i++) {
				tmp = Traversal(graph, graph.get(element).get(i), Visited, Dvisited);
				if (tmp == true)
					return true;
			}
			if (tmp == false) {
				if (Dvisited.indexOf(element) == -1)
					Dvisited.add(element);
				Visited.remove(Visited.size() - 1);
				return false;
			}

		} else {
			return true;
		}
		return false;
	}

	public static <T> boolean isConnected(Map<T, List<T>> graph) {
		Map<T, List<T>> temp = graph.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		for (T key : temp.keySet()) {
			int t = temp.get(key).size();
			for (int i = 0; i < t; i++) {
				T tempkey = graph.get(key).get(i);
				if (temp.get(tempkey).indexOf(key) == -1)
					temp.get(tempkey).add(key);
			}
		}
		Map.Entry<T, List<T>> entry = graph.entrySet().iterator().next();
		T key = entry.getKey();

		ArrayList<T> Visited = new ArrayList<T>(); // Creating a list of visited elements
		conTraversal(temp, key, Visited);
		if (Visited.size() < graph.size()) // If the final path obtained does not contain all the elements then
			return false; // the graph is disconnected

		return true;
	}

	// isCyclic function to check if graphs are cyclic
	public static <T> boolean isCyclic(Map<T, List<T>> graph, boolean considerDirections) {
		if (considerDirections == true) {
			ArrayList<T> Visited = new ArrayList<T>();
			ArrayList<T> Dvisited = new ArrayList<T>();
			boolean bool;
			for (T key : graph.keySet()) {
				if (Dvisited.indexOf(key) == -1) {
					bool = Traversal(graph, key, Visited, Dvisited);
					if (bool == true)
						return true;
				}
			}
			return false;
		} else {
			Map<T, List<T>> temp = graph.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			for (T key : temp.keySet()) {
				int t = temp.get(key).size();
				for (int i = 0; i < t; i++) {
					T tempkey = graph.get(key).get(i);
					if (temp.get(tempkey).indexOf(key) == -1)
						temp.get(tempkey).add(key);
				}
			}
			ArrayList<T> Visited = new ArrayList<T>();
			for (T key : graph.keySet()) {
				if (Visited.indexOf(key) == -1) {
					boolean f = Traversal(temp, key, Visited);
					if (f == true)
						return true;
				}
			}
			return false;
		}
	}
}
