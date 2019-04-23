/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
