package com.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Dependency;
import com.beans.Relation;
import com.beans.Schema;

public class ProblemParser {

	public Query parseQueryFromFile(String filePath) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
			String currentLine;
			Query query = new Query();
			int flag = 0;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.isEmpty()) {
					// Skip line
					continue;
				} else if (currentLine.equals("q")) {
					flag = 1;
					continue;
				} else if (currentLine.equals("free")) {
					flag = 2;
					continue;
				}

				if (flag == 1) {
					// Parse atoms
					String[] parts = currentLine.split("\t");
					Atom atom = new Atom(parts[0]);
					for (String attribute : parts[1].split(",")) {
						atom.addAttribute(attribute);
						atom.setAtomIndex(query.getAtomsCountByName(parts[0]) + 1);
					}
					query.addAtom(atom);
				} else if (flag == 2) {
					String[] parts = currentLine.split(",");
					for (String var : parts) {
						query.getFreeVars().add(var);
					}
				}
			}
			return query;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public Schema parseSchema(String filePath) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
			String currentLine;
			Schema schema = new Schema();
			Set<Relation> relations = new HashSet<Relation>();

			int lineStatus = 0;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.isEmpty()) {
					// Skip line
					continue;
				} else if (currentLine.equals("s")) {
					// Schema definition starts
					lineStatus = 1;
					continue;
				} else if (currentLine.equals("fd")) {
					// Functional dependencies definition starts
					lineStatus = 2;
					continue;
				} else if (currentLine.equals("keys")) {
					// Keys definition starts
					lineStatus = 3;
					continue;
				}
				switch (lineStatus) {
				case 1:
					// Parse relation names, and attributes in every relation
					String[] parts = currentLine.split("\t");
					Relation relation = new Relation(parts[0]);
					for (String attributeName : parts[1].split(",")) {
						relation.addAttribute(attributeName);
					}
					relations.add(relation);
					break;
				case 2:
					parts = currentLine.split("\t");
					for (Relation r : relations) {
						if (r.getName().equals(parts[0])) {
							Dependency fd = new Dependency();
							r.setDependency(fd);
							for (String attributeIndex : parts[1].split("->")[0].split(",")) {
								r.addToLeft(Integer.parseInt(attributeIndex));
							}
							for (String attributeIndex : parts[1].split("->")[1].split(",")) {
								r.addToRight(Integer.parseInt(attributeIndex));
							}
							break;
						}
					}
					break;
				case 3:
					parts = currentLine.split("\t");
					for (Relation r : relations) {
						if (r.getName().equals(parts[0])) {
							for (String attributeIndex : parts[1].split(",")) {
								r.addKeyAttribute(Integer.parseInt(attributeIndex));
							}
							break;
						}
					}
					break;
				default:
					break;
				}
			}
			schema.setRelations(relations);
			return schema;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return null;
	}
}