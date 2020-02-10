/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.beans.Atom;
import com.beans.DenialConstraint;
import com.beans.Expression;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class ProblemParser {

	public List<Query> parseUCQ(File file) {
		BufferedReader br = null;
		List<Query> uCQ = new ArrayList<Query>();
		try {
			br = new BufferedReader(new FileReader(file));
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.startsWith("%") || currentLine.isEmpty())
					continue;
				System.out.println(currentLine);
				Query query = new Query();
				query.setSyntax(currentLine);
				String parts[] = currentLine.split(":");
				String head = parts[0];
				String body = parts[1];
				for (String s : head.replaceAll("\\(", "").replaceAll("\\)", "").split(",")) {
					if (!query.getFreeVars().contains(s))
						query.getFreeVars().add(s);
				}
				for (String s : body.split(";")) {
					query.addAtom(parseAtom(s));
				}
				uCQ.add(query);
			}
			br.close();
			return uCQ;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<Query> parseUCQ(String filePath) {
		return parseUCQ(new File(filePath));
	}

	public Schema parseSchema(String filePath) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
			String currentLine;
			Schema schema = new Schema();
			Set<Relation> relations = new HashSet<Relation>();
			int lineStatus = 0, constraintID = 0;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.isEmpty()) {
					continue;
				} else if (currentLine.equals("s")) {
					lineStatus = 1;
					continue;
				} else if (currentLine.equals("dc")) {
					lineStatus = 2;
					continue;
				} else if (currentLine.equals("keys")) {
					lineStatus = 3;
					continue;
				}
				String[] parts = null;
				Relation relation = null;
				DenialConstraint dc = null;
				switch (lineStatus) {
				case 1:
					parts = currentLine.split("\t");
					relation = new Relation(parts[0]);
					for (String attributeName : parts[1].split(",")) {
						relation.addAttribute(attributeName);
					}
					relations.add(relation);
					break;
				case 2:
					dc = new DenialConstraint(constraintID++);
					parts = currentLine.split(";");
					for (String s : parts) {
						if (Arrays.stream(Constants.ops).parallel().anyMatch(s::contains))
							dc.getExpressions().add(parseExpression(s));
						else
							dc.getAtoms().add(parseAtom(s));
					}
					schema.getConstraints().add(dc);
					break;
				case 3:
					parts = currentLine.split("\t");
					for (Relation r : relations) {
						if (r.getName().equals(parts[0])) {
							relation = r;
							break;
						}
					}
					for (String attributeIndex : parts[1].split(",")) {
						relation.addKeyAttribute(Integer.parseInt(attributeIndex));
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

	private Expression parseExpression(String expStr) {
		String[] parts;
		for (String op : Constants.ops) {
			if (expStr.contains(op)) {
				parts = expStr.split(op);
				Expression expObj = new Expression(parts[0], parts[1], op);
				return expObj;
			}
		}
		return null;
	}

	private VarProperties checkVarProperties(String var) {
		boolean isKey = false, isConstant = false;
		String v = var;
		if (v.startsWith("{") && v.endsWith("}")) {
			v = v.replaceAll("\\{", "").replaceAll("\\}", "");
			isKey = true;
		}
		if (v.startsWith("'") && v.endsWith("'")) {
			v = v.replaceAll("'", "");
			isConstant = true;
		}
		return new VarProperties(v, isKey, isConstant);
	}

	private Atom parseAtom(String atomStr) {
		String parts[] = atomStr.split("\\(");
		Atom atom = new Atom(parts[0]);
		parts = parts[1].replaceAll("\\)", "").split(",");
		for (String s : parts) {
			VarProperties p = checkVarProperties(s);
			if (p.isKey) {
				atom.addKeyVar(p.var);
			} else {
				atom.addNonKeyVar(p.var);
			}
			if (p.isConstant)
				atom.getConstants().add(p.var);
			atom.addVar(p.var);
		}
		return atom;
	}

	private class VarProperties {
		private String var;
		private boolean isKey;
		private boolean isConstant;

		public VarProperties(String var, boolean isKey, boolean isConstant) {
			super();
			this.var = var;
			this.isKey = isKey;
			this.isConstant = isConstant;
		}
	}

}
