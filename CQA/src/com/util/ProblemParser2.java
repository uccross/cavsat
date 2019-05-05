/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beans.DRCQuery;
import com.beans.DRCQuery.Quant;
import com.beans.DRCQuery.Quantifier;
import com.beans.Expression;
import com.beans.FOFormula;
import com.beans.FOFormula.Op;
import com.beans.FuxmanAtom;
import com.beans.FuxmanQuery;
import com.beans.QueryVar;
import com.beans.SQLQuery;
import com.beans.Schema;

public class ProblemParser2 {

	/**
	 * @param file
	 *            containing one conjunctive query per line, written using the
	 *            syntax (w):R1({x},y);R2({y},z);R3({z},w);R4({y},'a')
	 * @return
	 */

	public SQLQuery parseSQLQuery(BufferedReader br) {
		try {
			SQLQuery query = new SQLQuery();
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.startsWith("%") || currentLine.isEmpty())
					continue;
				String[] parts;
				if (currentLine.startsWith("SELECT")) {
					if (currentLine.contains("DISTINCT")) {
						currentLine = currentLine.replaceAll("DISTINCT", "");
						query.setSelectDistinct(true);
					}
					parts = currentLine.replaceAll("SELECT ", "").replaceAll(" ", "").split(",");
					query.getSelect().addAll(Arrays.asList(parts));

				} else if (currentLine.startsWith("FROM")) {
					parts = currentLine.replaceAll("FROM ", "").replaceAll(" ", "").split(",");
					query.getFrom().addAll(Arrays.asList(parts));
				} else if (currentLine.startsWith("WHERE")) {
					parts = currentLine.replaceAll("WHERE ", "").split("AND");
					String[] subparts;
					for (int i = 0; i < parts.length; i++) {
						subparts = parts[i].split(",");
						for (String s : subparts) {
							if (!s.trim().isEmpty())
								switch (i) {
								case 0:
									query.getKj().add(parseExpression(s.trim()));
									break;
								case 1:
									query.getNkj().add(parseExpression(s.trim()));
									break;
								case 2:
									query.getSc().add(parseExpression(s.trim()));
									break;
								}
						}
					}
				} else if (currentLine.startsWith("ORDER BY")) {
					parts = currentLine.replaceAll("ORDER BY ", "").replaceAll(" ", "").split(",");
					query.getOrderby().addAll(Arrays.asList(parts));
				} else if (currentLine.startsWith("GROUP BY")) {
					parts = currentLine.replaceAll("GROUP BY ", "").replaceAll(" ", "").split(",");
					query.getGroupby().addAll(Arrays.asList(parts));
				}
			}
			return query;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
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

	public List<DRCQuery> parseUCQ1(BufferedReader br) {
		List<DRCQuery> uCQ = new ArrayList<DRCQuery>();
		try {
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.startsWith("%") || currentLine.isEmpty())
					continue;
				DRCQuery query = new DRCQuery();
				query.setSyntax(currentLine);
				String parts[] = currentLine.split(":"); // part[0] is head, parts[1] is body
				FOFormula main = new FOFormula();
				String[] atomsString = parts[1].split(";");
				for (int i = 0; i < atomsString.length; i++) {
					String s = atomsString[i];
					String atomParts[] = s.split("\\(");
					FuxmanAtom atom = new FuxmanAtom(atomParts[0]);
					atomParts = atomParts[1].replaceAll("\\)", "").split(",");
					for (String atomPart : atomParts) {
						VarProperties p = checkVarProperties(atomPart);

						boolean isAtomPartFreeVar = Arrays
								.asList(parts[0].replaceAll("\\(", "").replaceAll("\\)", "").split(","))
								.contains(p.var);
						Quantifier qu = query.new Quantifier(Quant.EXISTS, p.var);
						if (!isAtomPartFreeVar && !p.isConstant && !query.getQuantifiers().contains(qu)) {
							query.getQuantifiers().add(qu);
						}
						QueryVar queryVar = new QueryVar(p.var, p.isKey, p.isConstant, !isAtomPartFreeVar);
						atom.addVar(queryVar);

						if (p.isKey && !atom.getKeyVars().contains(queryVar))
							atom.getKeyVars().add(queryVar);
						else if (!p.isKey && !atom.getNonKeyVars().contains(queryVar))
							atom.getNonKeyVars().add(queryVar);
						if (p.isConstant && !atom.getConstants().contains(queryVar))
							atom.getConstants().add(queryVar);
					}
					if (main.getLeft() == null) {
						main.setLeft(atom);
						continue;
					}
					if (main.getRight() == null) {
						main.setOp(Op.AND);
						main.setRight(atom);
						continue;
					}
					main = new FOFormula(atom, main, Op.AND);
				}
				query.setFormula(main);
				uCQ.add(query);
			}
			br.close();
			return uCQ;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public FuxmanAtom parseAtomFromString(String atomString) {
		String atomParts[] = atomString.split("\\(");
		FuxmanAtom atom = new FuxmanAtom(atomParts[0]);
		atomParts = atomParts[1].replaceAll("\\)", "").split(",");
		for (String atomPart : atomParts) {
			VarProperties p = checkVarProperties(atomPart);
			QueryVar queryVar = new QueryVar(p.var, p.isKey, p.isConstant, false);
			atom.addVar(queryVar);

			if (p.isKey && !atom.getKeyVars().contains(queryVar))
				atom.getKeyVars().add(queryVar);
			else if (!p.isKey && !atom.getNonKeyVars().contains(queryVar))
				atom.getNonKeyVars().add(queryVar);
			if (p.isConstant && !atom.getConstants().contains(queryVar))
				atom.getConstants().add(queryVar);
		}
		return atom;
	}

	private List<FuxmanQuery> parseUCQ(BufferedReader br) {
		List<FuxmanQuery> uCQ = new ArrayList<FuxmanQuery>();
		try {
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				if (currentLine.startsWith("%") || currentLine.isEmpty())
					continue;
				FuxmanQuery query = new FuxmanQuery();
				query.setSyntax(currentLine);
				String parts[] = currentLine.split(":"); // part[0] is head, parts[1] is body
				for (String s : parts[1].split(";")) {
					String atomParts[] = s.split("\\(");
					FuxmanAtom atom = new FuxmanAtom(atomParts[0]);
					atomParts = atomParts[1].replaceAll("\\)", "").split(",");
					for (String atomPart : atomParts) {
						VarProperties p = checkVarProperties(atomPart);

						boolean isAtomPartFreeVar = Arrays
								.asList(parts[0].replaceAll("\\(", "").replaceAll("\\)", "").split(","))
								.contains(p.var);

						QueryVar queryVar = new QueryVar(p.var, p.isKey, p.isConstant, !isAtomPartFreeVar);
						atom.addVar(queryVar);

						if (p.isKey && !atom.getKeyVars().contains(queryVar))
							atom.getKeyVars().add(queryVar);
						else if (!p.isKey && !atom.getNonKeyVars().contains(queryVar))
							atom.getNonKeyVars().add(queryVar);
						if (p.isConstant && !atom.getConstants().contains(queryVar))
							atom.getConstants().add(queryVar);
						if (isAtomPartFreeVar && !query.getFreeVars().contains(queryVar.getVarString()))
							query.getFreeVars().add(queryVar.getVarString());
					}
					query.addAtom(atom);
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

	private List<SQLQuery> parseSQLQueriesFromFOSyntax(BufferedReader br, Schema schema) {
		List<SQLQuery> uCQ = new ArrayList<SQLQuery>();
		try {
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
				Map<String, Boolean> isAttributeKey = new HashMap<String, Boolean>();
				if (currentLine.startsWith("%") || currentLine.isEmpty())
					continue;
				SQLQuery query = new SQLQuery();
				String parts[] = currentLine.split(":"); // part[0] is head, parts[1] is body
				for (String s : parts[1].split(";")) {
					List<String> atomParts = Arrays.asList(s.split("\\("));
					String atomName = atomParts.get(0);
					// Filling FROM clause
					query.getFrom().add(atomName);
					atomParts = Arrays.asList(atomParts.get(1).replaceAll("\\)", "").split(","));
					for (String atomPart : atomParts) {
						VarProperties p = checkVarProperties(atomPart);
						boolean isAtomPartFreeVar = Arrays
								.asList(parts[0].replaceAll("\\(", "").replaceAll("\\)", "").split(","))
								.contains(p.var);
						String attribute = atomName + "."
								+ schema.getRelationByName(atomName).getAttributes().get(atomParts.indexOf(atomPart));
						if (p.isConstant)
							attribute = "'" + p.var + "'";
						if (!varAttrMap.keySet().contains(p.var))
							varAttrMap.put(p.var, new ArrayList<String>());

						if (!varAttrMap.get(p.var).contains(attribute))
							varAttrMap.get(p.var).add(attribute);
						isAttributeKey.put(attribute, p.isKey);
						// Filling SELECT clause
						if (isAtomPartFreeVar) {
							query.getSelect().add(attribute);
						}
					}
				}
				// Filling WHERE clause
				for (String var : varAttrMap.keySet()) {
					if (varAttrMap.get(var).size() > 1) {
						for (int i = 1; i < varAttrMap.get(var).size(); i++) {
							Expression exp = new Expression(varAttrMap.get(var).get(0), varAttrMap.get(var).get(i),
									"=");
							if (isAttributeKey.get(varAttrMap.get(var).get(0))
									&& isAttributeKey.get(varAttrMap.get(var).get(i)))
								query.getKj().add(exp);
							else if (!isAttributeKey.get(varAttrMap.get(var).get(0))
									&& !isAttributeKey.get(varAttrMap.get(var).get(i))) {
								System.err.println("Nonkey to nonkey join found");
								return null;
							} else {
								query.getNkj().add(exp);
							}
						}
					}
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

	public List<SQLQuery> parseSQLQueriesFromFOSyntax(File file, Schema schema) {
		try {
			return parseSQLQueriesFromFOSyntax(new BufferedReader(new FileReader(file)), schema);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<SQLQuery> parseSQLQueriesFromFOSyntax(String filepath, Schema schema) {
		return parseSQLQueriesFromFOSyntax(new File(filepath), schema);
	}

	public SQLQuery parseSQLQuery(File file) {
		try {
			return parseSQLQuery(new BufferedReader(new FileReader(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public SQLQuery parseSQLQuery(String filepath) {
		return parseSQLQuery(new File(filepath));
	}

	public List<DRCQuery> parseUCQ1(File file) {
		try {
			return parseUCQ1(new BufferedReader(new FileReader(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public SQLQuery parseSQLQueryFromFOSyntax(String query, Schema schema) {
		return parseSQLQueriesFromFOSyntax(new BufferedReader(new StringReader(query)), schema).get(0);
	}

	public List<DRCQuery> parseUCQ1(String filepath) {
		return parseUCQ1(new File(filepath));
	}

	public DRCQuery parseQueryFromString1(String query) {
		return parseUCQ1(new BufferedReader(new StringReader(query))).get(0);
	}

	public List<FuxmanQuery> parseUCQ(File file) {
		try {
			return parseUCQ(new BufferedReader(new FileReader(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<FuxmanQuery> parseUCQ(String filepath) {
		return parseUCQ(new File(filepath));
	}

	public FuxmanQuery parseQueryFromString(String query) {
		return parseUCQ(new BufferedReader(new StringReader(query))).get(0);
	}

	private VarProperties checkVarProperties(String var) {
		boolean isKey = false, isConstant = false;
		String v = var.replaceAll(" ", "");
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

	private class VarProperties {
		private String var;
		private boolean isKey;
		private boolean isConstant;

		private VarProperties(String var, boolean isKey, boolean isConstant) {
			super();
			this.var = var;
			this.isKey = isKey;
			this.isConstant = isConstant;
		}
	}
}
