/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Query {
	private String syntax;
	private List<Atom> atoms;
	private List<String> freeVars;

	public Query() {
		super();
		this.atoms = new ArrayList<Atom>();
		this.freeVars = new ArrayList<String>();
	}

	public String getSyntax() {
		return syntax;
	}

	public void setSyntax(String syntax) {
		this.syntax = syntax;
	}

	public int getSize() {
		return this.atoms.size();
	}

	public boolean isBoolean() {
		for (String var : this.freeVars) {
			if (!var.isEmpty()) {
				return false;
			}
		}
		return true;
	}

	public List<Atom> getAtoms() {
		return atoms;
	}

	public void setAtoms(List<Atom> atoms) {
		this.atoms = atoms;
	}

	public void addAtom(Atom atom) {
		this.atoms.add(atom);
	}

	public List<String> getFreeVars() {
		return freeVars;
	}

	public Atom getAtomByPosition(int index) {
		return this.atoms.get(index);
	}

	public int getPositionOfAtom(Atom atom) {
		return this.atoms.indexOf(atom);
	}

	public String getFreeVarsCSV() {
		String output = "";
		for (String var : freeVars) {
			output += var + ",";
		}
		if (!output.isEmpty()) {
			output = output.substring(0, output.length() - 1);
		}
		return output;
	}

	public void setFreeVars(List<String> freeVars) {
		this.freeVars = freeVars;
	}

	public String getAtomNamesCSV() {
		String atomsCSV = "";
		for (Atom a : this.atoms) {
			atomsCSV += a.getName() + ",";
		}
		return atomsCSV.substring(0, atomsCSV.length() - 1);
	}

	public Set<String> getParticipatingRelationNames() {
		Set<String> relationNames = new HashSet<String>();
		for (Atom atom : this.atoms) {
			relationNames.add(atom.getName());
		}
		return relationNames;
	}

	public List<String> getAllVars() {
		List<String> allVars = new ArrayList<String>();
		for (Atom atom : this.atoms) {
			for (String var : atom.getVars()) {
				if (!allVars.contains(var))
					allVars.add(var);
			}
		}
		return allVars;
	}

	public String getVarByPosition(int index) {
		return getAllVars().get(index);
	}

	public int getPositionOfVar(String var) {
		return getAllVars().indexOf(var);
	}

	public int getAtomsCountByName(String name) {
		int count = 0;
		for (Atom a : getAtoms()) {
			if (a.getName().equals(name))
				count++;
		}
		return count;
	}

	/**
	 * @param atomName
	 * @return First atom of the matching name, otherwise null
	 */
	public Atom getAtomByName(String atomName) {
		for (Atom atom : this.atoms) {
			if (atom.getName().equalsIgnoreCase(atomName))
				return atom;
		}
		return null;
	}

	public String getAttributeFromVar(Schema schema, Atom atom, String var, int varIndex) {
		if (atom != null) {
			return atom.getName() + "." + schema.getRelationByName(atom.getName()).getAttributes().get(varIndex);
		}
		for (Atom curAtom : this.getAtoms()) {
			if (curAtom.getVars().contains(var)) {
				return getAttributeFromVar(schema, curAtom, var, curAtom.getVars().indexOf(var));
			}
		}
		return null;
	}

	public String getSQL(Schema schema) {
		String sqlQuery = "SELECT ";
		Set<String> whereConditions = new HashSet<String>();
		if (this.isBoolean())
			sqlQuery += "TRUE";
		else {
			sqlQuery += this.freeVars.stream().map(var -> getAttributeFromVar(schema, null, var, -1))
					.collect(Collectors.joining(","));
		}
		sqlQuery += " FROM ";
		sqlQuery += this.getAtoms().stream().map(atom -> atom.getName()).collect(Collectors.joining(","));

		Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
		for (Atom atom : this.getAtoms()) {
			Relation relation = schema.getRelationByName(atom.getName());
			for (int i = 0; i < atom.getVars().size(); i++) {
				String var = atom.getVars().get(i);
				if (varAttrMap.containsKey(var)) {
					varAttrMap.get(var).add(relation.getName() + "." + relation.getAttributes().get(i));
				} else {
					List<String> list = new ArrayList<String>();
					list.add(relation.getName() + "." + relation.getAttributes().get(i));
					varAttrMap.put(var, list);
				}
				if (atom.getConstants().contains(var))
					varAttrMap.get(var).add(var);
			}
		}
		for (String var : varAttrMap.keySet()) {
			if (varAttrMap.get(var).size() > 1) {
				String first = varAttrMap.get(var).get(0);
				for (int i = 1; i < varAttrMap.get(var).size(); i++) {
					whereConditions.add(first + "=" + varAttrMap.get(var).get(i));
				}
			}
		}
		if (!whereConditions.isEmpty())
			sqlQuery += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		return sqlQuery;
	}

	public void print() {
		System.out.println(this.syntax);
	}
}
