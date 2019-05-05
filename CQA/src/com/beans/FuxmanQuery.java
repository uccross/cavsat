/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FuxmanQuery {
	private String syntax;
	private List<FuxmanAtom> atoms;
	private List<String> freeVars;

	public FuxmanQuery() {
		this.atoms = new ArrayList<FuxmanAtom>();
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

	public List<FuxmanAtom> getAtoms() {
		return atoms;
	}

	public void setAtoms(List<FuxmanAtom> atoms) {
		this.atoms = atoms;
	}

	public void addAtom(FuxmanAtom atom) {
		this.atoms.add(atom);
	}

	public List<String> getFreeVars() {
		return freeVars;
	}

	public FuxmanAtom getAtomByPosition(int index) {
		return this.atoms.get(index);
	}

	public int getPositionOfAtom(FuxmanAtom atom) {
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
		for (FuxmanAtom a : this.atoms) {
			atomsCSV += a.getName() + ",";
		}
		return atomsCSV.substring(0, atomsCSV.length() - 1);
	}

	public Set<String> getParticipatingRelationNames() {
		Set<String> relationNames = new HashSet<String>();
		for (FuxmanAtom atom : this.atoms) {
			relationNames.add(atom.getName());
		}
		return relationNames;
	}

	public Set<String> getAllVars() {
		Set<String> allVars = new HashSet<String>();
		for (FuxmanAtom atom : this.atoms) {
			for (QueryVar var : atom.getVars()) {
				if (!var.isConstant())
					allVars.add(var.getVarString());
			}
		}
		return allVars;
	}

	public Set<String> getAllConstants() {
		Set<String> allConstants = new HashSet<String>();
		for (FuxmanAtom atom : this.atoms) {
			for (QueryVar var : atom.getVars()) {
				if (var.isConstant())
					allConstants.add(var.getVarString());
			}
		}
		return allConstants;
	}

	public int getAtomsCountByName(String name) {
		int count = 0;
		for (FuxmanAtom a : getAtoms()) {
			if (a.getName().equals(name))
				count++;
		}
		return count;
	}

	/**
	 * @param atomName
	 * @return First atom of the matching name, otherwise null
	 */
	public FuxmanAtom getAtomByName(String atomName) {
		for (FuxmanAtom atom : this.atoms) {
			if (atom.getName().equalsIgnoreCase(atomName))
				return atom;
		}
		return null;
	}

	public void print() {
		System.out.println(this.syntax);
	}
}
