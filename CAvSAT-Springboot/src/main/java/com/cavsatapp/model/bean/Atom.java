/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.cavsatapp.model.bean;

import java.util.ArrayList;
import java.util.List;

public class Atom {
	private String name;
	// private int atomIndex;
	private List<String> vars;
	private List<String> constants;
	private List<String> keyVars;
	private List<String> nonKeyVars;

	@Override
	public boolean equals(Object arg0) {
		if (!(arg0 instanceof Atom)) {
			return false;
		} else {
			return ((Atom) arg0).getName().equals(this.name) && ((Atom) arg0).getVars().equals(this.vars);
		}
	}

	@Override
	public String toString() {
		return this.name;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	public Atom(String name) {
		super();
		this.name = name;
		this.vars = new ArrayList<String>();
		this.constants = new ArrayList<String>();
		this.keyVars = new ArrayList<String>();
		this.nonKeyVars = new ArrayList<String>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/*
	 * public int getAtomIndex() { return atomIndex; }
	 * 
	 * public void setAtomIndex(int atomIndex) { this.atomIndex = atomIndex; }
	 */

	public List<String> getVars() {
		return vars;
	}

	public void setVars(List<String> vars) {
		this.vars = vars;
	}

	public void addVar(String var) {
		this.vars.add(var);
	}

	public List<String> getConstants() {
		return constants;
	}

	public void setConstants(List<String> constants) {
		this.constants = constants;
	}

	public List<String> getKeyVars() {
		return keyVars;
	}

	public void setNonKeyVars(List<String> nonKeyVars) {
		this.nonKeyVars = nonKeyVars;
	}

	public List<String> getNonKeyVars() {
		return nonKeyVars;
	}

	public void setKeyVars(List<String> keyVars) {
		this.keyVars = keyVars;
	}

	public void addKeyVar(String var) {
		this.keyVars.add(var);
	}

	public void addNonKeyVar(String var) {
		this.nonKeyVars.add(var);
	}

	public String getVarByIndex(int index) {
		return this.vars.get(index - 1); // List indexes start from 0
	}

	public String getVarsCSV() {
		String result = "";
		for (String var : getVars()) {
			result += var + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getKeyVarsCSV() {
		String result = "";
		for (String var : getKeyVars()) {
			result += var + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getNameWithVars() {
		return this.name + "(" + getVarsCSV() + ")";
	}

	public List<String> getSharedVars(Atom atom) {
		List<String> sharedVars = new ArrayList<String>();
		for (String var : this.vars) {
			if (atom.getVars().contains(var))
				sharedVars.add(var);
		}
		return sharedVars;
	}
}
