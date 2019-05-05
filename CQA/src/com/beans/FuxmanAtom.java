/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FuxmanAtom extends FOFormula {
	private String name;
	private List<QueryVar> vars;
	private List<QueryVar> constants;
	private List<QueryVar> keyVars;
	private List<QueryVar> nonKeyVars;

	@Override
	public boolean equals(Object arg0) {
		if (!(arg0 instanceof FuxmanAtom)) {
			return false;
		} else {
			return ((FuxmanAtom) arg0).getName().equals(this.name) && ((FuxmanAtom) arg0).getVars().equals(this.vars);
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

	public FuxmanAtom(String name) {
		super();
		this.name = name;
		this.vars = new ArrayList<QueryVar>();
		this.constants = new ArrayList<QueryVar>();
		this.keyVars = new ArrayList<QueryVar>();
		this.nonKeyVars = new ArrayList<QueryVar>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<QueryVar> getVars() {
		return vars;
	}

	public void setVars(List<QueryVar> vars) {
		this.vars = vars;
	}

	public void addVar(QueryVar var) {
		this.vars.add(var);
	}

	public List<QueryVar> getConstants() {
		return constants;
	}

	public void setConstants(List<QueryVar> constants) {
		this.constants = constants;
	}

	public List<QueryVar> getKeyVars() {
		return keyVars;
	}

	public void setNonKeyVars(List<QueryVar> nonKeyVars) {
		this.nonKeyVars = nonKeyVars;
	}

	public List<QueryVar> getNonKeyVars() {
		return nonKeyVars;
	}

	public void setKeyVars(List<QueryVar> keyVars) {
		this.keyVars = keyVars;
	}

	public void addKeyVar(QueryVar var) {
		this.keyVars.add(var);
	}

	public void addNonKeyVar(QueryVar var) {
		this.nonKeyVars.add(var);
	}

	public QueryVar getVarByIndex(int index) {
		return this.vars.get(index - 1); // List indexes start from 0
	}

	public Set<String> getExistential() {
		Set<String> result = new HashSet<String>();
		for (QueryVar var : getVars()) {
			if (var.isExistential())
				result.add(var.getVarString());
		}
		return result;
	}

	public String getVarsCSV() {
		return getVarsCSV(false);
	}

	public String getVarsCSV(boolean withSyntax) {
		String result = "";
		for (QueryVar var : getVars()) {
			result += var.getVarString(withSyntax) + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getKeyVarsCSV() {
		String result = "";
		for (QueryVar var : getKeyVars()) {
			result += var.getVarString() + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getNameWithVars() {
		return this.name + "(" + getVarsCSV() + ")";
	}

	public List<String> getSharedVars(FuxmanAtom atom) {
		List<String> sharedVars = new ArrayList<String>();
		for (QueryVar var : this.vars) {
			if (atom.getVars().contains(var))
				sharedVars.add(var.getVarString());
		}
		return sharedVars;
	}
}
