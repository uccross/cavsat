/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Clause {
	private Set<Integer> vars;
	private int weight;
	private String description;

	public Clause() {
		this.vars = new HashSet<Integer>();
	}

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		if (!(arg0 instanceof Clause))
			return false;
		else
			return ((Clause) arg0).vars.equals(this.vars);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Set<Integer> getVars() {
		return vars;
	}

	public void setVars(Set<Integer> vars) {
		this.vars = vars;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void addVar(int var) {
		this.vars.add(var);
	}

	public void removeVar(int var) {
		this.vars.remove(var);
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public boolean isEmpty() {
		return this.vars.isEmpty();
	}

	public String getDimacsLine(boolean withWeight) {
		String line = "";
		for (int var : this.vars) {
			line += Integer.toString(var) + " ";
		}
		if (withWeight)
			line = Integer.toString(this.weight) + " " + line;
		return line + "0 c " + this.description + "\n";
	}

	public String getDimacsLine() {
		return getDimacsLine(false);
	}

	public List<Clause> cnfNeg() {
		List<Clause> list = new ArrayList<Clause>();
		List<Integer> literalsSoFar = new ArrayList<Integer>();
		Clause c;
		for (int var : this.vars) {
			c = new Clause();
			c.setWeight(this.weight);
			for (int lit : literalsSoFar)
				c.addVar(lit);
			c.addVar(-1 * var);
			list.add(c);
			literalsSoFar.add(var);
		}
		return list;
	}
}