/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Data;

@Data
public class Clause {
	private Set<Integer> vars;
	private double weight;
	private String description;

	public Clause() {
		this.vars = new HashSet<Integer>();
	}

	@Override
	public boolean equals(Object arg0) {
		return equalsIgnoreWeight(arg0) && ((Clause) arg0).weight == this.weight;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	public boolean equalsIgnoreWeight(Object arg0) {
		if (null == arg0 || !(arg0 instanceof Clause))
			return false;
		else
			return ((Clause) arg0).vars.equals(this.vars);
	}

	public void addVar(int var) {
		this.vars.add(var);
	}

	public void removeVar(int var) {
		this.vars.remove(var);
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
			line = Double.toString(this.weight) + " " + line;
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