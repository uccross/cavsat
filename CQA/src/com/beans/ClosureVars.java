package com.beans;

import java.util.HashSet;
import java.util.Set;

public class ClosureVars {
	private String atomName;
	private Set<String> vars;

	public ClosureVars(String atomName) {
		super();
		this.atomName = atomName;
		this.vars = new HashSet<String>();
	}

	public String getAtomName() {
		return atomName;
	}

	public Set<String> getVars() {
		return vars;
	}

	public void setVars(Set<String> vars) {
		this.vars = vars;
	}

	public void addVar(String var) {
		this.vars.add(var);
	}

	public void addVars(Set<String> vars) {
		this.vars.addAll(vars);
	}

	public void print() {
		String output = "Atom: " + this.atomName + "\t Attribute Closure: {";
		for (String var : this.vars) {
			output += var + ",";
		}
		if (output.endsWith(","))
			output = output.substring(0, output.length() - 1);
		output += "}";
		System.out.println(output);
	}
}
