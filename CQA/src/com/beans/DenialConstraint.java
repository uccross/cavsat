package com.beans;

import java.util.ArrayList;
import java.util.List;

public class DenialConstraint {
	private int id;
	private List<Atom> atoms;
	private List<Expression> expressions;

	public DenialConstraint(List<Atom> atoms, List<Expression> expressions, int id) {
		super();
		this.id = id;
		this.atoms = atoms;
		this.expressions = expressions;
	}

	public DenialConstraint(int id) {
		super();
		this.id = id;
		this.atoms = new ArrayList<Atom>();
		this.expressions = new ArrayList<Expression>();
	}

	public int getId() {
		return id;
	}

	public List<Atom> getAtoms() {
		return atoms;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public String toString() {
		String constraint = "";
		for (Atom atom : this.atoms) {
			constraint += atom.getName() + "(" + atom.getVarsCSV() + ");";
		}
		for (Expression exp : this.expressions) {
			constraint += exp.getVar1() + exp.getOp() + exp.getVar2() + ";";
		}
		return constraint;
	}

}
