/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.List;

public class FOFormula {

	public enum Op {
		AND, OR, IMPLIES, EQUALS, NOTEQUALS
	}

	private FOFormula left;
	private FOFormula right;
	private Op op;
	private String var;
	private boolean negation;

	public FOFormula() {
		super();
	}

	public FOFormula(String var) {
		super();
		this.left = null;
		this.right = null;
		this.op = null;
		this.var = var;
		this.negation = false;
	}

	public FOFormula(FOFormula left, FOFormula right, Op op) {
		super();
		this.left = left;
		this.right = right;
		this.op = op;
		this.var = null;
		this.negation = false;
	}

	public FOFormula getLeft() {
		return left;
	}

	public void setLeft(FOFormula left) {
		this.left = left;
	}

	public FOFormula getRight() {
		return right;
	}

	public void setRight(FOFormula right) {
		this.right = right;
	}

	public Op getOp() {
		return op;
	}

	public void setOp(Op op) {
		this.op = op;
	}

	public String getVar() {
		return var;
	}

	public void setVar(String var) {
		this.var = var;
	}

	public boolean isNegation() {
		return negation;
	}

	public void toggleNegation() {
		this.negation = !this.negation;
	}

	public static <T extends FOFormula> T getConjunction(List<T> list, Class<T> type) {
		if (list == null || list.isEmpty())
			return null;
		if (list.size() == 1)
			return list.get(0);
		FOFormula conjunction = new FOFormula(list.get(0), list.get(1), Op.AND);
		for (int i = 2; i < list.size(); i++) {
			conjunction = new FOFormula(list.get(i), conjunction, Op.AND);
		}
		return type.cast(conjunction);
	}

	public void eliminateImplication() {
		if (this.op.equals(Op.IMPLIES)) {
			this.op = Op.OR;
			this.left.toggleNegation();
		}
	}

	public void print() {
		System.out.print("(");
		if (this.negation)
			System.out.print("NOT ");
		if (this instanceof FuxmanAtom) {
			FuxmanAtom atom = (FuxmanAtom) this;
			System.out.print(atom.getName() + "(" + atom.getVarsCSV(true) + ")");
		} else if (this instanceof DRCQuery) {
			((DRCQuery) this).printElaborate();
		} else if (this.var != null) {
			System.out.print(this.var);
		} else {
			if (this.left != null)
				this.left.print();
			if (this.op != null)
				System.out.print(" " + this.op + " ");
			if (this.right != null)
				this.right.print();
		}
		System.out.print(")");
	}
}
