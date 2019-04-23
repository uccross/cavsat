/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.querypreprocessor.CertainRewriter;

public class TRCQuery {
	public enum Q {
		EXISTS, NOTEXISTS, FORALL, NOTFORALL;
	}

	public enum Op {
		AND, OR, IMPLIES, EQUALS, NOTEQUALS
	}

	private List<Quantifier> quantifiers;
	private Formula formula;
	private List<VarAttrPair> freeVars;

	public TRCQuery() {
		super();
		this.quantifiers = new ArrayList<Quantifier>();
		this.freeVars = new ArrayList<VarAttrPair>();
		this.formula = null;
	}

	public List<Quantifier> getQuantifiers() {
		return quantifiers;
	}

	public List<VarAttrPair> getFreeVars() {
		return freeVars;
	}

	public Formula getFormula() {
		return formula;
	}

	public void setFormula(Formula formula) {
		this.formula = formula;
	}

	public void print() {
		for (VarAttrPair varAttrPair : this.freeVars) {
			System.out.print(varAttrPair.tupleVar.var + "." + varAttrPair.attribute + ", ");
		}
		System.out.print("| ");
		for (Quantifier quantifier : this.quantifiers) {
			System.out.print(quantifier.quantification + " " + quantifier.tupleVar.var + " in "
					+ quantifier.tupleVar.relation.getName() + ", ");
		}
		if (this.formula != null)
			this.formula.print();
		else
			System.out.print("(NULL)");
	}

	public String printSQL() {
		StringBuilder builder = new StringBuilder("\n");
		String tabs = "\t", prefix = "", fromClause = "";
		Map<Integer, String[]> freeVarAttrPairs = CertainRewriter.FREE_VARS_MAP;
		builder.append("SELECT DISTINCT ");
		for (int key : freeVarAttrPairs.keySet()) {
			builder.append(prefix);
			fromClause += prefix;
			prefix = ",";
			builder.append(CertainRewriter.FREE_TUPLE_ALIAS + freeVarAttrPairs.get(key)[0] + "."
					+ freeVarAttrPairs.get(key)[1]);
			fromClause += freeVarAttrPairs.get(key)[0] + " AS " + CertainRewriter.FREE_TUPLE_ALIAS
					+ freeVarAttrPairs.get(key)[0];
		}
		builder.append(" FROM " + fromClause);
		builder.append("\n");
		for (Quantifier quantifier : this.quantifiers) {
			if (quantifier.quantification == Q.EXISTS) {
				builder.append(tabs + " WHERE EXISTS(\n");
			} else if (quantifier.quantification == Q.NOTEXISTS) {
				builder.append(tabs + " WHERE NOT EXISTS(\n");
			} else {
				System.out.println(quantifier.quantification + " CANNOT PRINT SQL");
				return null;
			}
			tabs += "\t";
			builder.append(tabs + "SELECT * FROM " + quantifier.tupleVar.relation.getName() + " AS "
					+ quantifier.tupleVar.var + "\n");
		}
		builder.append(tabs + "WHERE ");
		builder.append(this.formula.printSQL(tabs).toString());
		builder.append("\n");
		for (int i = 0; i < this.quantifiers.size(); i++) {
			tabs = tabs.substring(0, tabs.length() - 1);
			builder.append(tabs);
			builder.append(")");
			builder.append("\n");
		}
		return builder.toString();
	}

	public class TupleVar {
		private Relation relation;
		private String var;

		public TupleVar(Relation relation, String var) {
			super();
			this.relation = relation;
			this.var = var;
		}

		public Relation getRelation() {
			return relation;
		}

		public void setRelation(Relation relation) {
			this.relation = relation;
		}

		public String getVar() {
			return var;
		}

		public void setVar(String var) {
			this.var = var;
		}

	}

	public class Quantifier {
		private Q quantification;
		private TupleVar tupleVar;

		public Quantifier(Q quantification, TupleVar tupleVar) {
			super();
			this.quantification = quantification;
			this.tupleVar = tupleVar;
		}

		public Q getQuantification() {
			return quantification;
		}

		public void setQuantification(Q quantification) {
			this.quantification = quantification;
		}

		public TupleVar getTupleVar() {
			return tupleVar;
		}

		public void setTupleVar(TupleVar tupleVar) {
			this.tupleVar = tupleVar;
		}
	}

	public class Formula {
		private Formula left;
		private Formula right;
		private Op op;

		public Formula() {
			super();
		}

		public Formula(Formula left, Formula right, Op op) {
			super();
			this.left = left;
			this.right = right;
			this.op = op;
		}

		public Formula getLeft() {
			return left;
		}

		public void setLeft(Formula left) {
			this.left = left;
		}

		public Formula getRight() {
			return right;
		}

		public void setRight(Formula right) {
			this.right = right;
		}

		public Op getOp() {
			return op;
		}

		public void setOp(Op op) {
			this.op = op;
		}

		public void print() {
			if (this instanceof VarAttrPair) {
				System.out.print((VarAttrPair) this);
			} else {
				System.out.print("(");
				if (this.left == null)
					System.out.print("NULL");
				else
					this.left.print();
				System.out.print(" " + this.op + " ");
				if (this.right == null)
					System.out.print("NULL");
				else
					this.right.print();
				System.out.print(")");
			}
		}

		public StringBuilder printSQL(String tabs) {
			StringBuilder builder = new StringBuilder("");
			if (this instanceof VarAttrPair) {
				VarAttrPair varAttrPair = (VarAttrPair) this;
				if (varAttrPair.tupleVar.var.equals(CertainRewriter.FREE_TUPLE)) {
					String[] arr = CertainRewriter.FREE_VARS_MAP.get(Integer.parseInt(varAttrPair.attribute));
					builder.append(CertainRewriter.FREE_TUPLE_ALIAS + arr[0] + "." + arr[1]);
				} else
					builder.append(varAttrPair);
			} else {
				builder.append(" (");
				if (this.left == null)
					builder.append("NULL");
				else
					builder.append(this.left.printSQL(tabs).toString());
				switch (this.op) {
				case EQUALS:
					builder.append("=");
					break;
				case NOTEQUALS:
					builder.append("!=");
					break;
				case AND:
					builder.append("\n" + tabs + "AND ");
					break;
				case OR:
					builder.append("\n" + tabs + "OR ");
					break;
				default:
					break;
				}
				if (this.right == null)
					builder.append("NULL");
				else
					builder.append(this.right.printSQL(tabs).toString());
				builder.append(") ");
			}
			return builder;
		}
	}

	public class VarAttrPair extends Formula {
		private TupleVar tupleVar;
		private String attribute;

		public TupleVar getTupleVar() {
			return tupleVar;
		}

		public String getAttribute() {
			return attribute;
		}

		public VarAttrPair(TupleVar tupleVar, String attribute) {
			super();
			this.tupleVar = tupleVar;
			this.attribute = attribute;
		}

		@Override
		public String toString() {
			return this.tupleVar.var + "." + this.attribute;
		}
	}

	public Formula negateFormula(Formula formula) {
		Formula f = formula;
		switch (f.getOp()) {
		case AND:
			return new Formula(negateFormula(formula.left), negateFormula(formula.right), Op.OR);
		case OR:
			return new Formula(negateFormula(formula.left), negateFormula(formula.right), Op.AND);
		case IMPLIES:
			return new Formula(formula.left, negateFormula(formula.right), Op.AND);
		case EQUALS:
			f.setOp(Op.NOTEQUALS);
			break;
		case NOTEQUALS:
			f.setOp(Op.EQUALS);
			break;
		}
		return f;
	}
}
