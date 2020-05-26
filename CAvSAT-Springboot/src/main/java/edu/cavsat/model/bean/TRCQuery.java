/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cavsat.util.Constants;

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
	private Map<Integer, String[]> freeVarsMap;

	public TRCQuery(Map<Integer, String[]> freeVarsMap) {
		super();
		this.quantifiers = new ArrayList<Quantifier>();
		this.freeVars = new ArrayList<VarAttrPair>();
		this.formula = null;
		this.freeVarsMap = freeVarsMap;
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

	public String toTRCString(boolean inUnicode) {
		StringBuilder sb = new StringBuilder();
		sb.append(this.freeVars.stream().map(varAttrPair -> varAttrPair.tupleVar.var + "." + varAttrPair.attribute)
				.collect(Collectors.joining(", ")));
		sb.append(" | ");
		if (inUnicode)
			sb.append(this.quantifiers.stream().map(quantifier -> quantifier.unicode + " " + quantifier.tupleVar.var
					+ " &isin; " + quantifier.tupleVar.relation.getName()).collect(Collectors.joining(", ")));
		else
			sb.append(
					this.quantifiers
							.stream().map(quantifier -> quantifier.quantification + " " + quantifier.tupleVar.var
									+ " IN " + quantifier.tupleVar.relation.getName())
							.collect(Collectors.joining(", ")));

		sb.append(" ");
		if (this.formula != null)
			sb.append(this.formula.pretty(inUnicode));
		else
			sb.append("(NULL)");
		return sb.toString();
	}

	public String toSQL() {
		StringBuilder builder = new StringBuilder("\n");
		String prefix = "";
		Set<String> fromClause = new HashSet<String>();
		builder.append("SELECT DISTINCT ");

		for (int key : this.freeVarsMap.keySet()) {
			builder.append(prefix);
			prefix = ",";
			builder.append(
					Constants.FREE_TUPLE_ALIAS + this.freeVarsMap.get(key)[0] + "." + this.freeVarsMap.get(key)[1]);
			fromClause.add(
					this.freeVarsMap.get(key)[0] + " AS " + Constants.FREE_TUPLE_ALIAS + this.freeVarsMap.get(key)[0]);
		}
		builder.append(" FROM " + fromClause.stream().collect(Collectors.joining(", ")));
		for (Quantifier quantifier : this.quantifiers) {
			switch (quantifier.quantification) {
			case EXISTS:
				builder.append(" WHERE EXISTS(");
				break;
			case NOTEXISTS:
				builder.append(" WHERE NOT EXISTS(");
				break;
			default:
				System.err.println(quantifier.quantification + " CANNOT PRINT SQL");
				return null;
			}
			builder.append(
					"SELECT * FROM " + quantifier.tupleVar.relation.getName() + " AS " + quantifier.tupleVar.var);
		}
		builder.append(" WHERE ");
		builder.append(this.formula.printSQL().toString());
		for (int i = 0; i < this.quantifiers.size(); i++) {
			builder.append(")");
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
		private String unicode;

		public Quantifier(Q quantification, TupleVar tupleVar) {
			super();
			setQuantification(quantification);
			this.tupleVar = tupleVar;
		}

		public Q getQuantification() {
			return quantification;
		}

		public void setQuantification(Q quantification) {
			this.quantification = quantification;
			switch (quantification) {
			case EXISTS:
				this.unicode = "&exist;";
				break;
			case NOTEXISTS:
				this.unicode = "&nexist;";
				break;
			case FORALL:
				this.unicode = "&forall;";
				break;
			case NOTFORALL:
				this.unicode = "&not;&forall;";
				break;
			}
		}

		public TupleVar getTupleVar() {
			return tupleVar;
		}

		public void setTupleVar(TupleVar tupleVar) {
			this.tupleVar = tupleVar;
		}

		public String getUnicode() {
			return unicode;
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

		public String pretty(boolean inUnicode) {
			StringBuilder sb = new StringBuilder();
			if (this instanceof VarAttrPair) {
				sb.append((VarAttrPair) this);
			} else {
				sb.append("(");
				if (this.left == null)
					sb.append("NULL");
				else
					sb.append(this.left.pretty(inUnicode));
				sb.append(" ");
				if (inUnicode) {
					switch (this.op) {
					case AND:
						sb.append("&and;");
						break;
					case EQUALS:
						sb.append("=");
						break;
					case IMPLIES:
						sb.append("&rarr;");
						break;
					case NOTEQUALS:
						sb.append("&ne;");
						break;
					case OR:
						sb.append("&or;");
						break;
					default:
						break;
					}
				} else {
					sb.append(this.op);
				}
				sb.append(" ");
				if (this.right == null)
					sb.append("NULL");
				else
					sb.append(this.right.pretty(inUnicode));
				sb.append(")");
			}
			String formula = sb.toString();
			System.out.println(formula);
			return formula;
		}

		private StringBuilder printSQL() {
			StringBuilder builder = new StringBuilder("");
			if (this instanceof VarAttrPair) {
				VarAttrPair varAttrPair = (VarAttrPair) this;
				if (varAttrPair.tupleVar.var.equals(Constants.FREE_TUPLE)) {
					String[] arr = freeVarsMap.get(Integer.parseInt(varAttrPair.attribute));
					builder.append(Constants.FREE_TUPLE_ALIAS + arr[0] + "." + arr[1]);
				} else
					builder.append(varAttrPair);
			} else {
				builder.append(" (");
				if (this.left == null)
					builder.append("NULL");
				else
					builder.append(this.left.printSQL().toString());
				switch (this.op) {
				case EQUALS:
					builder.append("=");
					break;
				case NOTEQUALS:
					builder.append("!=");
					break;
				case AND:
					builder.append(" AND ");
					break;
				case OR:
					builder.append(" OR ");
					break;
				default:
					break;
				}
				if (this.right == null)
					builder.append("NULL");
				else
					builder.append(this.right.printSQL().toString());
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
