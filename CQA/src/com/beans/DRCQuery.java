/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.List;

import com.util.ProblemParser2;

public class DRCQuery extends FOFormula {

	public enum Quant {
		EXISTS, NOTEXISTS, FORALL, NOTFORALL;
	}

	private String syntax;
	private List<Quantifier> quantifiers;
	private FOFormula formula;

	public DRCQuery() {
		super();
		this.quantifiers = new ArrayList<Quantifier>();
		this.formula = null;
		this.syntax = null;
	}

	public String getSyntax() {
		return syntax;
	}

	public void setSyntax(String syntax) {
		this.syntax = syntax;
	}

	public void setQuantifiers(List<Quantifier> quantifiers) {
		this.quantifiers = quantifiers;
	}

	public List<Quantifier> getQuantifiers() {
		return quantifiers;
	}

	public FOFormula getFormula() {
		return formula;
	}

	public void setFormula(FOFormula formula) {
		this.formula = formula;
	}

	public void eliminateUniversalQuantification() {
		for (Quantifier qu : this.quantifiers) {
			qu.setQuantification(Quant.EXISTS);
		}
		this.getFormula().toggleNegation();
		this.toggleNegation();
	}

	public void printSyntax() {
		System.out.println(this.syntax);
	}

	public void printElaborate() {
		for (Quantifier quant : this.quantifiers) {
			System.out.print(quant.quantification + " " + quant.var + ", ");
		}
		this.formula.print();
	}

	public FuxmanQuery getFuxmanQuery() {
		return new ProblemParser2().parseQueryFromString(this.getSyntax());
	}

	public class Quantifier {
		private Quant quantification;
		private String var;

		@Override
		public boolean equals(Object obj) {
			return ((Quantifier) obj).quantification.equals(this.quantification)
					&& ((Quantifier) obj).var.equals(this.var);
		}

		@Override
		public int hashCode() {
			return -1;
		}

		public Quantifier(Quant quantification, String var) {
			super();
			this.quantification = quantification;
			this.var = var;
		}

		public Quant getQuantification() {
			return quantification;
		}

		public void setQuantification(Quant quantification) {
			this.quantification = quantification;
		}

		public String getVar() {
			return var;
		}

		public void setVar(String var) {
			this.var = var;
		}
	}
}
