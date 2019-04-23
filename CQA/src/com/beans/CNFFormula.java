/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CNFFormula {
	private Set<Clause> clauses;
	private Set<String> clauseStrings;
	private int noOfVariables;
	private int approach;

	public CNFFormula() {
		this.clauses = new HashSet<Clause>();
		this.clauseStrings = new HashSet<String>();
	}

	public Set<Clause> getClauses() {
		return clauses;
	}

	public void setClauses(Set<Clause> clauses) {
		this.clauses = clauses;
	}

	public int getNoOfVariables() {
		return noOfVariables;
	}

	public void setNoOfVariables(int noOfVariables) {
		this.noOfVariables = noOfVariables;
	}

	public Set<String> getClauseStrings() {
		return clauseStrings;
	}

	public void setClauseStrings(Set<String> clauseStrings) {
		this.clauseStrings = clauseStrings;
	}

	public void addClauseString(String clauseString) {
		if (!clauseString.isEmpty())
			this.clauseStrings.add(clauseString);
	}

	public void prettyPrint() {
		for (Clause c : this.clauses) {
			for (int literal : c.getVars()) {
				if (literal > 0) {
					System.out.print("x" + literal + " ");
				} else {
					System.out.print("-x" + literal * (-1) + " ");
				}
			}
			System.out.println();
		}
		System.out.println("-------------------------------------------------------------");
	}

	public CNFFormula combine(CNFFormula f2) {
		this.clauses.addAll(f2.getClauses());
		this.noOfVariables += f2.getNoOfVariables();
		return this;
	}

	public static void createCNFDimacsFile(CNFFormula[] formulas, int noOfVariables, boolean isBoolean,
			String filepath) {
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(filepath));
			int clauses = 0;
			for (CNFFormula formula : formulas) {
				clauses += formula.size();
			}
			if (isBoolean)
				writer.append("p cnf " + noOfVariables + " " + clauses + "\n");
			else
				writer.append("p wcnf " + noOfVariables + " " + clauses + "\n");
			for (CNFFormula formula : formulas) {
				for (String clauseString : formula.getClauseStrings()) {
					writer.append(clauseString + "0\n");
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int size() {
		return this.clauseStrings.size();
	}

	public void printClauseStrings() {
		for (String s : this.clauseStrings)
			System.out.println(s);
	}

	public void addClause(Clause clause) {
		if (null != clause && !clause.isEmpty())
			this.clauses.add(clause);
	}

	public int getApproach() {
		return approach;
	}

	public void setApproach(int approach) {
		this.approach = approach;
	}

	public boolean isEmpty() {
		return this.clauses.isEmpty();
	}
}
