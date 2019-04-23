/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

public class Expression {
	private String var1;
	private String var2;
	private String op;

	public Expression(String var1, String var2, String op) {
		super();
		this.var1 = var1;
		this.var2 = var2;
		this.op = op;
	}

	public String getVar1() {
		return var1;
	}

	public void setVar1(String var1) {
		this.var1 = var1;
	}

	public String getVar2() {
		return var2;
	}

	public void setVar2(String var2) {
		this.var2 = var2;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public void negate() {
		switch (this.op) {
		case "=":
			this.op = "!=";
			break;
		case "!=":
			this.op = "=";
			break;
		case ">":
			this.op = "<=";
			break;
		case "<":
			this.op = ">=";
			break;
		case "<=":
			this.op = ">";
			break;
		case ">=":
			this.op = "<";
			break;
		}
	}

	@Override
	public String toString() {
		return "(" + this.var1 + this.op + this.var2 + ")";
	}
}
