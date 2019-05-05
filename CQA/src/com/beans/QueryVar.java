/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

public class QueryVar {
	private String varString;
	private boolean isKey;
	private boolean isConstant;
	private boolean isExistential;

	@Override
	public boolean equals(Object obj) {
		return this.varString.equals(((QueryVar) obj).varString);
	}

	public QueryVar(String varString, boolean isKey, boolean isConstant, boolean isExistential) {
		super();
		this.varString = varString;
		this.isKey = isKey;
		this.isConstant = isConstant;
		this.isExistential = isExistential;
	}

	public String getVarString() {
		return varString;
	}

	public String getVarString(boolean withSyntax) {
		if (!withSyntax)
			return varString;
		String prefix = "", suffix = "";
		if (this.isKey()) {
			prefix += "{";
			suffix = "}" + suffix;
		}
		if (this.isConstant()) {
			prefix += "'";
			suffix = "'" + suffix;
		}
		return prefix + this.getVarString() + suffix;
	}

	public void setVar(String var) {
		this.varString = var;
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public boolean isConstant() {
		return isConstant;
	}

	public void setConstant(boolean isConstant) {
		this.isConstant = isConstant;
	}

	public boolean isExistential() {
		return isExistential;
	}

	public void setExistential(boolean isExistential) {
		this.isExistential = isExistential;
	}

	public void setVarString(String varString) {
		this.varString = varString;
	}
}
