/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.bean;

import java.util.ArrayList;
import java.util.List;

public class SQLQuery {
	private List<String> select;
	private boolean selectDistinct;
	private List<String> from;
	private List<Expression> kj; // Key-to-key join predicates
	private List<Expression> nkj; // Nonkey-to-key join predicates
	private List<Expression> sc; // Selection predicates
	private List<String> whereConditions;
	private List<String> orderingAttributes;
	private List<String> groupingAttributes;
	private List<String> aggFunctions;
	private List<String> aggAttributes;

	public SQLQuery() {
		super();
		this.select = new ArrayList<String>();
		this.selectDistinct = false;
		this.from = new ArrayList<String>();
		this.kj = new ArrayList<Expression>();
		this.nkj = new ArrayList<Expression>();
		this.sc = new ArrayList<Expression>();
		this.whereConditions = new ArrayList<String>();
		this.orderingAttributes = new ArrayList<String>();
		this.groupingAttributes = new ArrayList<String>();
		this.aggFunctions = new ArrayList<String>();
		this.aggAttributes = new ArrayList<String>();
	}

	public SQLQuery(SQLQuery copyFrom) {
		super();
		this.select = new ArrayList<String>(copyFrom.select);
		this.selectDistinct = Boolean.valueOf(copyFrom.selectDistinct);
		this.from = new ArrayList<String>(copyFrom.from);
		this.kj = new ArrayList<Expression>(copyFrom.kj);
		this.nkj = new ArrayList<Expression>(copyFrom.nkj);
		this.sc = new ArrayList<Expression>(copyFrom.sc);
		this.whereConditions = new ArrayList<String>(copyFrom.whereConditions);
		this.orderingAttributes = new ArrayList<String>(copyFrom.orderingAttributes);
		this.groupingAttributes = new ArrayList<String>(copyFrom.groupingAttributes);
		this.aggFunctions = new ArrayList<String>(copyFrom.aggFunctions);
		this.aggAttributes = new ArrayList<String>(copyFrom.aggAttributes);
	}

	public List<String> getSelect() {
		return select;
	}

	public boolean isSelectDistinct() {
		return selectDistinct;
	}

	public void setSelectDistinct(boolean selectDistinct) {
		this.selectDistinct = selectDistinct;
	}

	public void setSelect(List<String> select) {
		this.select = select;
	}

	public List<String> getFrom() {
		return from;
	}

	public void setFrom(List<String> from) {
		this.from = from;
	}

	public List<Expression> getKj() {
		return kj;
	}

	public void setKj(List<Expression> kj) {
		this.kj = kj;
	}

	public List<Expression> getNkj() {
		return nkj;
	}

	public void setNkj(List<Expression> nkj) {
		this.nkj = nkj;
	}

	public List<Expression> getSc() {
		return sc;
	}

	public void setSc(List<Expression> sc) {
		this.sc = sc;
	}

	public List<String> getWhereConditions() {
		return whereConditions;
	}

	public void setWhereConditions(List<String> whereConditions) {
		this.whereConditions = whereConditions;
	}

	public List<String> getOrderingAttributes() {
		return orderingAttributes;
	}

	public void setOrderingAttributes(List<String> orderingAttributes) {
		this.orderingAttributes = orderingAttributes;
	}

	public List<String> getGroupingAttributes() {
		return groupingAttributes;
	}

	public void setGroupingAttributes(List<String> groupingAttributes) {
		this.groupingAttributes = groupingAttributes;
	}

	public List<String> getAggFunctions() {
		return aggFunctions;
	}

	public void setAggFunctions(List<String> aggFunctions) {
		this.aggFunctions = aggFunctions;
	}

	public List<String> getAggAttributes() {
		return aggAttributes;
	}

	public void setAggAttributes(List<String> aggAttributes) {
		this.aggAttributes = aggAttributes;
	}

	public String getSQLSyntax() {
		return getSQLSyntax("");
	}

	public String getSQLSyntax(String selectInto) {
		String sqlSyntax = "";
		if (this.selectDistinct)
			sqlSyntax += "SELECT DISTINCT ";
		else
			sqlSyntax += "SELECT ";
		sqlSyntax += String.join(",", this.select) + "\n";
		if (selectInto != null && !selectInto.isEmpty())
			sqlSyntax += "INTO " + selectInto + " ";
		sqlSyntax += "FROM " + String.join(",", this.from) + "\n";
		if (!whereConditions.isEmpty())
			sqlSyntax += "WHERE " + String.join(" AND ", whereConditions) + "\n";
		if (!this.groupingAttributes.isEmpty())
			sqlSyntax += "GROUP BY " + String.join(",", this.groupingAttributes) + "\n";
		if (!this.orderingAttributes.isEmpty())
			sqlSyntax += "ORDER BY " + String.join(",", this.orderingAttributes) + "\n";
		return sqlSyntax;
	}

	public void print() {
		System.out.println(getSQLSyntax());
	}
}
