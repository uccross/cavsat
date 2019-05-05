/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLQuery {
	private List<String> select;
	private boolean selectDistinct;
	private List<String> from;
	private List<Expression> kj; // Key-to-key join predicates
	private List<Expression> nkj; // Nonkey-to-key join predicates
	private List<Expression> sc; // Selection predicates
	private List<String> orderby;
	private List<String> groupby;

	public SQLQuery() {
		super();
		this.select = new ArrayList<String>();
		this.selectDistinct = false;
		this.from = new ArrayList<String>();
		this.kj = new ArrayList<Expression>();
		this.nkj = new ArrayList<Expression>();
		this.sc = new ArrayList<Expression>();
		this.orderby = new ArrayList<String>();
		this.groupby = new ArrayList<String>();
	}

	public SQLQuery(SQLQuery copyFrom) {
		super();
		this.select = new ArrayList<String>(copyFrom.select);
		this.selectDistinct = Boolean.valueOf(copyFrom.selectDistinct);
		this.from = new ArrayList<String>(copyFrom.from);
		this.kj = new ArrayList<Expression>(copyFrom.kj);
		this.nkj = new ArrayList<Expression>(copyFrom.nkj);
		this.sc = new ArrayList<Expression>(copyFrom.sc);
		this.orderby = new ArrayList<String>(copyFrom.orderby);
		this.groupby = new ArrayList<String>(copyFrom.groupby);
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

	public List<String> getOrderby() {
		return orderby;
	}

	public void setOrderby(List<String> orderby) {
		this.orderby = orderby;
	}

	public List<String> getGroupby() {
		return groupby;
	}

	public void setGroupby(List<String> groupby) {
		this.groupby = groupby;
	}

	public String getSQLSyntax() {
		String sqlSyntax = "";
		if (this.selectDistinct)
			sqlSyntax += "SELECT DISTINCT ";
		else
			sqlSyntax += "SELECT ";
		sqlSyntax += String.join(",", this.select) + "\n";
		sqlSyntax += "FROM " + String.join(",", this.from) + "\n";
		if (!this.kj.isEmpty() || !this.nkj.isEmpty() || !this.sc.isEmpty())
			sqlSyntax += "WHERE " + String.join(" AND ",
					Stream.of(this.kj, this.nkj, this.sc).flatMap(x -> x.stream()).collect(Collectors.toList()).stream()
							.map(object -> Objects.toString(object, null)).collect(Collectors.toList()))
					+ "\n";
		if (!this.orderby.isEmpty())
			sqlSyntax += "ORDER BY " + String.join(",", this.orderby) + "\n";
		if (!this.groupby.isEmpty())
			sqlSyntax += "GROUP BY " + String.join(",", this.groupby) + "\n";

		return sqlSyntax;
	}

	public void print() {
		System.out.println(getSQLSyntax());
	}
}
