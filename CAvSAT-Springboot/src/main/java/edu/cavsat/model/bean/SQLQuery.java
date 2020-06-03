/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.Data;

@Data
public class SQLQuery {
	private List<String> select;
	private boolean selectDistinct;
	private boolean isAggregate;
	private List<String> from;
	private List<Expression> kj; // Key-to-key join predicates
	private List<Expression> nkj; // Nonkey-to-key join predicates
	private List<Expression> sc; // Selection predicates
	private List<String> whereConditions;
	private List<String> orderingAttributes;
	private List<Boolean> orderDesc;
	private List<String> groupingAttributes;
	private List<String> aggFunctions;
	private List<String> aggAttributes;

	public SQLQuery() {
		super();
		this.select = new ArrayList<String>();
		this.selectDistinct = false;
		this.isAggregate = false;
		this.from = new ArrayList<String>();
		this.kj = new ArrayList<Expression>();
		this.nkj = new ArrayList<Expression>();
		this.sc = new ArrayList<Expression>();
		this.whereConditions = new ArrayList<String>();
		this.orderingAttributes = new ArrayList<String>();
		this.orderDesc = new ArrayList<Boolean>();
		this.groupingAttributes = new ArrayList<String>();
		this.aggFunctions = new ArrayList<String>();
		this.aggAttributes = new ArrayList<String>();
	}

	public SQLQuery(SQLQuery copyFrom) {
		super();
		this.select = new ArrayList<String>(copyFrom.select);
		this.selectDistinct = Boolean.valueOf(copyFrom.selectDistinct);
		this.isAggregate = Boolean.valueOf(copyFrom.isAggregate);
		this.from = new ArrayList<String>(copyFrom.from);
		this.kj = new ArrayList<Expression>(copyFrom.kj);
		this.nkj = new ArrayList<Expression>(copyFrom.nkj);
		this.sc = new ArrayList<Expression>(copyFrom.sc);
		this.whereConditions = new ArrayList<String>(copyFrom.whereConditions);
		this.orderingAttributes = new ArrayList<String>(copyFrom.orderingAttributes);
		this.orderDesc = new ArrayList<Boolean>(copyFrom.orderDesc);
		this.groupingAttributes = new ArrayList<String>(copyFrom.groupingAttributes);
		this.aggFunctions = new ArrayList<String>(copyFrom.aggFunctions);
		this.aggAttributes = new ArrayList<String>(copyFrom.aggAttributes);
	}

	public String getSQLSyntax() {
		return getSQLSyntax("");
	}

	public String getSQLSyntaxWithoutAggregates() {
		return getSQLSyntaxWithoutAggregates("");
	}

	public SQLQuery getQueryWithoutAggregates() {
		SQLQuery noAggQuery = new SQLQuery(this);
		noAggQuery.aggAttributes.clear();
		noAggQuery.aggFunctions.clear();
		noAggQuery.groupingAttributes.clear();
		noAggQuery.isAggregate = false;
		noAggQuery.select = this.select.stream()
				.filter(Pattern.compile("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)").asPredicate().negate())
				.collect(Collectors.<String>toList());
		return noAggQuery;
	}

	public SQLQuery getQueryWithoutGroupBy() {
		SQLQuery noGroupByQuery = new SQLQuery(this);
		noGroupByQuery.groupingAttributes.clear();
		noGroupByQuery.select = this.select.stream()
				.filter(Pattern.compile("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)").asPredicate())
				.collect(Collectors.<String>toList());
		return noGroupByQuery;
	}

	/*
	 * public String getSQLSyntaxWithoutAggregates(String selectInto) {
	 * StringBuilder sqlSyntax = new StringBuilder("SELECT "); if
	 * (this.selectDistinct) sqlSyntax.append("DISTINCT "); sqlSyntax.append(
	 * this.select.stream().map(attribute ->
	 * attribute.replaceAll("(SUM|AVG|MIN|MAX|COUNT|\\(|\\))", ""))
	 * .collect(Collectors.joining(","))); if (selectInto != null &&
	 * !selectInto.isEmpty()) { sqlSyntax.append("\nINTO ");
	 * sqlSyntax.append(selectInto); } sqlSyntax.append("\nFROM ");
	 * sqlSyntax.append(String.join(",", this.from)); if
	 * (!this.whereConditions.isEmpty()) { sqlSyntax.append("\nWHERE ");
	 * sqlSyntax.append(String.join(" AND ", whereConditions)); } if
	 * (!this.orderingAttributes.isEmpty()) { sqlSyntax.append("\nORDER BY ");
	 * sqlSyntax.append(String.join(",", this.orderingAttributes)); } return
	 * sqlSyntax.toString(); }
	 */

	public String getSQLSyntaxWithoutAggregates(String selectInto) {
		return getQueryWithoutAggregates().getSQLSyntax(selectInto);
	}

	public String getSQLSyntax(String selectInto) {
		StringBuilder sqlSyntax = new StringBuilder("SELECT ");
		if (this.selectDistinct)
			sqlSyntax.append("DISTINCT ");
		sqlSyntax.append(String.join(",", this.select));
		if (selectInto != null && !selectInto.isEmpty()) {
			sqlSyntax.append("\nINTO ");
			sqlSyntax.append(selectInto);
		}
		sqlSyntax.append("\nFROM ");
		sqlSyntax.append(String.join(",", this.from));
		if (!this.whereConditions.isEmpty()) {
			sqlSyntax.append("\nWHERE ");
			sqlSyntax.append(String.join(" AND ", whereConditions));
		}
		if (!this.groupingAttributes.isEmpty()) {
			sqlSyntax.append("\nGROUP BY ");
			sqlSyntax.append(String.join(",", this.groupingAttributes));
		}
		if (!this.orderingAttributes.isEmpty()) {
			sqlSyntax.append("\nORDER BY ");
			sqlSyntax.append(this.orderingAttributes.stream()
					.map(a -> a + (this.orderDesc.get(this.orderingAttributes.indexOf(a)) ? " DESC" : " ASC"))
					.collect(Collectors.joining(",")));
		}
		return sqlSyntax.toString();
	}
}
