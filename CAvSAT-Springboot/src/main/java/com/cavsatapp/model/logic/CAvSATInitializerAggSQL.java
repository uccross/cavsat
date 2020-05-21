/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.SQLQuery;
import com.cavsatapp.model.bean.Schema;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;

import lombok.AllArgsConstructor;

/**
 * @author Akhil
 *
 */
@AllArgsConstructor
public class CAvSATInitializerAggSQL {
	private CAvSATSQLQueries sqlQueriesImpl;

	public void attachSequentialFactIDsToRelevantTables(SQLQuery query, Connection con) throws SQLException {
		int startFactID = 1;
		String relationName = null;
		for (String relationNameWithoutPrefix : query.getFrom()) {
			relationName = Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationNameWithoutPrefix;
			PreparedStatement attachFactID = con
					.prepareStatement(sqlQueriesImpl.getAttachFactIDQuery(relationName, startFactID));
			attachFactID.execute();
			PreparedStatement getMaxFactID = con.prepareStatement(sqlQueriesImpl.getMaxFactIDQuery(relationName));
			ResultSet rs = getMaxFactID.executeQuery();
			rs.next();
			startFactID = rs.getInt(1) + 1;
		}
	}

	public void createAnsFromCons(SQLQuery query, Schema schema, Connection con) throws SQLException {
		SQLQuery ansFromConsQuery = new SQLQuery(query);
		ansFromConsQuery.setFrom(query.getFrom().stream()
				.map(relationName -> Constants.CAvSAT_CONS_TABLE_PREFIX + relationName).collect(Collectors.toList()));
		ansFromConsQuery.setAggAttributes(ansFromConsQuery.getAggAttributes().stream()
				.map(a -> Constants.CAvSAT_CONS_TABLE_PREFIX + a).collect(Collectors.toList()));
/*		ansFromConsQuery.setSelect(query.getSelect().stream()
				.map(attribute -> (attribute.matches("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)")
						? attribute + " AS " + attribute.split("[()]")[0]
						: Constants.CAvSAT_CONS_TABLE_PREFIX + attribute + " AS " + attribute.split("\\.")[1]))
				.collect(Collectors.toList()));
*/		List<String> selectAttributes = new ArrayList<String>();
		int i = 0;
		for (String attr : query.getSelect()) {
			if (attr.matches("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)")) {
				selectAttributes.add(
						ansFromConsQuery.getAggFunctions().get(i) + "(" + ansFromConsQuery.getAggAttributes().get(i)
								+ ") AS " + ansFromConsQuery.getAggFunctions().get(i));
				i++;
			} else
				selectAttributes.add(Constants.CAvSAT_CONS_TABLE_PREFIX + attr + " AS " + attr.split("\\.")[1]);
		}
		ansFromConsQuery.setSelect(selectAttributes);
		List<String> newConditions = new ArrayList<String>();
		for (String condition : query.getWhereConditions()) {
			String newCondition = condition;
			for (String relationName : query.getFrom()) {
				newCondition = newCondition.replaceAll(relationName + "\\.",
						Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + "\\.");
			}
			newConditions.add(newCondition);
		}
		// Check for boolean query, and add 1 to the select clause (MS SQL Server
		// Syntax)
		if (ansFromConsQuery.getSelect().isEmpty())
			ansFromConsQuery.getSelect().add("1 AS " + Constants.BOOL_CONS_ANSWER_COLUMN_NAME);
		ansFromConsQuery.setWhereConditions(newConditions);
		ansFromConsQuery.setSelectDistinct(true);
		System.out.println("Answers from consistent part of the data:\n"
				+ ansFromConsQuery.getSQLSyntax(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME) + "\n");
		con.prepareStatement(ansFromConsQuery.getSQLSyntax(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute();
	}

	/*
	 * public void createWitnesses(SQLQuery query, Schema schema, Connection con)
	 * throws SQLException { SQLQuery witnessQuery = new SQLQuery(query);
	 * List<String> newConditions = new ArrayList<String>(); List<String>
	 * selectAttributes = new ArrayList<String>(); for (String relationName :
	 * witnessQuery.getFrom()) {
	 * selectAttributes.addAll(schema.getRelationByName(relationName).getAttributes(
	 * ).stream() .map(attr -> relationName + "." + attr + " AS " + relationName +
	 * "_" + attr) .collect(Collectors.toList())); String condition =
	 * "EXISTS (SELECT 1 FROM " + Constants.CAvSAT_INCONS_TABLE_PREFIX +
	 * relationName + " WHERE "; Set<String> keyConditions = new HashSet<String>();
	 * for (String key :
	 * schema.getRelationByName(relationName).getKeyAttributesList()) {
	 * keyConditions.add(Constants.CAvSAT_INCONS_TABLE_PREFIX + relationName + "." +
	 * key + " = " + relationName + "." + key); } condition = condition +
	 * String.join(" AND ", keyConditions) + ")"; newConditions.add(condition); }
	 * String condition = String.join(" OR ", newConditions);
	 * witnessQuery.getWhereConditions().add(condition);
	 * witnessQuery.setSelectDistinct(true);
	 * witnessQuery.setSelect(selectAttributes); System.out.println(
	 * "Creating witnesses:\n" +
	 * witnessQuery.getSQLSyntax(Constants.CAvSAT_WITNESSES_TABLE_NAME) + "\n");
	 * con.prepareStatement(witnessQuery.getSQLSyntax(Constants.
	 * CAvSAT_WITNESSES_TABLE_NAME)).execute(); }
	 */
	public void createWitnesses(SQLQuery query, Schema schema, Connection con) throws SQLException {
		SQLQuery q1 = new SQLQuery(query), q2 = new SQLQuery(query);
		List<String> selectAttributes1 = new ArrayList<String>(), selectAttributes2 = new ArrayList<String>();
		for (String relationName : query.getFrom()) {
			selectAttributes1.addAll(schema.getRelationByName(relationName).getAttributes().stream()
					.map(attr -> relationName + "." + attr + " AS " + relationName + "_" + attr)
					.collect(Collectors.toList()));
			selectAttributes2.addAll(schema.getRelationByName(relationName).getAttributes().stream()
					.map(attr -> Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + "." + attr + " AS " + relationName
							+ "_" + attr)
					.collect(Collectors.toList()));
		}
		q1.setSelectDistinct(true);
		q2.setSelectDistinct(true);
		q1.setSelect(selectAttributes1);
		q2.setSelect(selectAttributes2);
		q2.setFrom(q2.getFrom().stream().map(relationName -> Constants.CAvSAT_CONS_TABLE_PREFIX + relationName)
				.collect(Collectors.toList()));
		List<String> newConditions = new ArrayList<String>();
		for (String condition : q2.getWhereConditions()) {
			String newCondition = condition;
			for (String relationName : query.getFrom()) {
				newCondition = newCondition.replaceAll(relationName + "\\.",
						Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + "\\.");
			}
			newConditions.add(newCondition);
		}
		q2.setWhereConditions(newConditions);
		System.out.println("Creating witnesses:\n" + q1.getSQLSyntax(Constants.CAvSAT_WITNESSES_TABLE_NAME) + " EXCEPT "
				+ q2.getSQLSyntax() + "\n");
		con.prepareStatement(q1.getSQLSyntax(Constants.CAvSAT_WITNESSES_TABLE_NAME) + " EXCEPT " + q2.getSQLSyntax())
				.execute();
	}

	public boolean checkBooleanConsAnswer(Connection con) throws SQLException {
		ResultSet rsCheckBooleanConsAnswer = con.prepareStatement(sqlQueriesImpl.getCheckBooleanAnswerQuery())
				.executeQuery();
		if (rsCheckBooleanConsAnswer.next())
			return rsCheckBooleanConsAnswer.getInt(1) != 0;
		return false;
	}

	public void createRelevantTables(SQLQuery query, Schema schema, Connection con) throws SQLException {
		Set<String> whereConditions = new HashSet<String>();
		System.out.println("Creating relevant tables:");
		for (String relationName : query.getFrom()) {
			whereConditions.clear();
			Relation r = schema.getRelationByName(relationName);
			whereConditions.add(r
					.getKeyAttributesList().stream().map(key -> r.getName() + "." + key + "="
							+ Constants.CAvSAT_WITNESSES_TABLE_NAME + "." + relationName + "_" + key)
					.collect(Collectors.joining(" AND ")));
			String createRelevantViewQuery = sqlQueriesImpl.getCreateRelevantTablesQuery(
					relationName + "," + Constants.CAvSAT_WITNESSES_TABLE_NAME, whereConditions, r.getName());
			System.out.println(createRelevantViewQuery + "\n");
			con.prepareStatement(createRelevantViewQuery).execute();
		}
	}
}