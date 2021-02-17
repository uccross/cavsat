/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.logic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cavsat.model.bean.Relation;
import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.util.CAvSATSQLQueries;
import edu.cavsat.util.Constants;
import lombok.AllArgsConstructor;

/**
 * @author Akhil
 *
 */
@AllArgsConstructor
public class CAvSATInitializerAggSQL {
	private CAvSATSQLQueries sqlQueriesImpl;

	public void attachSequentialFactIDsToRelevantTables(SQLQuery query, Connection con) throws SQLException {
		attachSequentialFactIDs(query.getFrom().stream().map(a -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX + a)
				.collect(Collectors.toList()), con);
	}

	public void attachSequentialFactIDs(List<String> relations, Connection con) throws SQLException {
		int startFactID = 1;
		for (String relationName : relations) {
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
		List<String> aggAttributes = new ArrayList<String>();
		List<String> selectAttributes = new ArrayList<String>();
		List<String> newAggAttributes = new ArrayList<String>();
		ansFromConsQuery.setFrom(query.getFrom().stream()
				.map(relationName -> Constants.CAvSAT_CONS_TABLE_PREFIX + relationName).collect(Collectors.toList()));

		for (String aggAttribute : ansFromConsQuery.getAggAttributes()) {
			if (aggAttribute.equals("*"))
				aggAttributes.add("*");
			else if (aggAttribute.toLowerCase().startsWith("distinct "))
				aggAttributes.add("distinct " + Constants.CAvSAT_CONS_TABLE_PREFIX + aggAttribute.split(" ")[1]);
			else {
				// Fails if one relationName ends with another relationName, e.g., relations acc
				// and custacc
				for (String relationName : query.getFrom())
					aggAttribute = aggAttribute.replaceAll(relationName + ".",
							Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + ".");
				newAggAttributes.add(aggAttribute);
				/*
				 * for (String relationName : query.getFrom()) { if
				 * (aggAttribute.contains(relationName + ".")) {
				 * aggAttributes.add(aggAttribute.replaceAll(relationName + "\\.",
				 * Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + ".")); } }
				 */
				// aggAttributes.add(Constants.CAvSAT_CONS_TABLE_PREFIX + aggAttribute);
			}
		}
		aggAttributes.addAll(newAggAttributes);
		ansFromConsQuery.setAggAttributes(aggAttributes);

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
		// For a boolean query, add 1 to the select clause (MS SQL Server Syntax)
		if (ansFromConsQuery.getSelect().isEmpty())
			ansFromConsQuery.getSelect().add("1 AS " + Constants.BOOL_CONS_ANSWER_COLUMN_NAME);
		ansFromConsQuery.setWhereConditions(newConditions);
		ansFromConsQuery.setSelectDistinct(true);
		con.prepareStatement(ansFromConsQuery.getSQLSyntax(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute();
	}

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
		con.prepareStatement(q1.getSQLSyntax(Constants.CAvSAT_WITNESSES_TABLE_NAME) + " EXCEPT " + q2.getSQLSyntax())
				.execute();
	}

	public void createRelevantTables(SQLQuery query, Schema schema, Connection con) throws SQLException {
		Set<String> whereConditions = new HashSet<String>();
		for (String relationName : query.getFrom()) {
			whereConditions.clear();
			Relation r = schema.getRelationByName(relationName);
			whereConditions.add(r
					.getKeyAttributesList().stream().map(key -> r.getName() + "." + key + "="
							+ Constants.CAvSAT_WITNESSES_TABLE_NAME + "." + relationName + "_" + key)
					.collect(Collectors.joining(" AND ")));
			String createRelevantViewQuery = sqlQueriesImpl.getCreateRelevantTablesQuery(
					relationName + "," + Constants.CAvSAT_WITNESSES_TABLE_NAME, whereConditions, r.getName());
			con.prepareStatement(createRelevantViewQuery).execute();
		}
	}
}