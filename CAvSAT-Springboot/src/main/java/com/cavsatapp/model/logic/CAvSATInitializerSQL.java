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
public class CAvSATInitializerSQL {
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

	/*
	 * public void createKeysTables(Set<Relation> relations, Connection con) { try {
	 * for (Relation r : relations) { String q =
	 * sqlQueriesImpl.getCreateKeysTableQuery(r.getName(),
	 * r.getAttributesFromIndexesCSV(r.getKeyAttributes(), "")); PreparedStatement
	 * psKeys = con.prepareStatement(q); psKeys.execute(); } } catch (SQLException
	 * e) { e.printStackTrace(); } }
	 * 
	 * public void createAnsFromCons(SQLQuery query, Schema schema, Connection con)
	 * throws SQLException { SQLQuery ansFromConsQuery = new SQLQuery(query); for
	 * (Relation r : schema.getRelationsByNames(new
	 * HashSet<String>(query.getFrom()))) { Set<String> innerWhereConditions = new
	 * HashSet<String>(); for (String keyAttribute : r.getKeyAttributesList()) {
	 * innerWhereConditions.add(r.getName() + "." + keyAttribute + "=" +
	 * Constants.CAvSAT_KEYS_TABLE_PREFIX + r.getName() + "." + keyAttribute); }
	 * ansFromConsQuery.getWhereConditions() .add("NOT EXISTS (SELECT 1 FROM " +
	 * Constants.CAvSAT_KEYS_TABLE_PREFIX + r.getName() + " WHERE " +
	 * innerWhereConditions.stream().collect(Collectors.joining(" AND ")) + ")"); }
	 * ansFromConsQuery.setSelectDistinct(true);
	 * System.out.println(ansFromConsQuery.getSQLSyntax(Constants.
	 * CAvSAT_ANS_FROM_CONS_TABLE_NAME));
	 * con.prepareStatement(ansFromConsQuery.getSQLSyntax(Constants.
	 * CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute(); }
	 */

	public void createAnsFromConsNew(SQLQuery query, Schema schema, Connection con) throws SQLException {
		SQLQuery ansFromConsQuery = new SQLQuery(query);
		ansFromConsQuery.setFrom(query.getFrom().stream()
				.map(relationName -> Constants.CAvSAT_CONS_TABLE_PREFIX + relationName).collect(Collectors.toList()));
		ansFromConsQuery
				.setSelect(
						query.getSelect().stream()
								.map(attribute -> attribute + " AS "
										+ (attribute.matches("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)")
												? attribute.split("[()]")[0]
												: attribute.replaceAll("\\.", "_")))
								.collect(Collectors.toList()));
		List<String> newConditions = new ArrayList<String>();
		for (String condition : query.getWhereConditions()) {
			String newCondition = condition;
			for (String relationName : query.getFrom()) {
				newCondition = newCondition.replaceAll(relationName + "\\.",
						Constants.CAvSAT_CONS_TABLE_PREFIX + relationName + "\\.");
			}
			newConditions.add(newCondition);
		}
		ansFromConsQuery.setWhereConditions(newConditions);
		ansFromConsQuery.setSelectDistinct(true);
		con.prepareStatement(ansFromConsQuery.getSQLSyntax(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute();
	}

	public void createWitnesses(SQLQuery query, Schema schema, Connection con) throws SQLException {
		SQLQuery witnessQuery = new SQLQuery(query);
		Set<String> whereConditions = new HashSet<String>();
		String condition = "NOT EXISTS (SELECT 1 FROM " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + " WHERE ";
		Set<String> innerWhereConditions = new HashSet<String>();
		for (String attribute : witnessQuery.getSelect()) {
			innerWhereConditions
					.add(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + "." + attribute.split("\\.")[1] + "=" + attribute);
		}
		condition += innerWhereConditions.stream().collect(Collectors.joining(" AND ")) + ")";
		whereConditions.add(condition);
		witnessQuery.getWhereConditions().addAll(whereConditions);
		List<String> selectAttributes = new ArrayList<String>();
		for (String relationName : witnessQuery.getFrom()) {
			selectAttributes.add(schema.getRelationByName(relationName).getAttributes().stream()
					.map(attr -> relationName + "." + attr + " AS " + relationName + "_" + attr)
					.collect(Collectors.joining(",")));
		}
		witnessQuery.setSelect(selectAttributes);
		con.prepareStatement(witnessQuery.getSQLSyntax(Constants.CAvSAT_WITNESSES_TABLE_NAME)).execute();
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
