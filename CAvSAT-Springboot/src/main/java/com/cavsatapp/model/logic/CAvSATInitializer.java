/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cavsatapp.model.bean.Atom;
import com.cavsatapp.model.bean.Query;
import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.Schema;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;

import lombok.AllArgsConstructor;

/**
 * @author Akhil
 *
 */
@AllArgsConstructor
public class CAvSATInitializer {

	private CAvSATSQLQueries sqlQueriesImpl;

	public boolean checkCAvSATTablesPrepared(Schema schema, Connection con) {
		try {
			for (Relation relation : schema.getRelations()) {
				PreparedStatement checkTableExists = con.prepareStatement(
						sqlQueriesImpl.getCheckIfTableExistsQuery(Constants.CAvSAT_TBL_PREFIX + relation.getName()));
				if (!checkTableExists.executeQuery().next())
					return false;
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public void attachSequentialFactIDsToRelevantTables(Query query, Connection con) throws SQLException {
		int startFactID = 1;
		String tableName = null;
		for (Atom atom : query.getAtoms()) {
			tableName = Constants.CAvSAT_RELEVANT_TABLE_PREFIX + atom.getName();
			PreparedStatement attachFactID = con
					.prepareStatement(sqlQueriesImpl.getAttachFactIDQuery(tableName, startFactID));
			attachFactID.execute();
			PreparedStatement getMaxFactID = con.prepareStatement(sqlQueriesImpl.getMaxFactIDQuery(tableName));
			ResultSet rs = getMaxFactID.executeQuery();
			rs.next();
			startFactID = rs.getInt(1) + 1;
		}
	}

	public void prepareCAvSATTables(Schema schema, Connection con) throws SQLException {
		int startFactID = 1;
		cleanSchema(schema, con);
		for (Relation relation : schema.getRelations()) {
			PreparedStatement copyTable = con.prepareStatement(sqlQueriesImpl.getSelectIntoQuery(relation.getName(),
					Constants.CAvSAT_TBL_PREFIX + relation.getName()));
			copyTable.executeUpdate();
			PreparedStatement attachFactID = con.prepareStatement(
					sqlQueriesImpl.getAttachFactIDQuery(Constants.CAvSAT_TBL_PREFIX + relation.getName(), startFactID));
			attachFactID.execute();

			PreparedStatement getMaxFactID = con.prepareStatement(
					sqlQueriesImpl.getMaxFactIDQuery(Constants.CAvSAT_TBL_PREFIX + relation.getName()));
			ResultSet rs = getMaxFactID.executeQuery();
			rs.next();
			startFactID = rs.getInt(1) + 1;
		}
	}

	private void cleanSchema(Schema schema, Connection con) throws SQLException {
		for (Relation relation : schema.getRelations()) {
			PreparedStatement dropTable = con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_TBL_PREFIX + relation.getName()));
			dropTable.executeUpdate();
		}
	}

	public void createKeysTables(Set<Relation> relations, Connection con) {
		try {
			for (Relation r : relations) {
				con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_KEYS_TABLE_PREFIX + r.getName()))
						.execute();
				String q = sqlQueriesImpl.getCreateKeysTableQuery(r.getName(),
						r.getAttributesFromIndexesCSV(r.getKeyAttributes(), ""));
				PreparedStatement psKeys = con.prepareStatement(q);
				psKeys.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createAnsFromCons(Query query, Schema schema, Connection con) throws SQLException {
		String csvSelectAttributes = "", csvTables = "", csvGroupingAttributes = "";
		Set<String> whereConditions = new HashSet<String>();
		if (!query.isBoolean()) {
			csvSelectAttributes = query.getFreeVars().stream()
					.map(var -> query.getAttributeFromVar(schema, null, var, -1)).collect(Collectors.joining(","));
		}
		csvTables = query.getAtoms().stream().map(atom -> String.valueOf(atom.getName()))
				.collect(Collectors.joining(","));

		// Forming join conditions
		Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
		for (Atom atom : query.getAtoms()) {
			Relation relation = schema.getRelationByName(atom.getName());
			for (int i = 0; i < atom.getVars().size(); i++) {
				String var = atom.getVars().get(i);
				if (varAttrMap.containsKey(var)) {
					varAttrMap.get(var).add(relation.getName() + "." + relation.getAttributes().get(i));
				} else {
					List<String> list = new ArrayList<String>();
					list.add(relation.getName() + "." + relation.getAttributes().get(i));
					varAttrMap.put(var, list);
				}
				if (atom.getConstants().contains(var))
					varAttrMap.get(var).add(var);
			}
		}
		for (String var : varAttrMap.keySet()) {
			if (varAttrMap.get(var).size() > 1) {
				String first = varAttrMap.get(var).get(0);
				for (int i = 1; i < varAttrMap.get(var).size(); i++) {
					whereConditions.add(first + "=" + varAttrMap.get(var).get(i));
				}
			}
		}

		for (Relation r : schema.getRelationsByNames(query.getParticipatingRelationNames())) {
			Set<String> innerWhereConditions = new HashSet<String>();
			for (String keyAttribute : r.getKeyAttributesList()) {
				innerWhereConditions.add(r.getName() + "." + keyAttribute + "=" + Constants.CAvSAT_KEYS_TABLE_PREFIX
						+ r.getName() + "." + keyAttribute);
			}
			whereConditions.add("NOT EXISTS (SELECT 1 FROM " + Constants.CAvSAT_KEYS_TABLE_PREFIX + r.getName()
					+ " WHERE " + innerWhereConditions.stream().collect(Collectors.joining(" AND ")) + ")");
		}

		if (!query.isBoolean())
			csvGroupingAttributes = query.getFreeVars().stream()
					.map(var -> query.getAttributeFromVar(schema, null, var, -1)).collect(Collectors.joining(","));
		con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute();
		String createAnsFromConsQuery = sqlQueriesImpl.getSelectAnsFromConsQuery(csvSelectAttributes, csvTables,
				whereConditions, csvGroupingAttributes);
		con.prepareStatement(createAnsFromConsQuery).execute();
	}

	public void createWitnesses(Query query, Schema schema, Connection con) throws SQLException {
		String csvTables = "";
		List<String> attributesFromOneAtom = new ArrayList<String>();
		Set<String> whereConditions = new HashSet<String>();
		for (Atom atom : query.getAtoms()) {
			attributesFromOneAtom.add(schema.getRelationByName(atom.getName()).getAttributes().stream()
					.map(attr -> atom.getName() + "." + attr + " AS " + atom.getName() + "_" + attr)
					.collect(Collectors.joining(",")));
		}
		csvTables = query.getAtoms().stream().map(atom -> atom.getName()).collect(Collectors.joining(","));

		// Forming join conditions
		Map<String, List<String>> varAttrMap = new HashMap<String, List<String>>();
		for (Atom atom : query.getAtoms()) {
			Relation relation = schema.getRelationByName(atom.getName());
			for (int i = 0; i < atom.getVars().size(); i++) {
				String var = atom.getVars().get(i);
				if (varAttrMap.containsKey(var)) {
					varAttrMap.get(var).add(relation.getName() + "." + relation.getAttributes().get(i));
				} else {
					List<String> list = new ArrayList<String>();
					list.add(relation.getName() + "." + relation.getAttributes().get(i));
					varAttrMap.put(var, list);

				}
				if (atom.getConstants().contains(var))
					varAttrMap.get(var).add(var);
			}
		}
		for (String var : varAttrMap.keySet()) {
			if (varAttrMap.get(var).size() > 1) {
				String first = varAttrMap.get(var).get(0);
				for (int i = 1; i < varAttrMap.get(var).size(); i++) {
					whereConditions.add(first + "=" + varAttrMap.get(var).get(i));
				}
			}
		}

		/*
		 * if (!query.isBoolean()) { whereConditions.add("(" +
		 * query.getFreeVars().stream().map(var -> query.getAttributeFromVar(schema,
		 * null, var, -1)) .collect(Collectors.joining(",")) +
		 * ") NOT IN (SELECT * FROM " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME +
		 * ")"); }
		 */

		if (!query.isBoolean()) {
			String condition = "NOT EXISTS (SELECT 1 FROM " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + " WHERE ";
			Set<String> innerWhereConditions = new HashSet<String>();
			for (String var : query.getFreeVars()) {
				String attribute = query.getAttributeFromVar(schema, null, var, -1);
				innerWhereConditions.add(
						Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + "." + attribute.split("\\.")[1] + "=" + attribute);
			}
			condition += innerWhereConditions.stream().collect(Collectors.joining(" AND ")) + ")";
			whereConditions.add(condition);
		}

		String createWitnessesQuery = sqlQueriesImpl.getCreateWitnessesQuery(
				attributesFromOneAtom.stream().collect(Collectors.joining(",")), csvTables, whereConditions);
		con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_WITNESSES_TABLE_NAME)).execute();
		PreparedStatement ps = con.prepareStatement(createWitnessesQuery);
		ps.execute();
		SQLWarning warning = ps.getWarnings();
		while (warning != null) {
			System.out.println(warning.getMessage());
			warning = warning.getNextWarning();
		}
	}

	public boolean checkBooleanConsAnswer(Connection con) throws SQLException {
		ResultSet rsCheckBooleanConsAnswer = con.prepareStatement(sqlQueriesImpl.getCheckBooleanAnswerQuery())
				.executeQuery();
		if (rsCheckBooleanConsAnswer.next())
			return rsCheckBooleanConsAnswer.getInt(1) != 0;
		return false;
	}

	public void createRelevantTables(Query query, Schema schema, Connection con) throws SQLException {
		Set<String> whereConditions = new HashSet<String>();
		for (Atom atom : query.getAtoms()) {
			whereConditions.clear();
			Relation r = schema.getRelationByName(atom.getName());
			whereConditions.add(r
					.getKeyAttributesList().stream().map(key -> r.getName() + "." + key + "="
							+ Constants.CAvSAT_WITNESSES_TABLE_NAME + "." + r.getName() + "_" + key)
					.collect(Collectors.joining(" AND ")));
			String createRelevantViewQuery = sqlQueriesImpl.getCreateRelevantTablesQuery(
					atom.getName() + "," + Constants.CAvSAT_WITNESSES_TABLE_NAME, whereConditions, r.getName());
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + r.getName()))
					.execute();
			con.prepareStatement(createRelevantViewQuery).execute();
		}
	}
}
