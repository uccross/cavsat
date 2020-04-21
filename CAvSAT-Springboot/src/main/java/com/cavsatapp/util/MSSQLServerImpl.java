/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.Schema;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Akhil
 *
 */
public class MSSQLServerImpl implements CAvSATSQLQueries {

	@Override
	public String getDropTableQuery(String tableName) {
		return "DROP TABLE IF EXISTS " + tableName;
	}

	@Override
	public String getCreateKeysTableQuery(String tableName, String csvKeyAttributes) {
		return "SELECT " + csvKeyAttributes + " INTO " + Constants.CAvSAT_KEYS_TABLE_PREFIX + tableName + " FROM "
				+ tableName + " GROUP BY " + csvKeyAttributes + " HAVING COUNT(*) > 1";
	}

	@Override
	public String getSelectIntoQuery(String tableFrom, String tableInto) {
		return "SELECT * INTO " + tableInto + " FROM " + tableFrom;
	}

	@Override
	public String getAttachFactIDQuery(String tableName, int startFactID) {
		return "ALTER TABLE " + tableName + " ADD " + Constants.CAvSAT_FACTID_COLUMN_NAME + " INT IDENTITY ("
				+ startFactID + ",1) CONSTRAINT PK_CAVSAT_" + tableName + "_FACTID PRIMARY KEY CLUSTERED";
	}

	@Override
	public String getMaxFactIDQuery(String tableName) {
		return "SELECT MAX(" + Constants.CAvSAT_FACTID_COLUMN_NAME + ") FROM " + tableName;
	}

	@Override
	public String getSchemasQuery() {
		return "SELECT name FROM sys.databases";
	}

	@Override
	public String getAlphaClausesQuery(String tableName, String csvKeyAttributes) {
		return "SELECT " + csvKeyAttributes + ", " + Constants.CAvSAT_FACTID_COLUMN_NAME + " FROM " + tableName
				+ " ORDER BY " + csvKeyAttributes;
	}

	@Override
	public String getAlphaClausesUnOptQuery(String tableName, String csvKeyAttributes) {
		return "SELECT " + csvKeyAttributes + ", " + Constants.CAvSAT_UNOPT_FACTID_COLUMN_NAME + " FROM " + tableName
				+ " ORDER BY " + csvKeyAttributes;
	}

	@Override
	public String getTablesAndColumnsQuery() {
		return "SELECT TABLE_NAME, COLUMN_NAME FROM	INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME NOT LIKE '"
				+ Constants.CAvSAT_TBL_PREFIX + "%' ORDER BY TABLE_NAME, ORDINAL_POSITION";
	}

	@Override
	public String getColumnsQuery() {
		return "SELECT TABLE_NAME, COLUMN_NAME FROM	INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";
	}

	@Override
	public String getCheckIfTableExistsQuery(String tableName) {
		return "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
	}

	@Override
	public String getSelectAnsFromConsQuery(String csvSelectAttributes, String csvTables, Set<String> whereConditions,
			String csvGroupingAttributes) {
		String query = "SELECT ";
		if (csvSelectAttributes.isEmpty())
			query += "1 ";
		else
			query += csvSelectAttributes;
		query += " INTO " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + " FROM " + csvTables;
		if (!whereConditions.isEmpty())
			query += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		if (!csvGroupingAttributes.isEmpty())
			query += " GROUP BY " + csvGroupingAttributes;
		return query;
	}

	@Override
	public String getCreateWitnessesQuery(String csvSelectAttributes, String csvTables, Set<String> whereConditions) {
		String query = "SELECT " + csvSelectAttributes + " INTO " + Constants.CAvSAT_WITNESSES_TABLE_NAME + " FROM "
				+ csvTables;
		if (!whereConditions.isEmpty())
			query += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		return query;
	}

	@Override
	public String getCheckBooleanAnswerQuery() {
		return "SELECT COUNT(*) FROM " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME;
	}

	@Override
	public String getCreateRelevantTablesQuery(String csvTables, Set<String> whereConditions, String intoTableName) {
		String query = "SELECT DISTINCT " + intoTableName + ".* INTO " + Constants.CAvSAT_RELEVANT_TABLE_PREFIX
				+ intoTableName + " FROM " + csvTables;
		if (!whereConditions.isEmpty())
			query += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		return query;
	}

	@Override
	public String getSelectQuery(Set<String> selectAttributes, Set<String> fromTables, String intoTable,
			Set<String> whereConditions) {
		String query = "SELECT DISTINCT " + selectAttributes.stream().collect(Collectors.joining(","));
		if (intoTable != null)
			query += " " + intoTable;
		query += " FROM " + fromTables.stream().collect(Collectors.joining(","));
		if (!whereConditions.isEmpty())
			query += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		return query;
	}

	@Override
	public String getDistinctPotentialAnswersQuery(List<String> selectAttributes, Set<String> fromTables,
			String intoTable, Set<String> whereConditions) {
		String query = "SELECT DISTINCT " + selectAttributes.stream().collect(Collectors.joining(","));
		if (intoTable != null)
			query += " INTO " + intoTable;
		query += " FROM " + fromTables.stream().collect(Collectors.joining(","));
		if (!whereConditions.isEmpty())
			query += " WHERE " + whereConditions.stream().collect(Collectors.joining(" AND "));
		return query;
	}

	@Override
	public String getDatabasePreviewAsJSON(Schema schema, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException {
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode root = mapper.createArrayNode();
		for (Relation relation : schema.getRelations()) {
			ObjectNode table = mapper.createObjectNode();
			ArrayNode columns = mapper.createArrayNode();
			ArrayNode data = mapper.createArrayNode();

			for (String attr : relation.getAttributes()) {
				ObjectNode columnMeta = mapper.createObjectNode();
				columnMeta.put("dataField", attr);
				columnMeta.put("text", attr);
				columns.add(columnMeta);
			}
			ResultSet rs = con.prepareStatement("SELECT TOP " + rowLimit + " * FROM " + relation.getName())
					.executeQuery();
			while (rs.next()) {
				ObjectNode row = mapper.createObjectNode();
				for (String attr : relation.getAttributes()) {
					row.put(attr, rs.getString(attr));
				}
				data.add(row);
			}
			table.put("name", relation.getName());
			table.putPOJO("columns", columns);
			table.putPOJO("data", data);
			root.add(table);
		}
		return mapper.writeValueAsString(root);
	}

	@Override
	public String getTablePreviewAsJSON(String tableName, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode table = mapper.createObjectNode();
		ArrayNode columns = mapper.createArrayNode();
		ArrayNode data = mapper.createArrayNode();

		PreparedStatement psColumns = con.prepareStatement(getColumnsQuery());
		psColumns.setString(1, tableName);
		ResultSet rsColumns = psColumns.executeQuery();
		Set<String> attributes = new HashSet<String>();
		while (rsColumns.next()) {
			ObjectNode columnMeta = mapper.createObjectNode();
			columnMeta.put("dataField", rsColumns.getString(2));
			columnMeta.put("text", rsColumns.getString(2));
			columns.add(columnMeta);
			attributes.add(rsColumns.getString(2));
		}

		ResultSet rs = con.prepareStatement("SELECT TOP " + rowLimit + " * FROM " + tableName).executeQuery();
		while (rs.next()) {
			ObjectNode row = mapper.createObjectNode();
			for (String attr : attributes) {
				row.put(attr, rs.getString(attr));
			}
			data.add(row);
		}
		table.put("name", tableName);
		table.putPOJO("columns", columns);
		table.putPOJO("data", data);
		return mapper.writeValueAsString(table);
	}

	@Override
	public String getQueryResultPreviewAsJSON(String query, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode table = mapper.createObjectNode();
		ArrayNode columns = mapper.createArrayNode();
		ArrayNode data = mapper.createArrayNode();

		ResultSet rs = con.prepareStatement(query).executeQuery();
		Set<String> attributes = new HashSet<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			ObjectNode columnMeta = mapper.createObjectNode();
			columnMeta.put("dataField", rsmd.getColumnLabel(i));
			columnMeta.put("text", rsmd.getColumnLabel(i));
			columns.add(columnMeta);
			attributes.add(rsmd.getColumnLabel(i));
		}
		int count = 0;
		while (rs.next()) {
			if (rowLimit > 0) {
				ObjectNode row = mapper.createObjectNode();
				for (String attr : attributes) {
					row.put(attr, rs.getString(attr));
				}
				data.add(row);
				rowLimit--;
			}
			count++;
		}
		table.put("name", "Result");
		table.putPOJO("columns", columns);
		table.putPOJO("data", data);
		table.put("rowCount", count);
		return mapper.writeValueAsString(table);
	}

	@Override
	public String getBuildFinalAnswers(List<String> columns) {
		return "SELECT * INTO " + Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME + " FROM (SELECT * FROM "
				+ Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + " UNION SELECT " + String.join(",", columns) + " FROM "
				+ Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME + ") a";
	}

	@Override
	public String getDropColumnQuery(String tableName, String columnName) {
		return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName;
	}

	@Override
	public String getNumberOfRows(String tableName) {
		return "SELECT COUNT(*) FROM " + tableName;
	}
}
