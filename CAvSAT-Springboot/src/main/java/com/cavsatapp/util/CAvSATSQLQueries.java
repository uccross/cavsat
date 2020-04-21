/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.cavsatapp.model.bean.Schema;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author Akhil
 *
 */
public interface CAvSATSQLQueries {
	public String getDatabasePreviewAsJSON(Schema schema, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException;

	public String getNumberOfRows(String tableName);

	public String getTablePreviewAsJSON(String tableName, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException;

	public String getQueryResultPreviewAsJSON(String query, Connection con, int rowLimit)
			throws JsonProcessingException, SQLException;

	public String getCheckIfTableExistsQuery(String tableName);

	public String getDropTableQuery(String tableName);

	public String getCreateKeysTableQuery(String tableName, String csvKeyAttributes);

	public String getSelectAnsFromConsQuery(String csvAttributes, String csvTables, Set<String> whereConditions,
			String csvGroupingAttributes);

	public String getSelectIntoQuery(String tableFrom, String tableInto);

	public String getAttachFactIDQuery(String tableName, int startFactID);

	public String getMaxFactIDQuery(String tableName);

	public String getSchemasQuery();

	public String getAlphaClausesQuery(String tableName, String csvKeyAttributes);

	public String getAlphaClausesUnOptQuery(String tableName, String csvKeyAttributes);

	public String getTablesAndColumnsQuery();

	public String getColumnsQuery();

	public String getCreateWitnessesQuery(String csvSelectAttributes, String csvTables, Set<String> whereConditions);

	public String getCheckBooleanAnswerQuery();

	public String getCreateRelevantTablesQuery(String csvTables, Set<String> whereConditions, String intoTableName);

	public String getSelectQuery(Set<String> selectAttributes, Set<String> fromTables, String intoTable,
			Set<String> whereConditions);

	public String getDistinctPotentialAnswersQuery(List<String> selectAttributes, Set<String> fromTables,
			String intoTable, Set<String> whereConditions);

	public String getDropColumnQuery(String tableName, String columnName);

	public String getBuildFinalAnswers(List<String> columns);
}
