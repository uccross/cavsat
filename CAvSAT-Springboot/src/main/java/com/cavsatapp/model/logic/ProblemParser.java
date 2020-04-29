/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cavsatapp.model.bean.Atom;
import com.cavsatapp.model.bean.DBEnvironment;
import com.cavsatapp.model.bean.Query;
import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.SQLQuery;
import com.cavsatapp.model.bean.Schema;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;
import com.cavsatapp.util.DBUtil;
import com.cavsatapp.util.MSSQLServerImpl;

/**
 * @author Akhil
 *
 */
public class ProblemParser {
	public static Schema parseSchema(DBEnvironment dbEnv, String schemaName) {
		String url = DBUtil.constructConnectionURL(dbEnv, schemaName);
		Connection con = null;
		Schema schema = new Schema();
		Relation relation = null;
		ResultSet resultSet = null;
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		String relationName = "", currRelationName = "", columnName = "", constraintType = "",
				constraintDefinition = "";
		try {
			con = DriverManager.getConnection(url, dbEnv.getUsername(), dbEnv.getPassword());
			if (con != null) {
				resultSet = con.prepareStatement(sqlQueriesImpl.getTablesAndColumnsQuery()).executeQuery();
				while (resultSet.next()) {
					relationName = resultSet.getString(1);
					if (relationName.toUpperCase().contains("FIREWALL"))
						continue;
					columnName = resultSet.getString(2);
					if (currRelationName.equals(relationName)) {
						schema.getRelationByName(relationName).addAttribute(columnName);
					} else {
						currRelationName = relationName;
						relation = new Relation(relationName);
						relation.addAttribute(columnName);
						schema.getRelations().add(relation);
					}
				}

				resultSet = con.prepareStatement(Constants.CAvSAT_GET_CONSTRAINTS_QUERY).executeQuery();
				while (resultSet.next()) {
					constraintType = resultSet.getString(2);
					constraintDefinition = resultSet.getString(3);
					switch (constraintType.toLowerCase()) {
					case "primary key":
						relationName = constraintDefinition.split("\\(")[0];
						relation = schema.getRelationByName(relationName);
						for (String keyAttribute : constraintDefinition.split("\\(")[1].replaceAll("\\)", "")
								.replaceAll(" ", "").split(",")) {
							schema.getRelationByName(relationName)
									.addKeyAttribute(relation.getAttributes().stream().map(String::toLowerCase)
											.collect(Collectors.toList()).indexOf(keyAttribute.toLowerCase()) + 1);
						}
						break;

					default:
						break;
					}
				}
				con.close();
			}
			return schema;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static Query parseQuery(String queryString, Schema schema, String queryLanguage) {
		switch (queryLanguage.toLowerCase()) {
		case "sql":
			return parseQuerySQL(queryString, schema);
		case "fol":
			return parseQueryFOL(queryString, schema);
		default:
			return null;
		}
	}

	private static Query parseQueryFOL(String queryString, Schema schema) {
		Query query = new Query();
		query.setSyntax(queryString);

		if (queryString == null || queryString.isEmpty())
			return null;
		String parts[] = queryString.split(":");
		String head = parts[0];
		String body = parts[1];
		for (String s : head.replaceAll("\\(", "").replaceAll("\\)", "").split(",")) {
			if (!query.getFreeVars().contains(s))
				query.getFreeVars().add(s);
		}
		for (String s : body.split(";")) {
			query.addAtom(parseAtom(s, schema));
		}
		return query;
	}

	public static SQLQuery parseSQLQuery(String sqlSyntax, Schema schema) {
		SQLQuery query = new SQLQuery();
		List<String> selectAttributes, whereConditions = new ArrayList<String>();
		int i = 1;
		String upperSQLSyntax = sqlSyntax.toUpperCase();
		if (upperSQLSyntax.contains("DISTINCT")) {
			query.setSelectDistinct(true);
			upperSQLSyntax.replaceAll("DISTINCT", "");
		}

		List<String> parts = Arrays.asList(upperSQLSyntax.split("(SELECT|FROM|WHERE|GROUP BY|ORDER BY)")).stream()
				.map(str -> str.replaceAll(" ", "")).collect(Collectors.toList());

		selectAttributes = Arrays.asList(parts.get(i++).split(",")).stream().map(str -> str.replaceAll(" ", ""))
				.collect(Collectors.toList());
		for (String attribute : selectAttributes) {
			if (attribute.matches("^(SUM|AVG|MIN|MAX|COUNT)\\(.+\\)")) {
				String[] subparts = attribute.split("[()]");
				query.getAggFunctions().add(subparts[0]);
				query.getAggAttributes().add(subparts[1]);
				query.setAggregate(true);
			}
		}
		query.setSelect(selectAttributes);
		query.setFrom(Arrays.asList(parts.get(i++).split(",")).stream().map(str -> str.replaceAll(" ", ""))
				.collect(Collectors.toList()));

		if (upperSQLSyntax.contains(" WHERE ")) {
			String conditions = parts.get(i++);
			int count = 0;
			int andFinder = 0;
			StringBuilder sb = new StringBuilder("");
			for (char c : conditions.toCharArray()) {
				if (c == '(')
					count++;
				else if (c == ')')
					count--;
				else if (c == ' ' && andFinder == 0)
					andFinder++;
				else if (c == 'A' && andFinder == 1)
					andFinder++;
				else if (c == 'N' && andFinder == 2)
					andFinder++;
				else if (c == 'D' && andFinder == 3)
					andFinder++;
				else if (c == ' ' && andFinder == 4) {
					if (count == 0) {
						whereConditions.add(sb.toString().substring(0, sb.toString().length() - 4));
						andFinder = 0;
						sb = new StringBuilder("");
					} else {
						sb.append(c);
						andFinder = 0;
					}
				} else
					andFinder = 0;
				sb.append(c);
			}
			if (!sb.toString().isEmpty())
				whereConditions.add(sb.toString());
		}
		query.setWhereConditions(whereConditions);

		if (upperSQLSyntax.contains(" GROUP BY "))
			query.setGroupingAttributes(Arrays.asList(parts.get(i++).split(",")).stream()
					.map(str -> str.replaceAll(" ", "")).collect(Collectors.toList()));

		if (upperSQLSyntax.contains("ORDER BY"))
			query.setOrderingAttributes(Arrays.asList(parts.get(i++).split(",")).stream()
					.map(str -> str.replaceAll(" ", "")).collect(Collectors.toList()));
		System.out.println(query.getSQLSyntax());
		return query;
	}

	private static Query parseQuerySQL(String sqlSyntax, Schema schema) {
		List<String> tableNames, selectAttributes, whereConditions = new ArrayList<String>();
		String upperSQLSyntax = sqlSyntax.toUpperCase();
		System.out.println(upperSQLSyntax);
		tableNames = Arrays.asList(upperSQLSyntax.split(" FROM ")[1].split(" WHERE ")[0].replaceAll(" ", "").split(","))
				.stream().map(obj -> ((String) obj).replaceAll(" ", "")).collect(Collectors.toList());

		selectAttributes = Arrays
				.asList(upperSQLSyntax.split("SELECT ")[1].split(" FROM ")[0].replaceAll(" ", "").split(",")).stream()
				.map(obj -> ((String) obj).replaceAll(" ", "")).collect(Collectors.toList());

		if (upperSQLSyntax.contains(" WHERE ")) {
			whereConditions = Arrays.asList(upperSQLSyntax.split(" WHERE ")[1].split(" AND ")).stream()
					.map(obj -> ((String) obj).trim()).collect(Collectors.toList());
		}

		Query query = new Query();
		int varCount = 0;
		Map<String, String> attrVarMap = new HashMap<String, String>();
		for (String condition : whereConditions) {
			String[] arr = condition.split(" = ");
			if (arr.length != 2) // This where condition is not an equi-join, so skip
				continue;
			String first = arr[0].trim();
			String second = arr[1].trim();
			if (first.startsWith("'")) {
				if (attrVarMap.containsKey(second)) {
					attrVarMap = replaceValues(attrVarMap.get(second), first, attrVarMap);
				} else {
					attrVarMap.put(second, first);
				}
			} else if (second.startsWith("'")) {
				if (attrVarMap.containsKey(first)) {
					attrVarMap = replaceValues(attrVarMap.get(first), second, attrVarMap);
				} else {
					attrVarMap.put(first, second);
				}
			} else {
				if (attrVarMap.containsKey(first) && !attrVarMap.containsKey(second)) {
					attrVarMap.put(second, attrVarMap.get(first));
				} else if (!attrVarMap.containsKey(first) && attrVarMap.containsKey(second)) {
					attrVarMap.put(first, attrVarMap.get(second));
				} else if (!attrVarMap.containsKey(first) && !attrVarMap.containsKey(second)) {
					attrVarMap.put(first, "x" + varCount);
					attrVarMap.put(second, "x" + varCount);
					varCount++;
				} else {
					String firstVal = attrVarMap.get(first);
					String secondVal = attrVarMap.get(second);
					if (firstVal.startsWith("'") && !secondVal.startsWith("'")) {
						attrVarMap = replaceValues(secondVal, firstVal, attrVarMap);
					} else if (!firstVal.startsWith("'") && secondVal.startsWith("'")) {
						attrVarMap = replaceValues(firstVal, secondVal, attrVarMap);
					} else if (!firstVal.startsWith("'") && !secondVal.startsWith("'")) {
						attrVarMap = replaceValues(secondVal, firstVal, attrVarMap);
					}
				}
			}
		}
		for (String tableName : tableNames) {
			Relation relation = schema.getRelationByName(tableName);
			Atom atom = new Atom(tableName);
			for (String attribute : relation.getAttributes()) {
				String atomVar;
				if (attrVarMap.containsKey(tableName.toUpperCase() + "." + attribute.toUpperCase())) {
					atomVar = attrVarMap.get(tableName.toUpperCase() + "." + attribute.toUpperCase());
				} else {
					atomVar = "x" + varCount;
					varCount++;
				}
				atom.addVar(atomVar);
				if (selectAttributes.contains(tableName.toUpperCase() + "." + attribute.toUpperCase()))
					query.getFreeVars().add(atomVar);
				if (relation.getKeyAttributesList().contains(attribute))
					atom.addKeyVar(atomVar);
				else
					atom.addNonKeyVar(atomVar);
				if (atomVar.startsWith("'"))
					atom.getConstants().add(atomVar);
			}
			query.addAtom(atom);
		}
		return query;
	}

	private static Map<String, String> replaceValues(String oldVal, String newVal, Map<String, String> map) {
		Map<String, String> replaced = new HashMap<String, String>(map);
		for (String key : map.keySet()) {
			if (replaced.get(key).equals(oldVal)) {
				replaced.replace(key, oldVal, newVal);
			}
		}
		return replaced;
	}

	private static Atom parseAtom(String atomStr, Schema schema) {
		String parts[] = atomStr.split("\\(");
		Atom atom = new Atom(parts[0]);
		Set<Integer> keyIndexes = schema.getRelationByName(parts[0]).getKeyAttributes();
		parts = parts[1].replaceAll("\\)", "").split(",");
		int i = 1;
		for (String s : parts) {
			String var = s;
			if (s.startsWith("'") && s.endsWith("'")) {
				// var = s.replaceAll("'", "");
				atom.getConstants().add(var);
			}
			if (keyIndexes.contains(i++))
				atom.addKeyVar(var);
			else
				atom.addNonKeyVar(var);
			atom.addVar(s);

		}
		return atom;
	}
}
