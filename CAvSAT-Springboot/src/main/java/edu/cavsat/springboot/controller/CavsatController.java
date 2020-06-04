/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.springboot.controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cavsat.model.bean.CAvSATConstraint;
import edu.cavsat.model.bean.DBEnvironment;
import edu.cavsat.model.bean.Query;
import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.model.bean.Stats;
import edu.cavsat.model.logic.AnswersComputer;
import edu.cavsat.model.logic.AnswersComputerAgg;
import edu.cavsat.model.logic.CAvSATInitializer;
import edu.cavsat.model.logic.CAvSATInitializerAggSQL;
import edu.cavsat.model.logic.CAvSATInitializerSQL;
import edu.cavsat.model.logic.EncoderForPrimaryKeysAggSQL;
import edu.cavsat.model.logic.EncoderForPrimaryKeysSQL;
import edu.cavsat.model.logic.ProblemParser;
import edu.cavsat.model.logic.QueryAnalyser;
import edu.cavsat.util.CAvSATSQLQueries;
import edu.cavsat.util.Constants;
import edu.cavsat.util.DBUtil;
import edu.cavsat.util.ExecCommand;
import edu.cavsat.util.FileUtil;
import edu.cavsat.util.MSSQLServerImpl;
import lombok.Data;

/**
 * @author Akhil
 *
 */
@RestController
@RequestMapping("/api")
public class CavsatController {
	private Connection con;

	@PostMapping("/get-query-analysis")
	ResponseEntity<?> getGraph(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		Schema schema = ProblemParser.parseSchema(dbEnv, dbEnvWithInput.schemaName);
		SQLQuery sqlQuery = ProblemParser.parseSQLQuery(dbEnvWithInput.querySyntax, schema);
		if (sqlQuery.isAggregate())
			return ResponseEntity.ok().build();
		Query query = ProblemParser.parseQuery(dbEnvWithInput.querySyntax, schema, dbEnvWithInput.queryLanguage);
		try {
			QueryAnalyser qa = new QueryAnalyser();
			String jsonData = qa.analyseQuery(query, sqlQuery, schema);
			return ResponseEntity.ok(jsonData);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping("/check-jdbc-connection")
	ResponseEntity<?> getSchemas(@Valid @RequestBody DBEnvironment dbEnv) {
		DBEnvironment responseDbEnv = dbEnv;
		responseDbEnv.setSchemas(new ArrayList<String>());
		String url = DBUtil.constructConnectionURL(dbEnv, "");
		Connection con = null;
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		try {
			con = DriverManager.getConnection(url, dbEnv.getUsername(), dbEnv.getPassword());
			if (con != null) {
				ResultSet schemas = con.prepareStatement(sqlQueriesImpl.getSchemasQuery()).executeQuery();
				while (schemas.next())
					responseDbEnv.getSchemas().add(schemas.getString(1));
				con.close();
				return ResponseEntity.ok(new ObjectMapper().writeValueAsString(responseDbEnv));
			}
			return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
		} catch (SQLException | JsonProcessingException e) {
			System.out.println(e);
			return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
		}
	}

	@PostMapping("/get-cavsat-constraints")
	ResponseEntity<?> getCavSATConstraints(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		DBEnvironment currDbEnv = dbEnvWithInput.dbEnv;
		String url = DBUtil.constructConnectionURL(currDbEnv, dbEnvWithInput.schemaName);
		Connection con = null;
		try {
			con = DriverManager.getConnection(url, currDbEnv.getUsername(), currDbEnv.getPassword());

			if (con != null) {
				ResultSet rs = con.prepareStatement(Constants.CAvSAT_GET_CONSTRAINTS_QUERY).executeQuery();
				List<CAvSATConstraint> constraints = new ArrayList<CAvSATConstraint>();
				while (rs.next())
					constraints.add(new CAvSATConstraint(rs.getInt(1), rs.getString(2), rs.getString(3)));
				con.close();
				return ResponseEntity.ok(new ObjectMapper().writeValueAsString(constraints));
			}
			return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
		} catch (SQLException | JsonProcessingException e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
		}
	}

	@PostMapping("/prepare-cavsat-tables")
	ResponseEntity<?> prepareCAvSATTables(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		Schema schema = ProblemParser.parseSchema(dbEnv, dbEnvWithInput.schemaName);
		String url = DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName);
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		try {
			Connection con = DriverManager.getConnection(url, dbEnv.getUsername(), dbEnv.getPassword());
			CAvSATInitializer init = new CAvSATInitializer(sqlQueriesImpl);
			init.prepareCAvSATTables(schema, con);
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping("/get-database-preview")
	ResponseEntity<?> getDatabasePreview(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		Schema schema = ProblemParser.parseSchema(dbEnv, dbEnvWithInput.schemaName);
		String url = DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName);
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		try {
			Connection con = DriverManager.getConnection(url, dbEnv.getUsername(), dbEnv.getPassword());
			String jsonData = sqlQueriesImpl.getDatabasePreviewAsJSON(schema, con, 10);
			con.close();
			return ResponseEntity.ok(jsonData);
		} catch (SQLException | JsonProcessingException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping("/run-sat-module")
	ResponseEntity<?> runSATModule(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		Schema schema = ProblemParser.parseSchema(dbEnv, dbEnvWithInput.schemaName);
		SQLQuery sqlQuery = ProblemParser.parseSQLQuery(dbEnvWithInput.querySyntax, schema);
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		try {
			if (con == null)
				con = DriverManager.getConnection(DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName),
						dbEnv.getUsername(), dbEnv.getPassword());
			dropTables(sqlQueriesImpl, sqlQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("SAT solving start at " + new Timestamp(System.currentTimeMillis()));
		if (sqlQuery.isAggregate()) {
			return handleAggQueryViaSAT(schema, sqlQuery);
		} else {
			return handleSPJQueryViaSAT(schema, sqlQuery);
		}
	}

	private ResponseEntity<?> handleAggQueryViaSAT(Schema schema, SQLQuery sqlQuery) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		Map<String, Long> evalTimeData = new LinkedHashMap<String, Long>();
		PreparedStatement psInsert;
		ResultSet rsSelect;
		double[] bounds = null;
		boolean underlyingConsAns = false;
		try {
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_AGG_FINAL_ANSWERS_TABLE_NAME))
					.execute();
			if (sqlQuery.getGroupingAttributes().isEmpty()) {
				SQLQuery underlyingCQ = sqlQuery.getQueryWithoutAggregates();
				System.out.println(underlyingCQ.getSQLSyntax());
				handleSPJQueryViaSAT(schema, underlyingCQ);
				rsSelect = con
						.prepareStatement(sqlQueriesImpl.getNumberOfRows(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME))
						.executeQuery(); // Boolean SPJ query's consistent answer is stored in ans_from_cons table, and
											// final_answers table is never built
				rsSelect.next();
				if (rsSelect.getInt(1) != 0) {
					underlyingConsAns = true;
					dropTables(sqlQueriesImpl, underlyingCQ);
					bounds = handleScalarAggQuery(schema, sqlQuery);
					psInsert = con.prepareStatement(sqlQueriesImpl.insertIntoFinalAnswersAggTable(
							new ArrayList<String>(Arrays.asList(Constants.BOOL_CONS_ANSWER_COLUMN_NAME,
									Constants.GLB_COLUMN_NAME, Constants.LUB_COLUMN_NAME))));
					psInsert.setDouble(2, bounds[0]);
					psInsert.setDouble(3, bounds[1]);
				} else {
					psInsert = con.prepareStatement(sqlQueriesImpl.insertIntoFinalAnswersAggTable(
							Collections.singletonList(Constants.BOOL_CONS_ANSWER_COLUMN_NAME)));
				}
				con.prepareStatement(sqlQueriesImpl
						.createFinalAnswersAggTable(Collections.singletonList(Constants.BOOL_CONS_ANSWER_COLUMN_NAME)))
						.execute();
				psInsert.setString(1, underlyingConsAns ? "1" : "0");
				psInsert.executeUpdate();
			} else {
				SQLQuery underlyingCQ = sqlQuery.getQueryWithoutAggregates();
				SQLQuery groupWiseCQ = sqlQuery.getQueryWithoutGroupBy();
				List<String> answerAttributes = new ArrayList<String>(sqlQuery.getGroupingAttributes());
				answerAttributes.addAll(Arrays.asList(Constants.GLB_COLUMN_NAME, Constants.LUB_COLUMN_NAME));
				handleSPJQueryViaSAT(schema, underlyingCQ);
				con.prepareStatement(sqlQueriesImpl.createFinalAnswersAggTable(sqlQuery.getGroupingAttributes().stream()
						.map(a -> a.split("\\.")[1]).collect(Collectors.toList()))).execute();
				rsSelect = con.prepareStatement("SELECT * FROM " + Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME)
						.executeQuery();

				psInsert = con.prepareStatement(sqlQueriesImpl.insertIntoFinalAnswersAggTable(answerAttributes));
				while (rsSelect.next()) {
					dropTables(sqlQueriesImpl, underlyingCQ);
					groupWiseCQ.setWhereConditions(new ArrayList<String>(sqlQuery.getWhereConditions()));
					for (String attribute : underlyingCQ.getSelect()) {
						groupWiseCQ.getWhereConditions()
								.add(attribute + " = '" + rsSelect.getString(attribute.split("\\.")[1]) + "'");
					}
					con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME))
							.execute();
					bounds = handleScalarAggQuery(schema, groupWiseCQ);
					for (int i = 0; i < sqlQuery.getGroupingAttributes().size(); i++)
						psInsert.setString(i + 1, rsSelect.getString(i + 1));
					psInsert.setDouble(sqlQuery.getGroupingAttributes().size() + 1, bounds[0]);
					psInsert.setDouble(sqlQuery.getGroupingAttributes().size() + 2, bounds[1]);
					psInsert.addBatch();
				}
				psInsert.executeBatch();
			}

			String jsonData = sqlQueriesImpl.getTablePreviewAsJSON(Constants.CAvSAT_AGG_FINAL_ANSWERS_TABLE_NAME, con,
					Constants.PREVIEW_ROW_COUNT);
			node.set("jsonDataPreview", mapper.readValue(jsonData, ObjectNode.class));
			node.set("runningTimeAnalysis",
					wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
			node.put("totalRowCount", 1);
			node.put("previewRowCount", 1);
			node.put("approach", "Partial MaxSAT Solving");
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
		}
	}

	private double[] handleScalarAggQuery(Schema schema, SQLQuery sqlQuery) {
		switch (sqlQuery.getAggFunctions().get(0).toLowerCase()) {
		case "count":
			return handleCountQuery(schema, sqlQuery);
		case "sum":
			return handleSumQuery(schema, sqlQuery);
		case "min":
			return handleMinMaxQueryItr(schema, sqlQuery, true);
		case "max":
			return handleMinMaxQueryItr(schema, sqlQuery, false);
		case "average":
			System.out.println("Average function is not supported");
			return null;
		}
		return null;
	}

	private double[] handleCountQuery(Schema schema, SQLQuery sqlQuery) {
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		CAvSATInitializerAggSQL init = new CAvSATInitializerAggSQL(sqlQueriesImpl);
		int glb = Integer.MIN_VALUE, lub = Integer.MAX_VALUE;
		try {
			EncoderForPrimaryKeysAggSQL encoder = new EncoderForPrimaryKeysAggSQL(schema, con, sqlQueriesImpl);
			AnswersComputerAgg computer = new AnswersComputerAgg();

			init.createAnsFromCons(sqlQuery, schema, con);
			init.createWitnesses(sqlQuery, schema, con);
			init.createRelevantTables(sqlQuery, schema, con);
			init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
			encoder.createAlphaClauses(sqlQuery, true, Constants.FORMULA_FILE_NAME);
			encoder.createBetaClausesForCount(sqlQuery, Constants.FORMULA_FILE_NAME);
			encoder.writeFinalFormulaFile(false, true, Constants.FORMULA_FILE_NAME, Constants.FORMULA_FILE_NAME);
			AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.FORMULA_FILE_NAME,
					Constants.SAT_OUTPUT_FILE_NAME);
			glb = computer.getFalsifiedClausesCount(Constants.FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME));
			encoder.encodeWPMinSATtoWPMaxSAT();
			AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.MIN_TO_MAX_ENCODED_FORMULA_FILE_NAME,
					Constants.SAT_OUTPUT_FILE_NAME);
			lub = computer.getFalsifiedClausesCount(Constants.FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME));

			ResultSet rsSelect = con.prepareStatement(sqlQueriesImpl.getConsAnsAgg()).executeQuery();
			rsSelect.next();
			double ansFromCons = rsSelect.getDouble(1);
			glb += ansFromCons;
			lub += ansFromCons;
			System.out.println("GLB: " + glb + " LUB: " + lub);
			return new double[] { glb, lub };
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/*
	 * private ResponseEntity<?> handleMinMaxQuery(Schema schema, SQLQuery sqlQuery,
	 * boolean min) { ObjectMapper mapper = new ObjectMapper(); ObjectNode node =
	 * mapper.createObjectNode(); CAvSATSQLQueries sqlQueriesImpl = new
	 * MSSQLServerImpl(); CAvSATInitializerAggSQL init = new
	 * CAvSATInitializerAggSQL(sqlQueriesImpl); int glb = Integer.MIN_VALUE, lub =
	 * Integer.MAX_VALUE, bound1, bound2; try { EncoderForPrimaryKeysAggSQL encoder
	 * = new EncoderForPrimaryKeysAggSQL(schema, con, Constants.FORMULA_FILE_NAME,
	 * sqlQueriesImpl);
	 * 
	 * init.createAnsFromCons(sqlQuery, schema, con); init.createWitnesses(sqlQuery,
	 * schema, con); init.createRelevantTables(sqlQuery, schema, con);
	 * init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
	 * encoder.createAlphaClauses(sqlQuery, true);
	 * encoder.createBetaClausesForMinMax(sqlQuery, min);
	 * 
	 * encoder.writeFinalFormulaFile(true, true);
	 * AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND,
	 * Constants.FORMULA_FILE_NAME); bound1 =
	 * AnswersComputerAgg.computeDifficultBoundMinMax(Constants.FORMULA_FILE_NAME,
	 * ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME)); bound2 =
	 * AnswersComputerAgg.computeEasyBoundMinMax(sqlQuery, con); lub = min ? bound1
	 * : bound2; glb = min ? bound2 : bound1;
	 * 
	 * System.out.println("GLB: " + glb + " LUB: " + lub); return
	 * ResponseEntity.ok(mapper.writeValueAsString(node)); } catch (SQLException |
	 * IOException e) { e.printStackTrace(); return
	 * ResponseEntity.status(HttpStatus.NOT_FOUND).build(); } }
	 */

	private double[] handleMinMaxQueryItr(Schema schema, SQLQuery sqlQuery, boolean min) {
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		CAvSATInitializerAggSQL init = new CAvSATInitializerAggSQL(sqlQueriesImpl);
		double glb = Integer.MIN_VALUE, lub = Integer.MAX_VALUE, bound1, bound2;
		try {
			EncoderForPrimaryKeysAggSQL encoder = new EncoderForPrimaryKeysAggSQL(schema, con, sqlQueriesImpl);
			init.createAnsFromCons(sqlQuery, schema, con);
			init.createWitnesses(sqlQuery, schema, con);
			init.createRelevantTables(sqlQuery, schema, con);
			init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
			encoder.createAlphaClauses(sqlQuery, true, Constants.FORMULA_FILE_NAME);
			encoder.writeFinalFormulaFile(false, false, Constants.FORMULA_FILE_NAME, Constants.FORMULA_FILE_NAME);
			bound1 = encoder.computeDifficultBoundMinMaxItr(sqlQuery, min);
			bound2 = AnswersComputerAgg.computeEasyBoundMinMax(sqlQuery, con);
			lub = min ? bound1 : bound2;
			glb = min ? bound2 : bound1;

			ResultSet rsSelect = con.prepareStatement(sqlQueriesImpl.getConsAnsAgg()).executeQuery();
			rsSelect.next();
			double ansFromCons = rsSelect.getDouble(1);
			if (min) {
				glb = Double.min(glb, ansFromCons);
				lub = Double.min(lub, ansFromCons);
			} else {
				glb = Double.max(glb, ansFromCons);
				lub = Double.max(lub, ansFromCons);
			}
			System.out.println("GLB: " + glb + " LUB: " + lub);
			return new double[] { glb, lub };
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private double[] handleSumQuery(Schema schema, SQLQuery sqlQuery) {
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		CAvSATInitializerAggSQL init = new CAvSATInitializerAggSQL(sqlQueriesImpl);
		double glb = Integer.MIN_VALUE, lub = Integer.MAX_VALUE;
		try {
			EncoderForPrimaryKeysAggSQL encoder = new EncoderForPrimaryKeysAggSQL(schema, con, sqlQueriesImpl);
			init.createAnsFromCons(sqlQuery, schema, con);
			init.createWitnesses(sqlQuery, schema, con);
			init.createRelevantTables(sqlQuery, schema, con);
			init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
			encoder.createAlphaClauses(sqlQuery, true, Constants.FORMULA_FILE_NAME);
			FileUtil.copyFileUsingStream(Constants.FORMULA_FILE_NAME, Constants.SECOND_FORMULA_FILE_NAME);

			encoder.createBetaClausesForSum(sqlQuery, true, Constants.FORMULA_FILE_NAME);
			encoder.createBetaClausesForSum(sqlQuery, false, Constants.SECOND_FORMULA_FILE_NAME);

			encoder.writeFinalFormulaFile(true, true, Constants.FORMULA_FILE_NAME, Constants.FORMULA_FILE_NAME);
			encoder.writeFinalFormulaFile(true, true, Constants.SECOND_FORMULA_FILE_NAME,
					Constants.SECOND_FORMULA_FILE_NAME);

			AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.FORMULA_FILE_NAME,
					Constants.SAT_OUTPUT_FILE_NAME);
			AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.SECOND_FORMULA_FILE_NAME,
					Constants.SECOND_SAT_OUTPUT_FILE_NAME);

			lub = AnswersComputerAgg.computeSumLUB(Constants.FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME));
			glb = AnswersComputerAgg.computeSumLUB(Constants.SECOND_FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SECOND_SAT_OUTPUT_FILE_NAME));

			ResultSet rsSelect = con.prepareStatement(sqlQueriesImpl.getConsAnsAgg()).executeQuery();
			rsSelect.next();
			double ansFromCons = rsSelect.getDouble(1);
			glb += ansFromCons;
			lub += ansFromCons;
			System.out.println("GLB: " + glb + " LUB: " + lub);
			return new double[] { glb, lub };
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private ResponseEntity<?> handleSPJQueryViaSAT(Schema schema, SQLQuery sqlQuery) {
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		Map<String, Long> evalTimeData = new LinkedHashMap<String, Long>();
		long start, globalStart;
		try {
			CAvSATInitializerSQL init = new CAvSATInitializerSQL(sqlQueriesImpl);
			AnswersComputer computer = new AnswersComputer(con);
			EncoderForPrimaryKeysSQL encoder = new EncoderForPrimaryKeysSQL(schema, con, Constants.FORMULA_FILE_NAME,
					sqlQueriesImpl);
			start = System.currentTimeMillis();
			globalStart = start;
			init.createAnsFromConsNew(sqlQuery, schema, con);
			evalTimeData.put("Time to compute answers from the consistent part of the database (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			if (sqlQuery.getSelect().isEmpty() && init.checkBooleanConsAnswer(con)) {
				long totalEvaluationTime = System.currentTimeMillis() - globalStart;
				evalTimeData.put("Total Evaluation Time (ms)", totalEvaluationTime);
				node.put("totalEvaluationTime", totalEvaluationTime);
				String jsonData = sqlQueriesImpl.getTablePreviewAsJSON(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME, con,
						Constants.PREVIEW_ROW_COUNT);
				node.set("jsonDataPreview", mapper.readValue(jsonData, ObjectNode.class));
				node.set("runningTimeAnalysis",
						wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
				node.put("totalRowCount", 1);
				node.put("previewRowCount", 1);
				node.put("approach", "Consistent part of the DB");
				return ResponseEntity.ok(mapper.writeValueAsString(node));
			}

			init.createWitnesses(sqlQuery, schema, con);
			evalTimeData.put("Time to compute minimal witnesses to the query (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			init.createRelevantTables(sqlQuery, schema, con);
			evalTimeData.put("Time to compute relevant facts (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
			evalTimeData.put("Time to attach FactIDs to the relevant facts (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			encoder.createAlphaClausesOpt(sqlQuery);
			evalTimeData.put("Time to create positive clauses from key-equal groups (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			encoder.createBetaClausesOpt(sqlQuery);
			evalTimeData.put("Time to create negative clauses from minimal witnesses (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();
			String infinity;

			if (sqlQuery.getSelect().size() == 0) {
				infinity = encoder.writeFinalFormulaFile(Constants.FORMULA_FILE_NAME, false);
				Stats stats = computer.computeBooleanAnswer(Constants.FORMULA_FILE_NAME, "MaxHS");
				long totalEvaluationTime = System.currentTimeMillis() - globalStart;
				evalTimeData.put("Total Evaluation Time (ms)", totalEvaluationTime);
				node.put("totalEvaluationTime", totalEvaluationTime);
				String jsonData;
				if (!stats.isSolved())
					con.prepareStatement("insert into " + Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME + " values (1)")
							.execute();
				jsonData = sqlQueriesImpl.getTablePreviewAsJSON(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME, con,
						stats.isSolved() ? 0 : 1);
				node.put("totalRowCount", stats.isSolved() ? 0 : 1);
				node.put("previewRowCount", stats.isSolved() ? 0 : 1);
				node.set("jsonDataPreview", mapper.readValue(jsonData, ObjectNode.class));
				node.set("runningTimeAnalysis",
						wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
				node.put("approach", "SAT Solving");
				return ResponseEntity.ok(mapper.writeValueAsString(node));
			} else {
				infinity = encoder.writeFinalFormulaFile(Constants.FORMULA_FILE_NAME, true);
			}
			evalTimeData.put("Time to write the clauses to a DIMAC file (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			long satTime = computer.eliminatePotentialAnswersInMemory(Constants.FORMULA_FILE_NAME, infinity);
			evalTimeData.put("Time to eliminate inconsistent potential answers (ms)",
					System.currentTimeMillis() - start);
			evalTimeData.put("Total SAT-solving time (ms)", satTime);
			start = System.currentTimeMillis();

			computer.buildFinalAnswers(sqlQueriesImpl);
			evalTimeData.put("Time to write the final consistent answers to a table (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			int totalRowCount = computer.getRowCount(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME, sqlQueriesImpl);
			String jsonData = sqlQueriesImpl.getTablePreviewAsJSON(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME, con,
					Constants.PREVIEW_ROW_COUNT);
			long totalEvaluationTime = System.currentTimeMillis() - globalStart;
			evalTimeData.put("Total Evaluation Time (ms)", totalEvaluationTime);

			node.put("totalEvaluationTime", totalEvaluationTime);
			node.set("jsonDataPreview", mapper.readValue(jsonData, ObjectNode.class));
			node.set("runningTimeAnalysis",
					wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
			node.put("totalRowCount", totalRowCount);
			node.put("previewRowCount",
					totalRowCount < Constants.PREVIEW_ROW_COUNT ? totalRowCount : Constants.PREVIEW_ROW_COUNT);
			node.put("approach", "Partial MaxSAT Solving");
			System.out.println("SAT solving end at " + new Timestamp(System.currentTimeMillis()));
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	/*
	 * @PostMapping("/run-sat-module-unopt") ResponseEntity<?>
	 * runSATModuleUnOpt(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
	 * System.out.println("SAT solving UnOpt start at " + new
	 * Timestamp(System.currentTimeMillis())); DBEnvironment dbEnv =
	 * dbEnvWithInput.dbEnv; Schema schema = ProblemParser.parseSchema(dbEnv,
	 * dbEnvWithInput.schemaName); SQLQuery sqlQuery =
	 * ProblemParser.parseSQLQuery(dbEnvWithInput.querySyntax, schema);
	 * CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl(); ObjectMapper mapper
	 * = new ObjectMapper(); ObjectNode node = mapper.createObjectNode(); long
	 * start, globalStart; try { if (con == null) con =
	 * DriverManager.getConnection(DBUtil.constructConnectionURL(dbEnv,
	 * dbEnvWithInput.schemaName), dbEnv.getUsername(), dbEnv.getPassword());
	 * dropTables(sqlQueriesImpl, sqlQuery); AnswersComputer computer = new
	 * AnswersComputer(con); EncoderForPrimaryKeysSQL encoder = new
	 * EncoderForPrimaryKeysSQL(schema, con, Constants.FORMULA_FILE_NAME,
	 * sqlQueriesImpl); Map<String, Long> evalTimeData = new LinkedHashMap<String,
	 * Long>(); start = System.currentTimeMillis(); globalStart = start;
	 * 
	 * encoder.createAlphaClausesUnOpt(sqlQuery);
	 * evalTimeData.put("Time to create positive clauses from key-equal groups (ms)"
	 * , System.currentTimeMillis() - start); start = System.currentTimeMillis();
	 * 
	 * encoder.createBetaClausesUnOpt(sqlQuery); evalTimeData.
	 * put("Time to create negative clauses from minimal witnesses (ms)",
	 * System.currentTimeMillis() - start); start = System.currentTimeMillis();
	 * 
	 * String infinity = encoder.writeFinalFormulaFile(Constants.FORMULA_FILE_NAME,
	 * false); evalTimeData.put("Time to write the clauses to a DIMAC file (ms)",
	 * System.currentTimeMillis() - start); start = System.currentTimeMillis();
	 * 
	 * long satTime =
	 * computer.eliminatePotentialAnswersUnOptInMemory(Constants.FORMULA_FILE_NAME,
	 * infinity);
	 * evalTimeData.put("Time to eliminate inconsistent potential answers (ms)",
	 * System.currentTimeMillis() - start);
	 * evalTimeData.put("Total SAT-solving time (ms)", satTime); start =
	 * System.currentTimeMillis();
	 * 
	 * computer.buildFinalAnswersUnOpt(sqlQueriesImpl);
	 * evalTimeData.put("Time to write the final consistent answers to a table (ms)"
	 * , System.currentTimeMillis() - start); start = System.currentTimeMillis();
	 * 
	 * int totalRowCount =
	 * computer.getRowCount(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME,
	 * sqlQueriesImpl); String jsonData =
	 * sqlQueriesImpl.getTablePreviewAsJSON(Constants.
	 * CAvSAT_FINAL_ANSWERS_TABLE_NAME, con, Constants.PREVIEW_ROW_COUNT); long
	 * totalEvaluationTime = System.currentTimeMillis() - globalStart;
	 * evalTimeData.put("Total Evaluation Time (ms)", totalEvaluationTime);
	 * 
	 * node.put("totalEvaluationTime", totalEvaluationTime);
	 * node.set("jsonDataPreview", mapper.readValue(jsonData, ObjectNode.class));
	 * node.set("runningTimeAnalysis",
	 * wrapAttributeValueDataForBootstrapTable(evalTimeData,
	 * "Running Time Analysis")); node.put("totalRowCount", totalRowCount);
	 * node.put("previewRowCount", totalRowCount < Constants.PREVIEW_ROW_COUNT ?
	 * totalRowCount : Constants.PREVIEW_ROW_COUNT); node.put("approach",
	 * "Partial MaxSAT Solving"); System.out.println("SAT solving end at " + new
	 * Timestamp(System.currentTimeMillis())); return
	 * ResponseEntity.ok(mapper.writeValueAsString(node)); } catch (SQLException |
	 * IOException e) { e.printStackTrace(); } return ResponseEntity.ok().build(); }
	 */

	@PostMapping("/run-conquer-rewriting")
	ResponseEntity<?> runConQuerSQLRewriting(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		System.out.println("ConQuer start at " + new Timestamp(System.currentTimeMillis()));
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		long start;
		int previewRowCount = 100;
		Map<String, Long> evalTimeData = new TreeMap<String, Long>();
		try {
			if (con == null)
				con = DriverManager.getConnection(DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName),
						dbEnv.getUsername(), dbEnv.getPassword());
			AnswersComputer computer = new AnswersComputer(con);
			start = System.currentTimeMillis();
			String jsonData = computer.computeSQLQueryAnswers(dbEnvWithInput.conQuerSQLRewriting, sqlQueriesImpl,
					previewRowCount);
			long runnningtime = System.currentTimeMillis() - start;
			evalTimeData.put("Time to run ConQuer SQL Rewriting (ms)", runnningtime);
			ObjectNode data = mapper.readValue(jsonData, ObjectNode.class);
			int totalRowCount = data.get("rowCount").asInt(-1);
			data.remove("rowCount");
			node.set("jsonDataPreview", data);
			node.put("totalEvaluationTime", runnningtime);
			node.put("totalRowCount", totalRowCount);
			node.put("previewRowCount",
					totalRowCount < Constants.PREVIEW_ROW_COUNT ? totalRowCount : Constants.PREVIEW_ROW_COUNT);
			node.set("runningTimeAnalysis",
					wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
			node.put("approach", "ConQuer SQL Rewriting");
			System.out.println("ConQuer end at " + new Timestamp(System.currentTimeMillis()));
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping("/run-kw-rewriting")
	ResponseEntity<?> runKWSQLRewriting(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		System.out.println("KW start at " + new Timestamp(System.currentTimeMillis()));
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		long start;
		int previewRowCount = 100;
		Map<String, Long> evalTimeData = new TreeMap<String, Long>();
		try {
			if (con == null)
				con = DriverManager.getConnection(DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName),
						dbEnv.getUsername(), dbEnv.getPassword());
			AnswersComputer computer = new AnswersComputer(con);
			start = System.currentTimeMillis();
			String jsonData = computer.computeSQLQueryAnswers(dbEnvWithInput.kwSQLRewriting, sqlQueriesImpl,
					previewRowCount);
			long runnningtime = System.currentTimeMillis() - start;
			evalTimeData.put("Time to run Koutris-Wijsen SQL Rewriting (ms)", runnningtime);
			ObjectNode dataObject = mapper.readValue(jsonData, ObjectNode.class);
			int totalRowCount = dataObject.get("rowCount").asInt(-1);
			dataObject.remove("rowCount");
			node.set("jsonDataPreview", dataObject);
			node.put("totalEvaluationTime", runnningtime);
			node.put("totalRowCount", totalRowCount);
			node.put("previewRowCount",
					totalRowCount < Constants.PREVIEW_ROW_COUNT ? totalRowCount : Constants.PREVIEW_ROW_COUNT);
			node.set("runningTimeAnalysis",
					wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
			node.put("approach", "Koutris-Wijsen SQL Rewriting");
			System.out.println("KW end at " + new Timestamp(System.currentTimeMillis()));
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping("/compute-potential-answers")
	ResponseEntity<?> computePotentialAnswers(@Valid @RequestBody DBEnvWithInput dbEnvWithInput) {
		System.out.println("Pot start at " + new Timestamp(System.currentTimeMillis()));
		DBEnvironment dbEnv = dbEnvWithInput.dbEnv;
		Schema schema = ProblemParser.parseSchema(dbEnv, dbEnvWithInput.schemaName);
		SQLQuery sqlQuery = ProblemParser.parseSQLQuery(dbEnvWithInput.querySyntax, schema);
		String url = DBUtil.constructConnectionURL(dbEnv, dbEnvWithInput.schemaName);
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		Map<String, Long> evalTimeData = new TreeMap<String, Long>();
		long start;
		int previewRowCount = 100;
		try {
			if (con == null)
				con = DriverManager.getConnection(url, dbEnv.getUsername(), dbEnv.getPassword());
			AnswersComputer computer = new AnswersComputer(con);
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode node = mapper.createObjectNode();
			start = System.currentTimeMillis();
			if (sqlQuery.getSelect().size() == 0)
				sqlQuery.getSelect().add("1 AS " + Constants.BOOL_CONS_ANSWER_COLUMN_NAME);
			String jsonData = computer.computeSQLQueryAnswers(sqlQuery.getSQLSyntax(), sqlQueriesImpl, previewRowCount);

			ObjectNode dataObject = mapper.readValue(jsonData, ObjectNode.class);
			int totalRowCount = dataObject.get("rowCount").asInt(-1);
			dataObject.remove("rowCount");
			long totalEvaluationTime = System.currentTimeMillis() - start;
			evalTimeData.put("Time to compute potential answers (ms)", totalEvaluationTime);
			node.set("jsonDataPreview", dataObject);
			node.put("totalEvaluationTime", totalEvaluationTime);
			node.put("totalRowCount", totalRowCount);
			node.put("previewRowCount",
					totalRowCount < Constants.PREVIEW_ROW_COUNT ? totalRowCount : Constants.PREVIEW_ROW_COUNT);
			node.set("runningTimeAnalysis",
					wrapAttributeValueDataForBootstrapTable(evalTimeData, "Running Time Analysis"));
			System.out.println("Pot end at " + new Timestamp(System.currentTimeMillis()));
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
	}

	private void dropTables(CAvSATSQLQueries sqlQueriesImpl, SQLQuery query) {
		try {
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_ANS_FROM_CONS_TABLE_NAME)).execute();
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_WITNESSES_TABLE_NAME)).execute();
			for (String relationName : query.getFrom()) {
				con.prepareStatement(
						sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName))
						.execute();
			}
			con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
					.execute();
			con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
					.execute();
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME))
					.execute();
			con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME))
					.execute();
			con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME))
					.execute();
			con.prepareStatement(
					sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_ALL_DISTINCT_POTENTIAL_ANS_TABLE_NAME)).execute();
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery("CAVSAT_CONSISTENT_PVARS")).execute();
			con.prepareStatement(sqlQueriesImpl.getDropTableQuery(Constants.CAvSAT_FINAL_ANSWERS_TABLE_NAME)).execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private ObjectNode wrapAttributeValueDataForBootstrapTable(Map<String, Long> map, String tableName) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		ArrayNode columns = mapper.createArrayNode();
		ObjectNode columnMeta = mapper.createObjectNode();
		columnMeta.put("dataField", "attr");
		columnMeta.put("text", "Attribute");
		columns.add(columnMeta);
		columnMeta = mapper.createObjectNode();
		columnMeta.put("dataField", "value");
		columnMeta.put("text", "Value");
		columns.add(columnMeta);
		ArrayNode data = mapper.createArrayNode();

		node.put("name", tableName);
		node.putPOJO("columns", columns);
		for (String key : map.keySet()) {
			ObjectNode row = mapper.createObjectNode();
			row.put("attr", key);
			row.put("value", map.get(key));
			data.add(row);
		}
		node.put("name", tableName);
		node.putPOJO("columns", columns);
		node.putPOJO("data", data);
		return node;
	}

	@Data
	private static class DBEnvWithInput {
		private DBEnvironment dbEnv;
		private String schemaName;
		private String querySyntax;
		private String queryLanguage;
		private String conQuerSQLRewriting;
		private String kwSQLRewriting;
	}
}
