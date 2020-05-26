/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.springboot.controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
			return handleAggregationQueryViaSAT(schema, sqlQuery);
		} else {
			return handleSPJQueryViaSAT(schema, sqlQuery);
		}
	}

	private ResponseEntity<?> handleAggregationQueryViaSAT(Schema schema, SQLQuery sqlQuery) {
		CAvSATSQLQueries sqlQueriesImpl = new MSSQLServerImpl();
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		Map<String, Long> evalTimeData = new LinkedHashMap<String, Long>();
		long start;
		try {
			CAvSATInitializerAggSQL init = new CAvSATInitializerAggSQL(sqlQueriesImpl);
			EncoderForPrimaryKeysAggSQL encoder = new EncoderForPrimaryKeysAggSQL(schema, con,
					Constants.FORMULA_FILE_NAME, sqlQueriesImpl);
			AnswersComputerAgg computer = new AnswersComputerAgg();
			start = System.currentTimeMillis();

			init.createAnsFromCons(sqlQuery, schema, con);
			evalTimeData.put("Time to compute answers from the consistent part of the database (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			init.createWitnesses(sqlQuery, schema, con);
			evalTimeData.put("Time to compute minimal witnesses to the query (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			init.createRelevantTables(sqlQuery, schema, con);
			evalTimeData.put("Time to compute relevant facts (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			init.attachSequentialFactIDsToRelevantTables(sqlQuery, con);
			evalTimeData.put("Time to attach FactIDs to the relevant facts (ms)", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			encoder.createAlphaClauses(sqlQuery, true);
			evalTimeData.put("Time to create positive clauses from key-equal groups (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			encoder.createBetaClauses(sqlQuery);
			evalTimeData.put("Time to create negative clauses from minimal witnesses (ms)",
					System.currentTimeMillis() - start);
			start = System.currentTimeMillis();

			encoder.writeFinalFormulaFile(false);
			computer.runSolver(Constants.MAXSAT_COMMAND, Constants.FORMULA_FILE_NAME);
			int glb = computer.getFalsifiedClausesCount(Constants.FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME));
			encoder.encodeWPMinSATtoWPMaxSAT();
			computer.runSolver(Constants.MAXSAT_COMMAND, Constants.MIN_TO_MAX_ENCODED_FORMULA_FILE_NAME);
			int lub = computer.getFalsifiedClausesCount(Constants.FORMULA_FILE_NAME,
					ExecCommand.readOutput(Constants.SAT_OUTPUT_FILE_NAME));
			System.out.println("GLB: " + glb + " LUB: " + lub);
			return ResponseEntity.ok(mapper.writeValueAsString(node));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok().build();
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
