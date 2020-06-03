/**
 * 
 */
package edu.cavsat.model.logic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cavsat.model.bean.Clause;
import edu.cavsat.model.bean.Relation;
import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.util.CAvSATSQLQueries;
import edu.cavsat.util.Constants;
import edu.cavsat.util.ExecCommand;

/**
 * @author Akhil
 *
 */
public class EncoderForPrimaryKeysAggSQL {
	private Schema schema;
	private CAvSATSQLQueries sqlQueriesImpl;
	private Connection con;
	private BufferedWriter br;
	private int varIndex = 1;
	private Map<Integer, Integer> factIDBoolVarMap;

	public EncoderForPrimaryKeysAggSQL(Schema schema, Connection con, String formulaFileName,
			CAvSATSQLQueries SQLQueriesImpl) throws IOException {
		super();
		this.schema = schema;
		this.con = con;
		this.sqlQueriesImpl = SQLQueriesImpl;
		this.factIDBoolVarMap = new HashMap<Integer, Integer>();
		this.br = new BufferedWriter(new FileWriter(formulaFileName));
	}

	// exactlyOne: true -> Encodes "Exactly one fact from each key-equal group"
	// exactlyOne: false -> Encodes "At least one fact from each key-equal group"
	public void createAlphaClauses(SQLQuery query, boolean exactlyOne) throws IOException, SQLException {
		PreparedStatement psKeyEqualGroups;
		Clause clause = null;
		factIDBoolVarMap.clear();
		Relation r = null;
		for (String relationName : query.getFrom()) {
			r = schema.getRelationByName(relationName);
			String csvKeyAttributes = r.getKeyAttributesList().stream().collect(Collectors.joining(","));
			String alphaClausesQuery = sqlQueriesImpl
					.getAlphaClausesQuery(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + r.getName(), csvKeyAttributes);
			psKeyEqualGroups = con.prepareStatement(alphaClausesQuery);
			ResultSet rsKeyEqualGroups = psKeyEqualGroups.executeQuery();
			String curValue = "", receivedValue = "";
			int factID;
			while (rsKeyEqualGroups.next()) {
				factID = rsKeyEqualGroups.getInt(Constants.CAvSAT_FACTID_COLUMN_NAME);
				Integer xVar = factIDBoolVarMap.getOrDefault(factID, null);
				if (xVar == null) {
					xVar = varIndex;
					factIDBoolVarMap.put(factID, xVar);
					varIndex++;
				}
				receivedValue = "";
				for (int i = 1; i < rsKeyEqualGroups.getMetaData().getColumnCount(); i++) {
					receivedValue += rsKeyEqualGroups.getString(i);
				}
				if (!receivedValue.equals(curValue)) {
					if (null != clause) {
						clause.setDescription("A");
						br.append(clause.getDimacsLine());
						if (exactlyOne)
							encodeAtMostOne(new ArrayList<Integer>(clause.getVars()));
					}
					clause = new Clause();
					clause.addVar(xVar);
					curValue = receivedValue;
				} else {
					clause.addVar(xVar);
				}
			}
			if (null != clause) {
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
				if (exactlyOne)
					encodeAtMostOne(new ArrayList<Integer>(clause.getVars()));
			}
		}
	}

	// Binomial encoding
	private void encodeAtMostOne(List<Integer> vars) throws IOException {
		Clause clause;
		for (int i = 0; i < vars.size() - 1; i++) {
			for (int j = i + 1; j < vars.size(); j++) {
				clause = new Clause();
				clause.addVar(vars.get(i) * -1);
				clause.addVar(vars.get(j) * -1);
				clause.setDescription("A");
				br.append(clause.getDimacsLine());
			}
		}
	}

	public SQLQuery getWitnessesQuery(SQLQuery query, boolean desc) {
		SQLQuery betaQuery = query.getQueryWithoutAggregates();
		for (String relationName : betaQuery.getFrom())
			betaQuery.getSelect().add(relationName + "." + Constants.CAvSAT_FACTID_COLUMN_NAME);
		betaQuery.setFrom(
				query.getFrom().stream().map(relationName -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName)
						.collect(Collectors.toList()));
		betaQuery
				.setSelect(
						betaQuery
								.getSelect().stream().map(attribute -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX
										+ attribute + " AS " + attribute.replaceAll("\\.", "_"))
								.collect(Collectors.toList()));
		for (String attribute : query.getAggAttributes())
			if (!attribute.equals("*")) {
				betaQuery.getSelect().add(
						Constants.CAvSAT_RELEVANT_TABLE_PREFIX + attribute + " AS " + attribute.replaceAll("\\.", "_"));
				betaQuery.getOrderingAttributes().add(Constants.CAvSAT_RELEVANT_TABLE_PREFIX + attribute);
				betaQuery.getOrderDesc().add(desc);
			}
		betaQuery.setSelectDistinct(true);

		List<String> newConditions = new ArrayList<String>();
		String newCondition;
		for (String condition : betaQuery.getWhereConditions()) {
			newCondition = condition;
			for (String relationName : query.getFrom())
				newCondition = newCondition.replaceAll("(?i)" + relationName + "\\.",
						Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName + "\\.");
			newConditions.add(newCondition);
		}
		betaQuery.setWhereConditions(newConditions);
		System.out.println("Beta clauses query:\n" + betaQuery.getSQLSyntax());
		return betaQuery;
	}

	public void createBetaClausesForCount(SQLQuery query) throws SQLException, IOException {
		SQLQuery betaQuery = getWitnessesQuery(query, true); // Second parameter does not matter for Count function
		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		while (rs.next()) {
			Clause beta = new Clause();
			for (String relationName : query.getFrom())
				beta.addVar(
						-1 * factIDBoolVarMap.get(rs.getInt(relationName + "_" + Constants.CAvSAT_FACTID_COLUMN_NAME)));
			beta.setDescription("B S");
			br.append(beta.getDimacsLine());
		}
		br.close();
	}

	/*
	 * public void createBetaClausesForMinMax(SQLQuery query, boolean min) throws
	 * SQLException, IOException { int weight = 1, sum = 0; double prev = min ?
	 * Integer.MAX_VALUE : Integer.MIN_VALUE; SQLQuery betaQuery =
	 * query.getQueryWithoutAggregates(); betaQuery.setFrom(
	 * query.getFrom().stream().map(relationName ->
	 * Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName)
	 * .collect(Collectors.toList())); betaQuery .setSelect( betaQuery
	 * .getSelect().stream().map(attribute -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX
	 * + attribute + " AS " + attribute.replaceAll("\\.", "_"))
	 * .collect(Collectors.toList())); for (String attribute :
	 * query.getAggAttributes()) if (!attribute.equals("*")) {
	 * betaQuery.getSelect().add( Constants.CAvSAT_RELEVANT_TABLE_PREFIX + attribute
	 * + " AS " + attribute.replaceAll("\\.", "_"));
	 * betaQuery.getOrderingAttributes().add(Constants.CAvSAT_RELEVANT_TABLE_PREFIX
	 * + attribute); betaQuery.getOrderDesc().add(min); // DESC for Min, ASC for Max
	 * function } betaQuery.setSelectDistinct(true);
	 * 
	 * List<String> newConditions = new ArrayList<String>(); String newCondition;
	 * for (String condition : betaQuery.getWhereConditions()) { newCondition =
	 * condition; for (String relationName : query.getFrom()) newCondition =
	 * newCondition.replaceAll("(?i)" + relationName + "\\.",
	 * Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName + "\\.");
	 * newConditions.add(newCondition); }
	 * betaQuery.setWhereConditions(newConditions);
	 * System.out.println("Beta clauses query:\n" + betaQuery.getSQLSyntax());
	 * ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
	 * while (rs.next()) { Clause beta = new Clause(); for (String relationName :
	 * query.getFrom()) beta.addVar( -1 *
	 * factIDBoolVarMap.get(rs.getInt(relationName + "_" +
	 * Constants.CAvSAT_FACTID_COLUMN_NAME))); beta.setWeight(weight); sum +=
	 * weight; if ((min && rs.getDouble(2) < prev) || (!min && rs.getDouble(2) >
	 * prev)) weight = sum + 1; prev = rs.getDouble(2);
	 * beta.setDescription("B S W v " + rs.getDouble(2));
	 * br.append(beta.getDimacsLine(true)); } br.close(); }
	 */

	public double computeDifficultBoundMinMaxItr(SQLQuery query, boolean min) throws SQLException, IOException {
		double prev = min ? Integer.MIN_VALUE : Integer.MAX_VALUE, curr = prev;
		BufferedWriter wr = new BufferedWriter(new FileWriter(Constants.FORMULA_FILE_NAME, true));
		SQLQuery betaQuery = getWitnessesQuery(query, !min);
		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		while (rs.next()) {
			curr = rs.getDouble(2);
			if ((min && curr > prev) || (!min && curr < prev)) {
				AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.FORMULA_FILE_NAME);
				if (!ExecCommand.isSAT(Constants.SAT_OUTPUT_FILE_NAME, Constants.MAXSAT_SOLVER_NAME).isSolved()) {
					wr.close();
					return prev;
				}
			}
			Clause beta = new Clause();
			for (String relationName : query.getFrom())
				beta.addVar(
						-1 * factIDBoolVarMap.get(rs.getInt(relationName + "_" + Constants.CAvSAT_FACTID_COLUMN_NAME)));
			beta.setDescription("B S W v " + rs.getDouble(2));
			wr.append(beta.getDimacsLine()).flush();
			prev = curr;
		}
		AnswersComputerAgg.runSolver(Constants.MAXSAT_COMMAND, Constants.FORMULA_FILE_NAME);
		if (!ExecCommand.isSAT(Constants.SAT_OUTPUT_FILE_NAME, Constants.MAXSAT_SOLVER_NAME).isSolved()) {
			wr.close();
			return prev;
		}
		wr.close();
		return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
	}

	public void createBetaClausesForSum(SQLQuery query, boolean lub) throws SQLException, IOException {
		SQLQuery betaQuery = getWitnessesQuery(query, true); // Second parameter does not matter for Count function
		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		List<Clause> list;
		Clause beta;
		while (rs.next()) {
			if (rs.getDouble(2) == 0)
				continue;
			beta = new Clause();
			for (String relationName : query.getFrom())
				beta.addVar(
						-1 * factIDBoolVarMap.get(rs.getInt(relationName + "_" + Constants.CAvSAT_FACTID_COLUMN_NAME)));
			if ((rs.getDouble(2) > 0 && lub) || (rs.getDouble(2) < 0 && !lub)) {
				beta.setDescription("B S v " + Double.toString(rs.getDouble(2)));
				br.append(beta.getDimacsLine());
			} else if ((rs.getDouble(2) < 0 && lub) || (rs.getDouble(2) > 0 && !lub)) {
				list = beta.cnfNeg();
				for (Clause c : list) {
					beta.setDescription("B S N v " + Double.toString(rs.getDouble(2)));
					br.append(c.getDimacsLine());
				}
			}
		}
		br.close();
	}

	/**
	 * Kuegel's encoding from Weighted Partial MinSAT to Weighted Partial MaxSAT
	 */
	public void encodeWPMinSATtoWPMaxSAT() {
		List<String> clauses = new ArrayList<String>();
		String sCurrentLine, infinity = "-1", nVars = "-1", weight = "", previousClause = "", lit;
		String[] parts;
		try {
			BufferedReader br = new BufferedReader(new FileReader(Constants.FORMULA_FILE_NAME));
			if ((sCurrentLine = br.readLine()) != null) {
				parts = sCurrentLine.split(" ");
				nVars = parts[2];
				infinity = parts[4];
			}
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.startsWith(infinity)) {
					clauses.add(sCurrentLine);
				} else {
					parts = sCurrentLine.split(" ");
					weight = parts[0];
					previousClause = "";
					for (int i = 1; i < parts.length; i++) {
						lit = parts[i].trim();
						if (lit.equals("0"))
							break;
						clauses.add(weight + " " + previousClause
								+ (lit.startsWith("-") ? lit.replace("-", "") : "-" + lit) + " 0");
						previousClause = previousClause + lit;
					}
				}
			}
			br.close();

			BufferedWriter wr = new BufferedWriter(new FileWriter(Constants.MIN_TO_MAX_ENCODED_FORMULA_FILE_NAME));
			wr.write("p wcnf " + nVars + " " + clauses.size() + " " + infinity + "\n");
			for (String s : clauses)
				wr.append(s + "\n");
			wr.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public void writeFinalFormulaFile(boolean isBetaWeighted, boolean isPartial) {
		Set<String> clauses = new HashSet<String>();
		int infinity = 1;
		try {
			BufferedReader br = new BufferedReader(new FileReader(Constants.FORMULA_FILE_NAME));
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("W"))
					infinity += Integer.parseInt(sCurrentLine.split(" ")[0]);
				else if (sCurrentLine.contains("S"))
					infinity++;
				clauses.add(sCurrentLine);
			}
			br.close();
			BufferedWriter wr = new BufferedWriter(new FileWriter(Constants.FORMULA_FILE_NAME));
			if (!isPartial && !isBetaWeighted)
				wr.write("p cnf " + (varIndex - 1) + " " + clauses.size() + " " + "\n");
			else
				wr.write("p wcnf " + (varIndex - 1) + " " + clauses.size() + " " + infinity + "\n");

			if (isPartial && isBetaWeighted)
				for (String s : clauses)
					wr.append((s.contains("S") ? "" : infinity + " ") + s + "\n");
			else if (isPartial && !isBetaWeighted)
				for (String s : clauses)
					wr.append((s.contains("S") ? "1" : infinity) + " " + s + "\n");
			else
				for (String s : clauses)
					wr.append(s + "\n");
			wr.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public void closeBufferedReader() {
		try {
			this.br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}