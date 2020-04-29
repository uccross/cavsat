/**
 * 
 */
package com.cavsatapp.model.logic;

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

import com.cavsatapp.model.bean.Clause;
import com.cavsatapp.model.bean.Relation;
import com.cavsatapp.model.bean.SQLQuery;
import com.cavsatapp.model.bean.Schema;
import com.cavsatapp.util.CAvSATSQLQueries;
import com.cavsatapp.util.Constants;

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

	public void createAlphaClausesOpt(SQLQuery query) throws IOException, SQLException {
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
			}
		}
	}

	public void createBetaClausesOpt(SQLQuery query) throws SQLException, IOException {
		SQLQuery betaQuery = query.getQueryWithoutAggregates();
		betaQuery.setFrom(
				query.getFrom().stream().map(relationName -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX + relationName)
						.collect(Collectors.toList()));
		betaQuery.setSelect(query.getSelect().stream().map(attribute -> Constants.CAvSAT_RELEVANT_TABLE_PREFIX
				+ attribute + " AS " + attribute.replaceAll("\\.", "_")).collect(Collectors.toList()));
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

		ResultSet rs = con.prepareStatement(betaQuery.getSQLSyntax()).executeQuery();
		while (rs.next()) {
			Clause beta = new Clause();
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
				beta.addVar(-1 * factIDBoolVarMap.get(rs.getInt(i)));
			beta.setDescription("BS");
			br.append(beta.getDimacsLine());
		}
		br.close();
	}

	public String writeFinalFormulaFile(String formulaFileName) {
		Set<String> clauses = new HashSet<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(formulaFileName));
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null)
				clauses.add(sCurrentLine);
			br.close();
			BufferedWriter wr = new BufferedWriter(new FileWriter(formulaFileName));
			String infinity = Integer.toString(clauses.size() + 1);
			String firstLine = "p wcnf " + varIndex + " " + clauses.size() + " " + infinity + "\n";
			System.out.println(firstLine);
			wr.write(firstLine);
			for (String s : clauses) {
				if (s.contains("S")) {
					System.out.println("1 " + s + "\n");
					wr.append("1 " + s + "\n");
				} else {
					System.out.println(infinity + " " + s + "\n");
					wr.append(infinity + " " + s + "\n");
				}
			}
			wr.close();
			return infinity;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return "";
		}
	}
}
