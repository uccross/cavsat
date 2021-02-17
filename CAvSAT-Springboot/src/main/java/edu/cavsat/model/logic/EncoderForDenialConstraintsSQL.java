package edu.cavsat.model.logic;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cavsat.model.bean.Clause;
import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.model.bean.Schema;
import edu.cavsat.util.CAvSATSQLQueries;
import edu.cavsat.util.Constants;

public class EncoderForDenialConstraintsSQL {
	private Connection con;
	private int varIndex = 1;
	private Map<Integer, Integer> factIDBoolVarMap;
	private List<Set<Integer>> violations;
	private Set<Integer> factsInvolvedInViolations;

	public EncoderForDenialConstraintsSQL(Schema schema, Connection con, CAvSATSQLQueries SQLQueriesImpl)
			throws IOException {
		super();
		this.con = con;
		this.factIDBoolVarMap = new HashMap<Integer, Integer>();
		this.violations = new ArrayList<Set<Integer>>();
		this.factsInvolvedInViolations = new HashSet<Integer>();
	}

	public void populateFactIDBoolVarMap(SQLQuery sqlQuery) throws SQLException {
		ResultSet rsFactIDs = con.prepareStatement(getAllFactIDsSQL(sqlQuery)).executeQuery();
		while (rsFactIDs.next())
			factIDBoolVarMap.put(rsFactIDs.getInt(1), varIndex++);
	}

	public void createAlphaClauses(BufferedWriter wr) throws SQLException, IOException {
		String[] constraints = null;
		for (String dc : constraints) {
			Set<Integer> violation;
			ResultSet rsViolations = con.prepareStatement(getMinimalViolationsQueryFromDC(dc)).executeQuery();
			int columnCount = rsViolations.getMetaData().getColumnCount();
			while (rsViolations.next()) {
				violation = new HashSet<Integer>();
				for (int i = 1; i <= columnCount; i++)
					violation.add(rsViolations.getInt(i));
				violations.add(violation);
				factsInvolvedInViolations.addAll(violation);
			}
		}
		removeNonMinimalViolations();
		Clause clause;
		for (Set<Integer> violation : violations) {
			clause = new Clause();
			for (int factID : violation)
				clause.addVar(-1 * factIDBoolVarMap.get(factID));
			clause.setDescription("A H");
			wr.append(clause.getDimacsLine());
		}
	}

	public void createThetaGammaClauses(BufferedWriter wr) throws IOException {
		for (int factID : factIDBoolVarMap.keySet()) {
			List<Set<Integer>> nearViolations = new ArrayList<Set<Integer>>();
			if (factsInvolvedInViolations.contains(factID)) {
				List<Set<Integer>> filteredViolations = violations.stream().filter(a -> a.contains(factID))
						.collect(Collectors.toList());
				for (Set<Integer> filteredViolation : filteredViolations) {
					// A fact that itself is a minimal violation translates directly into a clause
					if (filteredViolation.size() == 1) {
						Clause clause = new Clause();
						clause.addVar(-1 * factIDBoolVarMap.get(filteredViolation.iterator().next()));
						clause.setDescription("G H");
						wr.append(clause.getDimacsLine());
					} else {
						filteredViolation.remove(factID);
						nearViolations.add(filteredViolation);
					}
				}
				Clause gammaClause = new Clause();
				gammaClause.addVar(factIDBoolVarMap.get(factID));
				for (Set<Integer> nearViolation : nearViolations) {
					// No need to add y-variable and theta-clauses if near-violation has size 1
					if (nearViolation.size() == 1)
						gammaClause.addVar(factIDBoolVarMap.get(nearViolation.iterator().next()));
					else {
						int yVar = varIndex++;
						gammaClause.addVar(yVar);
						Clause thetaClauseBig = new Clause();
						thetaClauseBig.addVar(yVar);
						for (int involvedFactID : nearViolation) {
							thetaClauseBig.addVar(-1 * involvedFactID);
							Clause thetaClauseSmall = new Clause();
							thetaClauseSmall.addVar(-1 * yVar);
							thetaClauseSmall.addVar(involvedFactID);
							thetaClauseSmall.setDescription("T H");
							wr.append(thetaClauseSmall.getDimacsLine());
						}
						thetaClauseBig.setDescription("T H");
						wr.append(thetaClauseBig.getDimacsLine());
					}
				}
				gammaClause.setDescription("G H");
				wr.append(gammaClause.getDimacsLine());
			} else {
				Clause clause = new Clause();
				clause.addVar(factIDBoolVarMap.get(factID));
				clause.setDescription("B H");
				wr.append(clause.getDimacsLine());
			}
		}
	}

	private void removeNonMinimalViolations() {
		List<Set<Integer>> nonMinimal = new ArrayList<Set<Integer>>();
		List<Set<Integer>> temp;
		for (Set<Integer> violation : violations) {
			temp = violations.stream().filter(a -> a.containsAll(violation)).collect(Collectors.toList());
			temp.remove(violation);
			nonMinimal.addAll(temp);
		}
		violations.removeAll(nonMinimal);
	}

	private String getMinimalViolationsQueryFromDC(String dc) {
		StringBuilder sqlQuery = new StringBuilder("SELECT ");
		String[] parts = dc.split(";");
		List<String> tuples = Arrays.asList(parts[0].split(","));
		List<String> expressions = Arrays.asList(parts[1].split(","));
		List<String> tupleAliases = new ArrayList<String>();
		List<String> relations = new ArrayList<String>();
		for (String tuple : tuples) {
			tupleAliases.add(tuple.substring(0, tuple.indexOf('(')));
			relations.add(tuple.substring(tuple.indexOf('(') + 1, tuple.length() - 1));
		}
		sqlQuery.append(tupleAliases.stream().map(a -> a + "." + Constants.CAvSAT_FACTID_COLUMN_NAME)
				.collect(Collectors.joining(", ")));
		sqlQuery.append(" FROM ");
		sqlQuery.append(relations.stream().map(a -> a + " " + tupleAliases.get(relations.indexOf(a)))
				.collect(Collectors.joining(", ")));
		sqlQuery.append(" WHERE ");
		sqlQuery.append(expressions.stream().collect(Collectors.joining(" AND ")));
		return sqlQuery.toString();
	}

	private String getAllFactIDsSQL(SQLQuery sqlQuery) {
		List<String> q = new ArrayList<String>();
		for (String relationName : sqlQuery.getFrom())
			q.add("SELECT " + Constants.CAvSAT_FACTID_COLUMN_NAME + " FROM " + relationName);
		return q.stream().collect(Collectors.joining(" UNION "));
	}
}
