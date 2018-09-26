package com.core;

import java.sql.Connection;

import com.beans.CNFFormula;
import com.beans.Query;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class Test {

	private static long start;

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();

		if (args.length < 2) {
			System.err.println("Usage: java -jar akhil.jar query.txt schema.txt approach (0/1)");
			return;
		}
		Query query = pp.parseQueryFromFile(args[0]);
		Schema schema = pp.parseSchema(args[1]);
		int approach = 0;
		if (args.length == 3)
			approach = Integer.parseInt(args[2]);

		long constantStart = System.currentTimeMillis();
		query.print();
		schema.print();
		Connection con = new DBEnvironment().getConnection();
		Preprocessor preprocessor = new Preprocessor(schema, query, con);
		start = System.currentTimeMillis();
		preprocessor.dropAllTables();
		System.out.println("All tables dropped in " + timeElapsed() + "ms");
		preprocessor.createIndexesOnKeys();
		System.out.println("Indexes created in " + timeElapsed() + "ms");
		preprocessor.createKeysViews();
		System.out.println("Keys table done in " + timeElapsed() + "ms");
		preprocessor.createAnsFromCons();
		System.out.println("cons ans done in " + timeElapsed() + "ms");
		if (query.isBoolean() && preprocessor.checkBooleanConsAnswer()) {
			System.out.println("Consistent answer is true");
			return;
		}
		preprocessor.createWitnesses(false);
		System.out.println("Witnesses computed in " + timeElapsed() + "ms");
		int totalRelevantFacts = preprocessor.createRelevantViews();
		System.out.println("Relevant views created in " + timeElapsed() + "ms");
		preprocessor.createWitnesses(true);
		System.out.println("IDs given to witnesses in " + timeElapsed() + "ms");
		System.out.println("Total Preprocessing done in " + (System.currentTimeMillis() - constantStart) + "ms");

		Encoder encoder = new Encoder(schema, query, con);
		CNFFormula f1 = encoder.createPositiveClauses(totalRelevantFacts);
		System.out.println("Positive clauses created in " + timeElapsed() + "ms");
		CNFFormula f2 = encoder.createNegativeClauses(totalRelevantFacts, approach);
		System.out.println("Negative clauses created in " + timeElapsed() + "ms");
		CNFFormula f = f1.combine(f2);
		Encoder.createDimacsFile("formula1.txt", f, approach);
		System.out.println("Dimacs file written in " + timeElapsed() + "ms");

		AnswersComputer computer = new AnswersComputer(con);
		if (query.isBoolean()) {
			boolean answer = computer.computeBooleanAnswer("formula1.txt", "glucose");
			System.out.println("Consistent answer is " + answer);
		} else if (approach == 0) {
			computer.eliminatePotentialAnswers("formula1.txt", f);
		} else {
			computer.computeNonBooleanAnswer("formula1.txt", "lingeling");
		}
		System.out.println("Solver took " + timeElapsed() + "ms");
		System.out.println("Total time: " + (System.currentTimeMillis() - constantStart) + "ms");
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}
}
