package com.core;

import java.sql.Connection;

import com.beans.CNFFormula;
import com.beans.Query;
import com.beans.Schema;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class Test1 {

	private static long start;

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();

		Query query = pp.parseQueryFromFile("C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\q9.txt");
		Schema schema = pp.parseSchema("C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\foodschema.txt");
		//Query query = pp.parseQueryFromFile(args[0]);
		//Schema schema = pp.parseSchema(args[1]);
		int approach = 0;
		if (args.length == 3)
			approach = Integer.parseInt(args[2]);

		query.print();
		schema.print();
		Connection con = new DBEnvironment().getConnection();
		Preprocessor preprocessor = new Preprocessor(schema, query, con);
		start = System.currentTimeMillis();
		preprocessor.dropAllTables();
		System.out.println("All tables dropped in " + timeElapsed() + "ms");
		Encoder1 encoder1 = new Encoder1(schema, query, con);
		System.out.println("Encoder created in " + timeElapsed() + "ms");
		long constantStart = System.currentTimeMillis();
		encoder1.createViews(schema.getRelationsByNames(query.getParticipatingRelationNames()));
		System.out.println("Keys views created in " + timeElapsed() + "ms");
		CNFFormula f1 = encoder1.createPositiveClausesFromAllFacts();
		System.out.println("Positive clauses created in " + timeElapsed() + "ms");
		CNFFormula f2 = encoder1.createNonBoolNegClauses(schema, f1.getNoOfVariables());
		System.out.println("Negative clauses created in " + timeElapsed() + "ms");
		CNFFormula f3 = encoder1.createPotAnswerClauses();
		System.out.println("Soft clauses created in " + timeElapsed() + "ms");
		CNFFormula f = f1.combine(f2).combine(f3);
		System.out.println("Formula combined in " + timeElapsed() + "ms");
		Encoder.createDimacsFile("formula1.txt", f, approach);
		System.out.println("Dimacs file written in " + timeElapsed() + "ms");

		AnswersComputer computer = new AnswersComputer(con);
		if (query.isBoolean()) {
			boolean answer = computer.computeBooleanAnswer("formula1.txt", "glucose");
			System.out.println("Consistent answer is " + answer);
		} else if (approach == 0) {
			encoder1.eliminatePotentialAnswers("formula1.txt", f);
			System.out.println("Answers eliminated in " + timeElapsed() + "ms");
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
