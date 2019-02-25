package com.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

import com.beans.CNFFormula;
import com.beans.Query;
import com.beans.Schema;
import com.beans.Stats;
import com.util.DBEnvironment;
import com.util.ProblemParser;
import com.util.SyntheticDataGenerator;

public class Test {

	private static long start;

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();
		/*
		 * if (args.length < 2) { System.err.
		 * println("Usage: java -jar akhil.jar query.txt schema.txt approach (0/1)");
		 * return; }
		 */

		List<File> filesInFolder = null;
		try {
			filesInFolder = Files.walk(Paths.get("FO-rewritable")).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}
		Schema schema = pp.parseSchema("schema.txt");
		Connection con = new DBEnvironment().getConnection();
		int size = 1000000;
		SyntheticDataGenerator gen = new SyntheticDataGenerator();
		for (int tada = 0; tada < 1; tada++) {
			gen.generate(size);
			System.out.println("DATA SIZE: " + size);
			size += 100000;
			for (File file : filesInFolder) {
				System.out.println(file.getName());
				Query query = pp.parseUCQ(file).get(0);
				int approach = 0;
				if (args.length == 3)
					approach = Integer.parseInt(args[2]);
				// query.print();
				// schema.print();
				Preprocessor preprocessor = new Preprocessor(schema, query, con);
				start = System.currentTimeMillis();
				preprocessor.dropAllTables();
				// System.out.println("All tables dropped in " + timeElapsed() + "ms");
				preprocessor.createIndexesOnKeys();
				// System.out.println("Indexes created in " + timeElapsed() + "ms");
				long constantStart = System.currentTimeMillis();
				preprocessor.createKeysViews();
				// System.out.println("Keys table done in " + timeElapsed() + "ms");
				preprocessor.createAnsFromCons();
				System.out.println("cons ans done in " + timeElapsed() + "ms");
				if (query.isBoolean() && preprocessor.checkBooleanConsAnswer()) {
					System.out.println("Consistent answer is true");
					return;
				}
				start = System.currentTimeMillis();
				preprocessor.createWitnesses(false);
				System.out.println("Witnesses computed in " + timeElapsed() + "ms");
				int totalRelevantFacts = preprocessor.createRelevantViews();
				System.out.println("Relevant views created in " + timeElapsed() + "ms");
				preprocessor.createWitnesses(true);
				System.out.println("IDs given to witnesses in " + timeElapsed() + "ms");
				System.out
						.println("Total Preprocessing done in " + (System.currentTimeMillis() - constantStart) + "ms");

				Encoder encoder = new Encoder(schema, query, con);
				start = System.currentTimeMillis();
				encoder.createPositiveClauses();
				System.out.println("Unbelievably done in " + (System.currentTimeMillis() - start));

				CNFFormula f1 = encoder.createPositiveClauses(totalRelevantFacts);
				System.out.println("Positive clauses created in " + timeElapsed() + "ms");
				System.out.println(f1.getNoOfVariables() + " variables, " + f1.getClauses().size() + " clauses");
				CNFFormula f2 = encoder.createNegativeClauses(totalRelevantFacts, approach);
				System.out.println("Negative clauses created in " + timeElapsed() + "ms");
				System.out.println(f2.getNoOfVariables() + " variables, " + f2.getClauses().size() + " clauses");
				CNFFormula f = f1.combine(f2);
				System.out.println(f.getNoOfVariables() + " variables, " + f.getClauses().size() + " clauses");
				Encoder.createDimacsFile("formula1.txt", f, approach);
				System.out.println("Dimacs file written in " + timeElapsed() + "ms");

				AnswersComputer computer = new AnswersComputer(con);
				if (query.isBoolean()) {
					Stats answer = computer.computeBooleanAnswer("formula1.txt", "glucose");
					System.out.println("Consistent answer is " + !answer.isSolved());
				} else if (approach == 0) {
					//computer.eliminatePotentialAnswers("formula1.txt", f);
				} else {
					computer.computeNonBooleanAnswer("formula1.txt", "glucose");
				}
				System.out.println("Solver took " + timeElapsed() + "ms");
				System.out.println("Total time: " + (System.currentTimeMillis() - constantStart) + "ms");
				System.out.println("-------------------------------------------------------------------------------");
			}
		}
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}
}
