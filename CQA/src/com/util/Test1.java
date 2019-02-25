package com.util;

import java.sql.Connection;
import java.util.List;

import com.beans.Query;
import com.beans.Schema;
import com.beans.Stats;
import com.core.AnswersComputer;
import com.core.Encoder3;
import com.core.Preprocessor;

public class Test1 {
	private static long start;

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema("schema.txt");
		Connection con = new DBEnvironment().getConnection();
		int size = 1000000;
		SyntheticDataGenerator gen = new SyntheticDataGenerator();
		List<Query> list = pp.parseUCQ("fo-queries.txt");
		for (int tada = 0; tada < 1; tada++) {
			gen.generate(size);
			System.out.println("DATA SIZE: " + size);
			size += 100000;
			int p = 1;
			Preprocessor preprocessor = null;
			// double prepro = 0, postime = 0, negtime = 0, nvars = 0, nclauses = 0,
			// iterations = 0, solvertime = 0;
			for (Query query : list) {
				double prepro = 0, postime = 0, negtime = 0, nvars = 0, nclauses = 0, iterations = 0, solvertime = 0;
				System.out.println("\rq" + (p++));
				preprocessor = new Preprocessor(schema, query, con);
				preprocessor.dropAllTables();
				preprocessor.createIndexesOnKeys();
				long constantStart = System.currentTimeMillis();
				preprocessor.createKeysViews();
				preprocessor.createAnsFromCons();
				if (query.isBoolean() && preprocessor.checkBooleanConsAnswer()) {
					System.out.println("Consistent answer is true, computed in "
							+ (System.currentTimeMillis() - constantStart) + "ms");
					continue;
				}
				preprocessor.createWitnesses(false);
				int totalRelevantFacts = preprocessor.createRelevantViews();
				preprocessor.createWitnesses(true);
				System.out
						.println("Total Preprocessing done in " + (System.currentTimeMillis() - constantStart) + "ms");
				prepro += (System.currentTimeMillis() - constantStart);
				Encoder3 encoder = new Encoder3(schema, query, con, "formulaFileName.txt");
				start = System.currentTimeMillis();
				int posClauses = encoder.createPositiveClauses();
				// postime += timeElapsed();
				int[] arr = encoder.createNegativeClauses(totalRelevantFacts);
				// negtime += timeElapsed();
				// System.out.println(postime + " " + negtime);
				encoder.closeBufferedReader();
				int vars = totalRelevantFacts + arr[0];
				int clauses = posClauses + arr[1];
				nvars += vars;
				nclauses += clauses;
				System.out.println(vars + " vars, " + clauses + " clauses");
				encoder.writeFinalFormula("formulaFileName.txt", vars, clauses);
				System.out.println("Encoding: " + timeElapsed());
				AnswersComputer computer = new AnswersComputer(con);
				if (query.isBoolean()) {
					Stats answer = computer.computeBooleanAnswer("final" + "formulaFileName.txt", "maxhas");
					System.out.println("Consistent answer is " + !answer.isSolved());
				} else {
					computer.eliminatePotentialAnswers("final" + "formulaFileName.txt", posClauses + arr[1]);
				}
				//solvertime += timeElapsed();
				// System.out.println("Answers computed in " + timeElapsed() + "ms");
				// System.out.println("Total time: " + (System.currentTimeMillis() -
				// constantStart) + "ms");
				System.out.println("Solvertime: " + timeElapsed());
				System.out.println("-------------------------------------------------------------------------------");
			}

			// System.out.println("Avg preprocessing time: " + (prepro / list.size()));
			// System.out.println("Avg posclauses creation time: " + (postime /
			// list.size()));
			// System.out.println("Avg negclauses creation time: " + (negtime /
			// list.size()));
			// System.out.println("Avg solver time: " + (solvertime / list.size()));
			// System.out.println("-------------------------------------------------------------------------------");
		}
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}
}
