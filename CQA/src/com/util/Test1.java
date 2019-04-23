/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.beans.Query;
import com.beans.Schema;
import com.beans.Stats;
import com.core.AnswersComputer;
import com.core.Encoder3;
import com.core.Preprocessor;

public class Test1 {
	private static long start;

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(args[0]);
		List<Query> uCQ = pp.parseUCQ(args[1]);
		Connection con = new DBEnvironment().getConnection();
		SyntheticDataGenerator3 gen = new SyntheticDataGenerator3();
		List<Integer> avPrepro = new ArrayList<Integer>();
		List<Integer> avSolvertime = new ArrayList<Integer>();
		for (int tada = 0; tada < 1; tada++) {
			Preprocessor preprocessor = null;
			for (Query q : uCQ) {
				q.print();
				gen.generateThirdColumnValues(100000);
				gen.generateConsistent(con, q, 950000, 0.15, false);
				gen.addInconsistency(con, schema, q, 100000, 2);
				// gen.adjustFactIDs(con, q);
				System.out.println("Data generated.");

				double prepro = 0, encoding = 0, nvars = 0, nclauses = 0, iterations = 0, solvertime = 0;
				preprocessor = new Preprocessor(schema, q, con);
				// System.out.println("Prepro object created");
				preprocessor.dropAllTables();
				System.out.println("Tables dropped");
				preprocessor.createIndexesOnKeys();
				System.out.println("Indexes created");
				start = System.currentTimeMillis();
				preprocessor.createKeysViews();
				System.out.println("Keys views created in " + timeElapsed());
				long constantStart = System.currentTimeMillis();
				preprocessor.createAnsFromCons();
				System.out.println("Ans from cons created in " + timeElapsed());
				if (q.isBoolean() && preprocessor.checkBooleanConsAnswer()) {
					System.out.println("Consistent answer is true, computed in "
							+ (System.currentTimeMillis() - constantStart) + "ms");
					avPrepro.add((int) (System.currentTimeMillis() - constantStart));
					continue;
				}
				preprocessor.createWitnesses(false, schema);
				System.out.println("Witnesses created in " + timeElapsed());
				int totalRelevantFacts = preprocessor.createRelevantViews();
				preprocessor.createWitnesses(true, schema);
				System.out.println("Relevant facts identified in " + timeElapsed());
				prepro += (System.currentTimeMillis() - constantStart);
				Encoder3 encoder = new Encoder3(schema, q, con, "formula.txt");
				int posClauses = encoder.createPositiveClauses();
				System.out.println("Positive clauses created in " + timeElapsed());
				// postime += timeElapsed();
				int[] arr = encoder.createNegativeClauses(totalRelevantFacts);
				System.out.println("Negative clauses created in " + timeElapsed());
				// negtime += timeElapsed();
				encoder.closeBufferedReader();
				int vars = totalRelevantFacts + arr[0];
				int clauses = posClauses + arr[1];
				nvars += vars;
				nclauses += clauses;
				//q.print();
				System.out.println(vars + " vars, " + clauses + " clauses");
				// System.out.println("------------------------------------");
				encoder.writeFinalFormula("formula.txt", vars, clauses);
				System.out.println("Encoding in " + timeElapsed());
				AnswersComputer computer = new AnswersComputer(con);
				if (q.isBoolean()) {
					Stats answer = computer.computeBooleanAnswer("final" + "formula.txt", "maxhs");
					System.out.println("Consistent answer is " + !answer.isSolved());
				} else {
					solvertime = computer.eliminatePotentialAnswers("final" + "formula.txt",
							(posClauses + arr[1]) * 10);
				}
				avPrepro.add((int) prepro);
				avSolvertime.add((int) solvertime);
				System.out.println("------------------------------------------------------------");

			}
			/*
			 * System.out.println(avPrepro); System.out.println(avSolvertime); double total
			 * = 0; for (int i : avPrepro) { total += i; } System.out.println(total /
			 * avPrepro.size()); total = 0; for (int i : avSolvertime) { total += i; }
			 * System.out.println(total / avSolvertime.size());
			 */
		}
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}
}
