package com.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.beans.Query;
import com.beans.Schema;
import com.core.AnswersComputer;
import com.core.Encoder4;

public class Test4 {
	private static long start;

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(args[0]);
		List<Query> uCQ = pp.parseUCQ(args[1]);
		Connection con = new DBEnvironment().getConnection();
		Encoder4 encoder = new Encoder4(schema, con, args[2]);

		for (Query q : uCQ) {
			SyntheticDataGenerator3 gen = new SyntheticDataGenerator3();
			gen.generateThirdColumnValues(100000);
			gen.generateConsistent(con, q, 950000, 0.15, true);
			gen.addInconsistency(con, schema, q, 100000, 2);
			int totalFacts = gen.adjustFactIDs(con, q);
			System.out.println("Data generated.");

			start = System.currentTimeMillis();
			encoder.openBr();
			encoder.createAlphaClausesFaster(q);
			System.out.println("Alpha done in " + timeElapsed());
			encoder.createBetaClausesFaster(q, totalFacts);
			System.out.println("Beta done in " + timeElapsed());
			
			encoder.writeFinalFormulaFile(args[2]);
			System.out.println("Formula written in " + timeElapsed());
			long time = new AnswersComputer(con).eliminatePotentialAnswers2(args[2], encoder.getClauseCount() + 1);
			// System.out.println("Answers in " + timeElapsed());
			System.out.println("Solvertime: " + time);
			System.out.println("------------------------------------------------------------------");
		}
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}

}
