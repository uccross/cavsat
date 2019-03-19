package com.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.beans.Query;
import com.beans.Schema;
import com.core.AnswersComputer;
import com.core.Encoder2;

public class Test2 {
	private static long start;

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		Schema schema = pp.parseSchema(args[0]);
		Connection con = new DBEnvironment().getConnection();
		List<Query> uCQ = pp.parseUCQ(args[1]);
		Encoder2 encoder = new Encoder2(schema, uCQ, con, args[2]);
		start = System.currentTimeMillis();
		encoder.createAlphaClausesInMemory();
		System.out.println("Alpha done in " + timeElapsed());
		encoder.createGammaClausesInMemory();
		System.out.println("Gamma done in " + timeElapsed());
		encoder.createThetaClausesInMemory();
		System.out.println("Theta done in " + timeElapsed());
		encoder.createBetaClausesInMemory();
		System.out.println("Beta done in " + timeElapsed());
		encoder.closeConnections();
		encoder.writeFinalFormulaFile(args[2]);
		System.out.println("Encoding done in " + timeElapsed() + "ms");
		long time = new AnswersComputer(con).eliminatePotentialAnswers2(args[2], encoder.getClauseCount() + 1);
		System.out.println("Solvertime: " + time);
		System.out.println("-------------------------------------------------------------------------------");
	}

	private static long timeElapsed() {
		long timeElapsed = System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		return timeElapsed;
	}
}
