package com.experiments;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.beans.Atom;
import com.beans.CNFFormula;
import com.beans.Query;
import com.beans.Schema;
import com.beans.Stats;
import com.core.AnswersComputer;
import com.core.Encoder;
import com.core.Preprocessor;
import com.querypreprocessor.AttackGraphBuilder;
import com.querypreprocessor.CertainRewriter;
import com.util.DBEnvironment;
import com.util.ProblemParser;
import com.util.SyntheticDataGenerator1;

public class RewritabilityExperiment {
	private static long start;

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		List<File> filesInFolder = null;
		try {
			filesInFolder = Files.walk(Paths.get("FO-rewritable")).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}
		Schema schema = pp.parseSchema("schema.txt");
		Connection con = new DBEnvironment().getConnection();
		for (File file : filesInFolder) {
			Query q = pp.parseUCQ(file).get(0);
			System.out.println(file.getName());
			//SyntheticDataGenerator1.generateData(q, schema, con, 10, 1000000);
			// doExperiment(q, schema, con);

			Preprocessor preprocessor = new Preprocessor(schema, q, con);
			preprocessor.dropAllTables();
			System.out.println("All tables dropped");
			preprocessor.createIndexesOnKeys();
			System.out.println("Indexes created");
			long constantStart = System.currentTimeMillis();
			start = System.currentTimeMillis();

			preprocessor.createKeysViews();
			System.out.println("Keys table done in " + timeElapsed() + "ms");
			preprocessor.createAnsFromCons();
			System.out.println("cons ans done in " + timeElapsed() + "ms");
			if (q.isBoolean() && preprocessor.checkBooleanConsAnswer()) {
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

			Encoder encoder = new Encoder(schema, q, con);
			CNFFormula f1 = encoder.createPositiveClauses(totalRelevantFacts);
			System.out.println("Positive clauses created in " + timeElapsed() + "ms");
			CNFFormula f2 = encoder.createNegativeClauses(totalRelevantFacts, 0);
			System.out.println("Negative clauses created in " + timeElapsed() + "ms");
			CNFFormula f = f1.combine(f2);
			start = System.currentTimeMillis();
			Encoder.createDimacsFile("formula1.txt", f, 0);
			System.out.println("Dimacs file written in " + timeElapsed() + "ms");
			long solverTime = 0;
			AnswersComputer computer = new AnswersComputer(con);
			if (q.isBoolean()) {
				Stats answer = computer.computeBooleanAnswer("formula1.txt", "glucose");
				System.out.println("Consistent answer is " + !answer.isSolved());
			} else {
				//solverTime = computer.eliminatePotentialAnswers("formula1.txt", f);
			}
			System.out.println("Solver took " + solverTime + "ms");
			System.out.println("Total time: " + (System.currentTimeMillis() - constantStart) + "ms");
			System.out.println("-------------------------------------------------------------------------------");
		}
	}

	public static void doExperiment(Query query, Schema schema, Connection con) throws SQLException {
		int approach = 0;
		start = System.currentTimeMillis();
		AttackGraphBuilder builder = new AttackGraphBuilder(query);
		if (!builder.isQueryFO())
			return;
		System.out.println("Attack graph built in " + timeElapsed() + "ms");
		List<Atom> sortedAtoms = builder.topologicalSort();
		CertainRewriter certainRewriter = new CertainRewriter();
		query.setAtoms(sortedAtoms);
		String sqlQuery = certainRewriter.getCertainRewritingSQL(query, schema);
		System.out.println(sqlQuery);
		System.out.println("Rewriting done in " + timeElapsed() + "ms");

		final PreparedStatement ps = con.prepareStatement(sqlQuery);
		final Runnable stuffToDo = new Thread() {
			@Override
			public void run() {
				try {
					start = System.currentTimeMillis();
					// ps.executeQuery();
					System.out.println("Query executed in " + timeElapsed() + "ms");
					ps.close();
					System.out.println("ps is closed");
				} catch (SQLException e) {
					System.out.println(e);
				}
			}
		};

		final ExecutorService executor = Executors.newSingleThreadExecutor();
		final Future<?> future = executor.submit(stuffToDo);
		executor.shutdown();

		try {
			future.get(120, TimeUnit.MINUTES);
		} catch (TimeoutException | InterruptedException | ExecutionException te) {
			System.err.println("30 mins timeout");
			ps.cancel();
			ps.close();
		}
		if (!executor.isTerminated())
			executor.shutdownNow();

		long constantStart = System.currentTimeMillis();
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
			Stats answer = computer.computeBooleanAnswer("formula1.txt", "glucose");
			System.out.println("Consistent answer is " + !answer.isSolved());
		} else if (approach == 0) {
			//computer.eliminatePotentialAnswers("formula1.txt", f);
		} else {
			computer.computeNonBooleanAnswer("formula1.txt", "glucose");
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

class InterruptTimerTask extends TimerTask {

	private Thread theTread;

	public InterruptTimerTask(Thread theTread) {
		this.theTread = theTread;
	}

	@Override
	public void run() {
		theTread.interrupt();
	}

}