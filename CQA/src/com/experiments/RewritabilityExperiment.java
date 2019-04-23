/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.experiments;

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

import com.beans.Atom;
import com.beans.Query;
import com.beans.Schema;
import com.querypreprocessor.AttackGraphBuilder;
import com.querypreprocessor.CertainRewriter;
import com.util.DBEnvironment;
import com.util.ProblemParser;
import com.util.SyntheticDataGenerator3;

public class RewritabilityExperiment {
	private static long start;

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		List<Query> uCQ = pp.parseUCQ(args[1]);
		Schema schema = pp.parseSchema(args[0]);
		Connection con = new DBEnvironment().getConnection();
		SyntheticDataGenerator3 gen = new SyntheticDataGenerator3();
		for (Query q : uCQ) {
			//gen.generateThirdColumnValues(1000);
			//gen.generateConsistent(con, q, 9500, 0.15, false);
			//gen.addInconsistency(con, schema, q, 1000, 2);
			q.print();
			doExperiment(q, schema, con);
		}
	}

	public static void doExperiment(Query query, Schema schema, Connection con) throws SQLException {
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
if(true)
	return;
		final PreparedStatement ps = con.prepareStatement(sqlQuery);
		final Runnable stuffToDo = new Thread() {
			@Override
			public void run() {
				try {
					start = System.currentTimeMillis();
					ps.executeQuery();
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
