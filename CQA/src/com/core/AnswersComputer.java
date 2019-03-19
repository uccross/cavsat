package com.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import com.beans.Stats;
import com.util.ExecCommand;

public class AnswersComputer {
	private Connection con;

	public AnswersComputer(Connection con) {
		super();
		this.con = con;
	}

	public Stats computeBooleanAnswer(String filename, String solvername) {
		ExecCommand command = new ExecCommand();
		if (solvername.equalsIgnoreCase("MaxHS")) {
			command.executeCommand(new String[] { "./maxhs", filename }, "output.txt");
		} else if (solvername.equalsIgnoreCase("Glucose")) {
			command.executeCommand(new String[] { "./glucose", filename }, "output.txt");
		} else if (solvername.equalsIgnoreCase("lingeling")) {
			command.executeCommand(new String[] { "./lingeling", filename }, "output.txt");
		}
		return command.isSAT("output.txt", solvername);
	}

	public void computeNonBooleanAnswer(String filename, String solvername) {
		long start, a = 0, b = 0, c = 0, d = 0;
		ExecCommand command = new ExecCommand();
		boolean clauseChanged = true;
		int i = 0;
		while (clauseChanged && !extraClauseIsEmpty(filename)) {
			i++;
			// System.out.println("Iteration "+i);
			start = System.currentTimeMillis();
			if (solvername.equalsIgnoreCase("MaxHS")) {
				command.executeCommand(new String[] { "./maxhs", filename }, "output.txt");
			} else if (solvername.equalsIgnoreCase("Glucose")) {
				command.executeCommand(new String[] { "./glucose", filename, "output.txt" }, null);
			} else if (solvername.equalsIgnoreCase("lingeling")) {
				command.executeCommand(new String[] { "./lingeling", filename }, "output.txt");
			}
			d += (System.currentTimeMillis() - start);
			if (!command.isSAT("output.txt", solvername).isSolved())
				break;
			start = System.currentTimeMillis();
			Set<Integer> inconsistentFactIDs = findLiteralsSetToOne("output.txt", solvername);
			a += (System.currentTimeMillis() - start);
			start = System.currentTimeMillis();
			clauseChanged = removeLiteralsFromExtraClause(filename, inconsistentFactIDs);
			b += (System.currentTimeMillis() - start);
			start = System.currentTimeMillis();
			// deleteInconsistentAdditionalAnswers(inconsistentFactIDs);
			c += (System.currentTimeMillis() - start);
		}
		System.out.println(a + " " + b + " " + c);
		System.out.println("SAT Iterations: " + i);
		System.out.println("Actual solving time: " + d);
		System.out.println("Dimacs file editing time: " + (a + b + c));
		// buildFinalAnswers();
	}

	@SuppressWarnings("unused")
	private void buildFinalAnswers() {
		String q1 = "ALTER TABLE ADDITIONAL_ANSWERS DROP COLUMN FACTID";
		String q2 = "CREATE TABLE FINAL_ANSWERS AS SELECT * FROM ANS_FROM_CONS UNION SELECT * FROM ADDITIONAL_ANSWERS";
		try {
			con.prepareStatement(q1).execute();
			con.prepareStatement("DROP TABLE IF EXISTS FINAL_ANSWERS").execute();
			con.prepareStatement(q2).execute();
			System.out.println("Certain answers are computed and stored in a view named final_answers");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean extraClauseIsEmpty(String filename) {
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("A")) {
					return sCurrentLine.split(" ").length == 3; // 3 characters left in the empty clause are 0, c, and A
				}
			}
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private Set<Integer> findLiteralsSetToOne(String filename, String solvername) {
		Set<Integer> literalsSetToOne = new HashSet<Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String sCurrentLine;
			if (solvername.equalsIgnoreCase("MaxHS") || solvername.equalsIgnoreCase("Lingeling")) {
				while ((sCurrentLine = br.readLine()) != null) {
					if (sCurrentLine.startsWith("v")) {
						// System.out.println("Assignment : " + sCurrentLine);
						String[] literals = sCurrentLine.split(" ");
						for (int i = 1; i < literals.length; i++) { // 0th index contains letter 'v'
							int literal = Integer.parseInt(literals[i]);
							if (literal > 0) {
								literalsSetToOne.add(literal);
								// System.out.print(literal + " ");
							}
						}
					}
				}
			} else if (solvername.equalsIgnoreCase("Glucose")) {
				sCurrentLine = br.readLine();
				String[] literals = sCurrentLine.split(" ");
				for (int i = 0; i < literals.length; i++) {
					int literal = Integer.parseInt(literals[i]);
					if (literal > 0) {
						literalsSetToOne.add(literal);
						// System.out.print(literal + " ");
					}
				}
			}

			return literalsSetToOne;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private boolean removeLiteralsFromExtraClause(String filename, Set<Integer> literals) {
		try {
			BufferedReader file = new BufferedReader(new FileReader(filename));
			String line, oldline;
			StringBuffer inputBuffer = new StringBuffer();
			while ((line = file.readLine()) != null) {
				if (line.contains("A")) {
					oldline = line;
					String[] values = line.split(" ");
					line = "";
					for (int i = 0; i < values.length - 2; i++) {
						if (!literals.contains(Integer.parseInt(values[i]))) {
							line += values[i] + " ";
						} else {
							// System.out.println("Removed " + values[i]);
						}
					}
					line += "c A";
					// System.out.println("Old clause: " + oldline);
					// System.out.println("New clause: " + line);
					if (oldline.equals(line)) {
						file.close();
						return false;
					}
				}
				inputBuffer.append(line);
				inputBuffer.append('\n');
			}
			String inputStr = inputBuffer.toString();
			file.close();
			FileOutputStream fileOut = new FileOutputStream(filename);
			fileOut.write(inputStr.getBytes());
			fileOut.close();
			return true;
		} catch (Exception e) {
			System.out.println("Problem reading file.");
			return false;
		}
	}

	public long eliminatePotentialAnswers2(String filename, int infinity) throws SQLException {
		boolean moreAnswers = true;
		BufferedWriter wr = null;
		PreparedStatement psSelect = con.prepareStatement("SELECT pVar FROM POTENTIAL_ANSWERS");
		PreparedStatement psDelete = con.prepareStatement("DELETE FROM POTENTIAL_ANSWERS WHERE pVar = ?");
		String output = "";
		Set<Integer> assignment = new HashSet<Integer>();
		ResultSet rsPotentialAnswers = null;
		int iterationCount = -1;
		long time = 0, start = 0;
		ExecCommand command = new ExecCommand();
		while (moreAnswers) {
			assignment.clear();
			iterationCount++;
			moreAnswers = false;
			start = System.currentTimeMillis();
			command.executeCommand(new String[] { "./maxhs", filename }, "output.txt");
			output = command.readOutput("output.txt");
			time += (System.currentTimeMillis() - start);
			StringTokenizer st = new StringTokenizer(output.substring(1), " ");
			while (st.hasMoreTokens())
				assignment.add(Integer.parseInt(st.nextToken()));
			try {
				rsPotentialAnswers = psSelect.executeQuery();
				wr = new BufferedWriter(new FileWriter(filename, true));
				while (rsPotentialAnswers.next()) {
					if (assignment.contains(rsPotentialAnswers.getInt(1))) {
						wr.append(infinity + " -" + rsPotentialAnswers.getInt(1) + " 0 c I\n");
						psDelete.setInt(1, rsPotentialAnswers.getInt(1));
						psDelete.addBatch();
						moreAnswers = true;
					}
				}
				wr.close();
				psDelete.executeBatch();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		System.out.println("MaxSAT Iterations: " + iterationCount);
		return time;
	}

	public long eliminatePotentialAnswers(String filename, int infinity) throws SQLException {
		boolean moreAnswers = true;
		BufferedWriter wr = null;
		String output = "";
		PreparedStatement psSelect = con.prepareStatement("SELECT FactID FROM ADDITIONAL_ANSWERS");
		PreparedStatement psDelete = con.prepareStatement("DELETE FROM ADDITIONAL_ANSWERS WHERE FACTID = ?");
		Set<Integer> assignment = new HashSet<Integer>();
		int iterationCount = 0;
		long start = 0, time = 0;
		ExecCommand command = new ExecCommand();
		while (moreAnswers) {
			start = System.currentTimeMillis();
			assignment.clear();
			iterationCount++;
			moreAnswers = false;
			start = System.currentTimeMillis();
			command.executeCommand(new String[] { "./maxhs", filename }, "output.txt");
			output = command.readOutput("output.txt");
			time += (System.currentTimeMillis() - start);
			StringTokenizer st = new StringTokenizer(output.substring(1), " ");
			while (st.hasMoreTokens())
				assignment.add(Integer.parseInt(st.nextToken()));
			try {
				ResultSet rsSelect = psSelect.executeQuery();
				wr = new BufferedWriter(new FileWriter(filename, true));
				while (rsSelect.next()) {
					if (assignment.contains(rsSelect.getInt(1))) {
						wr.append(infinity + " -" + rsSelect.getInt(1) + " 0 c I\n");
						psDelete.setInt(1, rsSelect.getInt(1));
						psDelete.addBatch();
						moreAnswers = true;
					}
				}
				wr.close();
				psDelete.executeBatch();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Solvertime " + time);
		System.out.println("MaxSAT Iterations: " + iterationCount);
		return time;
	}
}