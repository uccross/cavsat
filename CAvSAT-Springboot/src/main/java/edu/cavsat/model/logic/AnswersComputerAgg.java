/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.logic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.cavsat.model.bean.SQLQuery;
import edu.cavsat.util.ExecCommand;

public class AnswersComputerAgg {

	public int getFalsifiedClausesCount(String formulaFileName, String assignment) {
		Set<String> a = new HashSet<String>();
		int satisfiedClauses = 0, totalClauses = 0;
		String[] parts = assignment.split(" ");
		for (String s : parts)
			a.add(s);
		try (BufferedReader br = new BufferedReader(new FileReader(formulaFileName))) {
			String sCurrentLine;
			if ((sCurrentLine = br.readLine()) != null)
				totalClauses = Integer.parseInt(sCurrentLine.split(" ")[3]); // reading first line, e.g., p wcnf 4 6 5
			while ((sCurrentLine = br.readLine()) != null) {
				parts = sCurrentLine.split(" ");
				for (int i = 1; i < parts.length - 1; i++)
					if (a.contains(parts[i])) {
						satisfiedClauses++;
						break;
					}
			}
			br.close();
			return totalClauses - satisfiedClauses;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static double computeSum(SQLQuery witnessQuery, String formulaFileName, String assignment, Connection con)
			throws SQLException {
		String aggAttribute = witnessQuery.getSelect().get(0).split(" AS ")[0];
		witnessQuery.getWhereConditions().add(aggAttribute + " > 0");
		ResultSet rs = con.prepareStatement(witnessQuery.getSQLSyntax()).executeQuery();
		rs.next();
		return rs.getDouble(1) - getSumOfWeightsOfSatisfiedClauses(formulaFileName, assignment);
	}

	private static double getSumOfWeightsOfSatisfiedClauses(String formulaFileName, String assignment) {
		Set<String> a = new HashSet<String>();
		boolean satisfiedFlag;
		double sum = 0;
		String[] parts = assignment.split(" ");
		for (String s : parts)
			if (!s.trim().toLowerCase().equals("v"))
				a.add(s);
		try (BufferedReader br = new BufferedReader(new FileReader(formulaFileName))) {
			String sCurrentLine = br.readLine(); // reading first line, e.g., p wcnf 4 6 5
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.toUpperCase().contains("H"))
					continue;
				parts = sCurrentLine.split(" ");
				satisfiedFlag = false;
				for (int i = 1; i < parts.length - 1; i++) {
					if (a.contains(parts[i])) {
						satisfiedFlag = true;
						break;
					}
				}
				if (satisfiedFlag)
					sum += Double.parseDouble(sCurrentLine.split("v")[1].trim());
			}
			br.close();
			return sum;
		} catch (IOException e) {
			e.printStackTrace();
			return Integer.MIN_VALUE;
		}
	}

	// Computes LUB for Min or GLB for the Max function
	public static int computeDifficultBoundMinMax(String formulaFileName, String assignment) {
		Set<String> a = new HashSet<String>();
		int highestFalsifiedWeight = 0, answer = Integer.MIN_VALUE;
		String[] parts = assignment.trim().split(" ");
		for (String s : parts)
			a.add(s);
		try (BufferedReader br = new BufferedReader(new FileReader(formulaFileName))) {
			String sCurrentLine;
			sCurrentLine = br.readLine(); // reading first line, e.g., p wcnf 4 6 5
			while ((sCurrentLine = br.readLine()) != null) {
				parts = sCurrentLine.trim().split("0")[0].trim().split(" ");
				boolean clauseSatisfied = false;
				for (int i = 1; i < parts.length; i++) {
					if (a.contains(parts[i])) {
						clauseSatisfied = true;
						break;
					}
				}
				if (!clauseSatisfied && Integer.parseInt(parts[0]) > highestFalsifiedWeight) {
					highestFalsifiedWeight = Integer.parseInt(parts[0]);
					answer = Integer.parseInt(sCurrentLine.substring(sCurrentLine.indexOf("v") + 1).trim());
				}
			}
			br.close();
			return answer;
		} catch (IOException e) {
			e.printStackTrace();
			return Integer.MIN_VALUE;
		}
	}

	// Simply evaluates the query on the database
	// Computes GLB for Min or LUB for the Max function
	public static int computeEasyBoundMinMax(SQLQuery query, Connection con) {
		ResultSet rs;
		try {
			rs = con.prepareStatement(query.getSQLSyntax()).executeQuery();
			if (rs.next())
				return rs.getInt(1);
			return Integer.MIN_VALUE;
		} catch (SQLException e) {
			e.printStackTrace();
			return Integer.MIN_VALUE;
		}
	}

	public static long runSolver(String solverCommand, String formulaFilename, String outputFileName)
			throws SQLException {
		long time = System.currentTimeMillis();
		ExecCommand.executeCommand(new String[] { solverCommand, formulaFilename }, outputFileName);
		time = System.currentTimeMillis() - time;
		return time;
	}
}