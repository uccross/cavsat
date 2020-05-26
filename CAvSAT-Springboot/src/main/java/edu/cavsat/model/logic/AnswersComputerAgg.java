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
import edu.cavsat.util.Constants;
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

	// Simply evaluates the query on the database
	// Computes GLB for Min or LUB for the Max function
	public int computeEasyBoundMinMax(SQLQuery query, Connection con) {
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

	public void runSolver(String solverCommand, String formulaFilename) throws SQLException {
		ExecCommand.executeCommand(new String[] { solverCommand, formulaFilename }, Constants.SAT_OUTPUT_FILE_NAME);
	}
}