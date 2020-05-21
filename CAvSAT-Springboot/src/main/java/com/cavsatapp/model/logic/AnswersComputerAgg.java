/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.model.logic;

import java.sql.SQLException;

import com.cavsatapp.util.Constants;
import com.cavsatapp.util.ExecCommand;

public class AnswersComputerAgg {

	public long computeGLB(String filename) throws SQLException {
		ExecCommand command = new ExecCommand();
		command.executeCommand(new String[] { Constants.MAXSAT_COMMAND, filename }, Constants.SAT_OUTPUT_FILE_NAME);
		return command.getFalsifiedSofts(Constants.SAT_OUTPUT_FILE_NAME);
	}

	public long computeLUB(String filename) throws SQLException {
		ExecCommand command = new ExecCommand();
		command.executeCommand(new String[] { Constants.MINSAT_COMMAND, filename }, Constants.SAT_OUTPUT_FILE_NAME);
		return command.getFalsifiedSofts(Constants.SAT_OUTPUT_FILE_NAME);
	}
}