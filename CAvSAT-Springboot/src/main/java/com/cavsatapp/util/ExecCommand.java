/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package com.cavsatapp.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.cavsatapp.model.bean.Stats;

public class ExecCommand {

	public int executeCommand(String[] command, String outputfilename) {
		ProcessBuilder pb = new ProcessBuilder(command);
		if (null != outputfilename) {
			// send standard output to a file
			pb.redirectOutput(new File(outputfilename));
			// merge standard error with standard output
			pb.redirectErrorStream(true);
		}
		try {
			Process p = pb.start();
			int exitVal = p.waitFor();
			return exitVal;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public String readOutput(String filename) {
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.startsWith("v")) {
					br.close();
					return sCurrentLine;
				}
			}
			br.close();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Stats isSAT(String filename, String solvername) {
		Stats stats = new Stats();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String sCurrentLine;
			if (solvername.equalsIgnoreCase("MaxHS")) {
				while ((sCurrentLine = br.readLine()) != null) {
					if (sCurrentLine.startsWith("c Solved: Number")) {
						stats.setSolved(true);
						br.close();
						return stats;
						// return Integer.parseInt(sCurrentLine.split("=")[1].replaceAll(" ", "")) == 0;
					}
				}
			} else if (solvername.equalsIgnoreCase("Glucose") || solvername.equalsIgnoreCase("Lingeling")) {
				while ((sCurrentLine = br.readLine()) != null) {
					if (sCurrentLine.startsWith("s SATISFIABLE")) {
						stats.setSolved(true);
					} else if (sCurrentLine.startsWith("c conflicts")) {
						stats.setConflicts(Long.parseLong(sCurrentLine.split(":")[1].trim().split(" ")[0]));
					} else if (sCurrentLine.startsWith("c decisions")) {
						stats.setDecisions(Long.parseLong(sCurrentLine.split(":")[1].trim().split(" ")[0]));
					} else if (sCurrentLine.startsWith("c propagations")) {
						stats.setPropagations(Long.parseLong(sCurrentLine.split(":")[1].trim().split(" ")[0]));
					} else if (sCurrentLine.startsWith("c CPU time")) {
						stats.setTime(Double.parseDouble(sCurrentLine.split(":")[1].trim().split(" ")[0]));
					}
				}
				br.close();
				return stats;
			}
			br.close();
			return stats;
		} catch (IOException e) {
			e.printStackTrace();
			return stats;
		}
	}
}
