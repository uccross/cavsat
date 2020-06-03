/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.util;

/**
 * @author Akhil
 *
 */
public class Constants {
	// Constants to represent the data complexity of a conjunctive query
	public static final int CFOREST = 0;
	public static final int FO_REWRITABLE_BUT_NOT_CFOREST = 1;
	public static final int P_BUT_NOT_FO_REWRITABLE = 2;
	public static final int CONPCOMPLETE = 3;
	public static final int UNKNOWN = -1;
	public static final int PREVIEW_ROW_COUNT = 100;
	public static final String FREE_TUPLE = "freeTuple";
	public static final String FREE_TUPLE_ALIAS = "free_";
	public static final String BOOL_CONS_ANSWER_COLUMN_NAME = "CONS_ANSWER";
	public static final String GLB_COLUMN_NAME = "GLB";
	public static final String LUB_COLUMN_NAME = "LUB";

	public static final String CAvSAT_TBL_PREFIX = "CAVSAT_";
	public static final String CAvSAT_CONSTRAINTS_TABLE_NAME = CAvSAT_TBL_PREFIX + "SYS_CONSTRAINTS";
	public static final String CAvSAT_ANS_FROM_CONS_TABLE_NAME = CAvSAT_TBL_PREFIX + "ANS_FROM_CONS";
	public static final String CAvSAT_CONS_TABLE_PREFIX = CAvSAT_TBL_PREFIX + "CONS_";
	public static final String CAvSAT_INCONS_TABLE_PREFIX = CAvSAT_TBL_PREFIX + "INCONS_";
	public static final String CAvSAT_WITNESSES_TABLE_NAME = CAvSAT_TBL_PREFIX + "WITNESSES";
	public static final String CAvSAT_WITNESSES_WITH_FACTID_TABLE_NAME = CAvSAT_TBL_PREFIX + "WITNESSES_WITH_FACTID";
	public static final String CAvSAT_UNOPT_WITNESSES_WITH_FACTID_TABLE_NAME = CAvSAT_TBL_PREFIX
			+ "UNOPT_WITNESSES_WITH_FACTID";
	public static final String CAvSAT_RELEVANT_DISTINCT_POTENTIAL_ANS_TABLE_NAME = CAvSAT_TBL_PREFIX
			+ "RELEVANT_DISTINCT_POTENTIAL_ANSWERS";
	public static final String CAvSAT_UNOPT_DISTINCT_POTENTIAL_ANS_TABLE_NAME = CAvSAT_TBL_PREFIX
			+ "UNOPT_DISTINCT_POTENTIAL_ANSWERS";
	public static final String CAvSAT_ALL_DISTINCT_POTENTIAL_ANS_TABLE_NAME = CAvSAT_TBL_PREFIX
			+ "ALL_DISTINCT_POTENTIAL_ANSWERS";
	public static final String CAvSAT_FINAL_ANSWERS_TABLE_NAME = CAvSAT_TBL_PREFIX + "FINAL_ANSWERS";
	public static final String CAvSAT_AGG_FINAL_ANSWERS_TABLE_NAME = CAvSAT_TBL_PREFIX + "AGG_FINAL_ANSWERS";
	public static final String CAvSAT_CONQUER_CONSISTENT_ANSWERS_TABLE_NAME = CAvSAT_TBL_PREFIX + "CONQUER_ANSWERS";
	public static final String CAvSAT_FACTID_COLUMN_NAME = "CAvSAT_FactID";
	public static final String CAvSAT_UNOPT_FACTID_COLUMN_NAME = "CAvSAT_UNOPT_FactID";
	public static final String CAvSAT_KEYS_TABLE_PREFIX = CAvSAT_TBL_PREFIX + "KEYS_";
	public static final String CAvSAT_RELEVANT_TABLE_PREFIX = CAvSAT_TBL_PREFIX + "RELEVANT_";
	public static final String CAvSAT_GET_CONSTRAINTS_QUERY = "SELECT Constraint_ID, Constraint_Type, Constraint_Definition FROM "
			+ CAvSAT_CONSTRAINTS_TABLE_NAME;
	public static final String FORMULA_FILE_NAME = "formula.txt";
	public static final String SAT_OUTPUT_FILE_NAME = "output.txt";
	public static final String MIN_TO_MAX_ENCODED_FORMULA_FILE_NAME = "formulaMinToMax.txt";
	public static final String MAXSAT_SOLVER_NAME = "MaxHS";
	public static final String MAXSAT_COMMAND = "maxhs";
	public static final String MINSAT_SOLVER_NAME = "MinSatz";
	public static final String MINSAT_COMMAND = "MinSatz2013";
	
}
