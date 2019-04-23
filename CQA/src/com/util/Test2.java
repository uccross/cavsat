/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import com.beans.SQLQuery;

public class Test2 {

	public static void main(String[] args) {
		SQLQuery query = new ProblemParser2().parseSQLQuery(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\conquer-query.txt");
		query.print();
	}
}
