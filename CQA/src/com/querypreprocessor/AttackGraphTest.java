package com.querypreprocessor;

import com.beans.Query;
import com.util.ProblemParser;

public class AttackGraphTest {

	public static void main(String[] args) {
		ProblemParser pp = new ProblemParser();
		Query query = pp
				.parseQueryFromFile("C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\q3.txt");
		AttackGraphBuilder builder = new AttackGraphBuilder(query);
		System.out.println(builder.isQueryFO());
		builder.topologicalSort();
	}
}