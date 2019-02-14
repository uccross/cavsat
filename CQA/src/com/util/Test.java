package com.util;

import java.io.IOException;
import java.util.List;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Schema;
import com.querypreprocessor.AttackGraphBuilder;
import com.querypreprocessor.CertainRewriter;

public class Test {

	public static void main(String[] args) throws IOException {
		ProblemParser pp = new ProblemParser();
		List<Query> uCQ = pp
				.parseUCQ("C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\toyquery.txt");
		Schema schema = pp
				.parseSchema("C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\schema.txt");
		AttackGraphBuilder builder = new AttackGraphBuilder(uCQ.get(0));
		System.out.println(builder.isQueryFO());
		List<Atom> list = builder.topologicalSort();
		System.out.println("Topological sorting: " + list);
		CertainRewriter certainRewriter = new CertainRewriter();
		uCQ.get(0).setAtoms(list);
		certainRewriter.doEverything(uCQ.get(0), schema);
	}
}
