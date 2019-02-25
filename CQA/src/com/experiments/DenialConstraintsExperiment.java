package com.experiments;

import java.util.List;

import com.beans.Query;
import com.beans.Schema;
import com.core.Preprocessor2;
import com.util.DBEnvironment;
import com.util.ProblemParser;

public class DenialConstraintsExperiment {

	public static void main(String[] args) {
		ProblemParser parser = new ProblemParser();
		List<Query> uCQ = parser.parseUCQ(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\FO-rewritable\\q3.txt");
		Schema schema = parser.parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\lingeling-master\\FO-rewritable\\schema.txt");
		Preprocessor2 pp = new Preprocessor2(schema, uCQ, new DBEnvironment().getConnection());
		pp.createMinimalWitnessesToUCQ();
	}
}
