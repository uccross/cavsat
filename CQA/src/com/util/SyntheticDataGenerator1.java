package com.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class SyntheticDataGenerator1 {

	public static void main(String[] args) throws SQLException {
		ProblemParser pp = new ProblemParser();
		List<File> filesInFolder = null;
		try {
			filesInFolder = Files.walk(Paths.get(
					"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\FO-rewritable"))
					.filter(Files::isRegularFile).map(Path::toFile).collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}
		Connection con = new DBEnvironment().getConnection();
		Schema schema = pp.parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\schema.txt");
		for (Relation r : schema.getRelations()) {
			createTable(r.getName(), r.getAttributes().toArray(new String[0]));
		}
		for (File file : filesInFolder) {
			System.out.println(file.getName());
			Query q = pp.parseUCQ(file).get(0);
			new SyntheticDataGenerator1().generate(q, schema, con, 1000, 10, 4);
			System.out.println("-------------------------------------------------------------------------------");
			break;
		}
	}

	public void generate(Query q, Schema schema, Connection con, int n, int inconsistency, int keyGroupSize)
			throws SQLException {
		RandomString randStr = new RandomString(5);
		Random rand = new Random();
		Atom firstAtom = q.getAtoms().get(0);
		createTable(firstAtom.getName(),
				schema.getRelationByName(firstAtom.getName()).getAttributes().toArray(new String[0]));
		PreparedStatement psInsert = con.prepareStatement("INSERT INTO " + firstAtom.getName() + " VALUES ("
				+ getQuestionMarks(firstAtom.getVars().size()) + ")");
		int i = 0;
		while (i < n) {
			for (int j = 1; j <= firstAtom.getVars().size(); j++) {
				psInsert.setString(j, randStr.nextString());
			}
			psInsert.addBatch();
			i++;
			if (rand.nextDouble() < (inconsistency / (100.0 * keyGroupSize))) {
				for (int k = 0; k < keyGroupSize - 1; k++) {
					psInsert.addBatch();
					i++;
				}
			}
		}
		psInsert.executeBatch();
		psInsert.close();
		for (int j = 1; i < q.getAtoms().size(); i++) {
			Atom atom = q.getAtoms().get(j);
			createTable(atom.getName(),
					schema.getRelationByName(atom.getName()).getAttributes().toArray(new String[0]));
			psInsert = con.prepareStatement(
					"INSERT INTO " + atom.getName() + " VALUES (" + getQuestionMarks(firstAtom.getVars().size()) + ")");
			for (int k = 0; k < j; k++) {

			}
		}

	}

	private static String getQuestionMarks(int howMany) {
		String s = "", prefix = "";
		for (int i = 0; i < howMany; i++) {
			s = s + prefix + "?";
			prefix = ",";
		}
		return s;
	}

	private static void createTable(String relationName, String[] attributes) {
		String dropTable = "DROP TABLE " + relationName + " CASCADE";
		String createTable = "CREATE TABLE " + relationName + " (";
		for (String attr : attributes) {
			createTable += attr + " varchar(10),";
		}
		createTable = createTable.substring(0, createTable.length() - 1) + ")";
		DBEnvironment db = new DBEnvironment();
		Connection con = db.getConnection();

		PreparedStatement psDropTable;
		PreparedStatement psCreateTable;

		try {
			psDropTable = con.prepareStatement(dropTable);
			psDropTable.execute();
			System.out.println("Table " + relationName + " dropped.");
		} catch (SQLException e) {
			System.err.println("Dropping table " + relationName + " failed.");
		}

		try {
			psCreateTable = con.prepareStatement(createTable);
			psCreateTable.execute();
			System.out.println("Table " + relationName + " created.");
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("Creating table " + relationName + " failed.");
		}
	}

}
