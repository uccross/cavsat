package com.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticDataGenerator {

	public static void main(String[] args) {
		SyntheticDataGenerator generator = new SyntheticDataGenerator();
		generator.generate(100000, 10);
	}

	public void generate(int size, int inconsistencyPercentage) {
		String[] attributes = { "A", "B", "C" };
		String[] keyAttributes = { "A" };
		generateConsistentData("R1", attributes, size, inconsistencyPercentage, 2, false, null, null, -1);
		generateInconsistentData("R1", attributes, keyAttributes, size, inconsistencyPercentage, 2);

		Map<Integer, Integer> joinAttributes = new HashMap<Integer, Integer>();
		joinAttributes.put(2, 1);
		joinAttributes.put(1, 2);
		generateConsistentData("R2", attributes, size, inconsistencyPercentage, 2, true, "R1", joinAttributes, 25);
		generateInconsistentData("R2", attributes, keyAttributes, size, inconsistencyPercentage, 2);

		generateConsistentData("R4", attributes, size, inconsistencyPercentage, 2, false, null, null, -1);
		generateInconsistentData("R4", attributes, keyAttributes, size, inconsistencyPercentage, 2);

		generateConsistentData("R3", new String[] { "A", "B" }, size, inconsistencyPercentage, 2, false, null, null,
				-1);
		// generator.createTable("R3", new String[] { "A", "B" });
		generateR3(size, 25);
		joinAttributes.clear();
		joinAttributes.put(2, 1);
		generateConsistentData("R5", attributes, size, inconsistencyPercentage, 2, true, "R1", joinAttributes, 25);
		generateInconsistentData("R5", attributes, new String[] { "A", "B" }, size, inconsistencyPercentage, 2);

		joinAttributes.clear();
		joinAttributes.put(2, 2);
		generateConsistentData("R6", attributes, size, inconsistencyPercentage, 2, true, "R1", joinAttributes, 25);
		generateInconsistentData("R6", attributes, new String[] { "A" }, size, inconsistencyPercentage, 2);

		joinAttributes.clear();
		joinAttributes.put(1, 1);
		joinAttributes.put(2, 2);
		generateConsistentData("R7", attributes, size, inconsistencyPercentage, 2, true, "R1", joinAttributes, 25);
		generateInconsistentData("R7", attributes, new String[] { "A" }, size, inconsistencyPercentage, 2);

		joinAttributes.clear();
		joinAttributes.put(1, 2);
		joinAttributes.put(2, 1);
		generateConsistentData("R8", attributes, size, inconsistencyPercentage, 2, true, "R1", joinAttributes, 25);
		generateConsistentData("R8", attributes, size, inconsistencyPercentage, 2, true, "R7", joinAttributes, 25);
		generateInconsistentData("R8", attributes, new String[] { "A" }, size, inconsistencyPercentage, 2);
	}

	private void generateR3(int size, double percentJoin) {
		Connection con = new DBEnvironment().getConnection();
		try {
			PreparedStatement ps1 = con
					.prepareStatement("SELECT B FROM R1 ORDER by RANDOM() LIMIT " + (int) (percentJoin * size / 100));
			PreparedStatement ps2 = con
					.prepareStatement("SELECT A FROM R4 ORDER by RANDOM() LIMIT " + (int) (percentJoin * size / 100));

			PreparedStatement psInsert = con.prepareStatement("INSERT INTO R3 VALUES (?,?)");
			ResultSet rs1 = ps1.executeQuery();
			ResultSet rs2 = ps2.executeQuery();

			while (rs1.next() && rs2.next()) {
				psInsert.setString(1, rs1.getString(1));
				psInsert.setString(2, rs2.getString(1));
				psInsert.addBatch();
			}
			psInsert.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void generateConsistentData(String relationName, String[] attributes, int nSize, int percentInconsistency,
			int keyGroupSize, boolean joinPrevious, String previousTableName, Map<Integer, Integer> joinAttributes,
			int percentJoin) {
		createTable(relationName, attributes);
		int nInconsistent = percentInconsistency * nSize / 100;
		int nKeysInconsistent = nInconsistent / keyGroupSize;
		int nConsistent = nSize - nInconsistent + nKeysInconsistent;
		insertConsistentRows(relationName, attributes, nConsistent, joinPrevious, previousTableName, joinAttributes,
				percentJoin);
	}

	public void generateInconsistentData(String relationName, String[] attributes, String[] keyAttributes, int nSize,
			int percentInconsistency, int keyGroupSize) {
		int nInconsistent = percentInconsistency * nSize / 100;
		int nKeysInconsistent = nInconsistent / keyGroupSize;
		insertInconsistency(relationName, attributes, ArrayToCSV(keyAttributes), nKeysInconsistent, keyGroupSize);
	}

	private void insertConsistentRows(String relationName, String[] attributes, int size, boolean joinPrevious,
			String previousTableName, Map<Integer, Integer> joinAttributes, int percentJoin) {
		String insert = "INSERT INTO " + relationName + " VALUES (";
		for (int i = 0; i < attributes.length; i++) {
			insert += "?,";
		}
		insert = insert.substring(0, insert.length() - 1) + ")";
		int joinedRows = percentJoin * size / 100;
		String selectRandomRowsToJoin = "SELECT * FROM " + previousTableName + " ORDER BY RANDOM() LIMIT " + joinedRows;
		Connection con = new DBEnvironment().getConnection();
		RandomString gen = new RandomString(4, ThreadLocalRandom.current());
		ResultSet rsJoinedRows = null;
		try {
			PreparedStatement psJoinedRows = con.prepareStatement(selectRandomRowsToJoin);
			if (joinPrevious) {
				rsJoinedRows = psJoinedRows.executeQuery();
			}
			PreparedStatement psInsert = con.prepareStatement(insert);
			for (int i = 0; i < size; i++) {
				for (int j = 1; j <= attributes.length; j++) {
					psInsert.setString(j, gen.nextString());
				}
				if (joinPrevious && i % (size / joinedRows) == 0) {
					if (rsJoinedRows.next()) {
						for (int j : joinAttributes.keySet()) {
							psInsert.setString(joinAttributes.get(j), rsJoinedRows.getString(j));
						}
					}
				}
				psInsert.addBatch();
			}
			psInsert.executeBatch();
			psInsert.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertInconsistency(String relationName, String[] attributes, String keyAttributesCSV, int nSize,
			int keyGroupSize) {
		Connection con = new DBEnvironment().getConnection();
		RandomString gen = new RandomString(4, ThreadLocalRandom.current());

		String insert = "INSERT INTO " + relationName + " VALUES (";
		for (int i = 0; i < attributes.length; i++) {
			insert += "?,";
		}
		insert = insert.substring(0, insert.length() - 1) + ")";
		String selectRandom = "SELECT " + keyAttributesCSV + " FROM " + relationName + " ORDER BY RANDOM() LIMIT "
				+ nSize;
		int noOfKeys = keyAttributesCSV.split(",").length;
		try {
			PreparedStatement psInsert = con.prepareStatement(insert);
			PreparedStatement psSelectRandom = con.prepareStatement(selectRandom);
			ResultSet rsSelectRandom = psSelectRandom.executeQuery();

			while (rsSelectRandom.next()) {
				for (int k = 0; k < keyGroupSize - 1; k++) {
					for (int j = 1; j <= noOfKeys; j++) {
						psInsert.setString(j, rsSelectRandom.getString(j));
					}
					for (int j = noOfKeys + 1; j <= attributes.length; j++) {
						psInsert.setString(j, gen.nextString());
					}
					psInsert.addBatch();
				}
			}
			psInsert.executeBatch();
			psSelectRandom.close();
			psInsert.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void createTable(String relationName, String[] attributes) {
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

	public String ArrayToCSV(String[] arr) {
		StringBuilder output = new StringBuilder();
		String delimiter = "";
		for (String s : arr) {
			output.append(delimiter);
			delimiter = ",";
			output.append(s);
		}
		return output.toString();
	}
}
